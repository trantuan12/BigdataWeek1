"""Shared runtime plumbing; no credentials are serialized or printed."""
import csv
import hashlib
import importlib.metadata
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
import boto3
import duckdb
from botocore.config import Config
from jsonschema import Draft202012Validator, FormatChecker

ROOT = Path(__file__).resolve().parent.parent
STORE = os.environ.get('STORE_ID', 'bd-g10-objects')
PREFIX = 'lab2/inputs/batch-01/'
OPERATOR = 'Automated pipeline execution'
ALLOWLIST = [('record_id','VARCHAR'),('sensor_id','VARCHAR'),('site','VARCHAR'),
    ('event_time_utc','TIMESTAMP'),('ingest_time_utc','TIMESTAMP'),
    ('temperature_c','DECIMAL(8,2)'),('is_late','BOOLEAN'),
    ('source_object','VARCHAR'),('source_row','BIGINT'),('source_sha256','VARCHAR')]

def now():
    return datetime.now(timezone.utc).isoformat()

def serial(value):
    if isinstance(value, (datetime, Decimal, Path)): return str(value)
    raise TypeError(type(value).__name__)

def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), default=serial).encode('utf-8')

def digest_bytes(data):
    return hashlib.sha256(data).hexdigest()

def sha(path):
    return digest_bytes(Path(path).read_bytes())

def write_json(path, value):
    p = Path(path); p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=serial)+'\n', encoding='utf-8')

def read_json(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))

def s3():
    return boto3.client('s3', endpoint_url=os.environ['S3_ENDPOINT'], region_name='us-east-1',
        config=Config(signature_version='s3v4', s3={'addressing_style':'path'},
                      connect_timeout=10, read_timeout=30, retries={'max_attempts':2}))

def get_bytes(client, bucket, key):
    body = client.get_object(Bucket=bucket, Key=key)['Body']
    try: return body.read()
    finally: body.close()

def trusted_path():
    mounted = Path('/trusted/trusted_manifest.json')
    return mounted if mounted.exists() else ROOT/'trusted_manifest.json'

def validate_json(value, schema):
    Draft202012Validator(read_json(schema), format_checker=FormatChecker()).validate(value)

def code_bundle():
    entries = [{'file':p.relative_to(ROOT).as_posix(), 'sha256':sha(p)}
        for p in sorted((ROOT/'code').glob('*')) if p.suffix in {'.py','.sql'}]
    return {'files':entries, 'sha256':digest_bytes(canonical(entries))}

def runtime():
    return {'python':sys.version.split()[0], 'packages':{p:importlib.metadata.version(p)
        for p in ['duckdb','boto3','jsonschema']},
        'image_reference':os.environ.get('RUNTIME_IMAGE','UNKNOWN'),
        'image_digest':os.environ.get('RUNTIME_DIGEST','UNKNOWN'),
        'platform':sys.platform, 'threads':2, 'memory_limit':'512MB'}

def event(kind, run_id, job, inputs, outputs):
    def ds(name): return {'namespace':'urn:bigdata:'+STORE, 'name':name}
    value = {'eventType':kind,'eventTime':now(),
        'producer':'urn:vnu-uet:bigdata:lab2:automated-pipeline',
        'schemaURL':'https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent',
        'run':{'runId':run_id}, 'job':{'namespace':'vnu-uet.bigdata.lab2','name':job},
        'inputs':[ds(x) for x in inputs], 'outputs':[ds(x) for x in outputs]}
    validate_json(value, ROOT/'schemas/OpenLineage-2-0-2.json')
    return value

def append_event(value, path=None):
    p = Path(path or ROOT/'lineage.jsonl')
    with p.open('a',encoding='utf-8') as f: f.write(json.dumps(value,ensure_ascii=False)+'\n')

def rows(con, query, params=None):
    cursor=con.execute(query, params or [])
    cols=[c[0] for c in cursor.description]
    return [dict(zip(cols,r)) for r in cursor.fetchall()]

def connect():
    con=duckdb.connect(); con.execute("SET TimeZone='UTC'; SET threads=2; SET memory_limit='512MB'")
    return con

def configure(con, contract):
    con.execute('CREATE OR REPLACE TABLE cfg(as_of TIMESTAMP, lo DOUBLE, hi DOUBLE, late_seconds BIGINT, id_pattern VARCHAR, timestamp_pattern VARCHAR)')
    con.execute('INSERT INTO cfg VALUES (?,?,?,?,?,?)', [contract['as_of'],contract['temperature_range_c']['min'],
        contract['temperature_range_c']['max'],contract['late_threshold_seconds'],contract['record_id_pattern'],contract['timestamp_pattern']])
    con.execute('CREATE OR REPLACE TABLE supported_units(unit VARCHAR)')
    con.executemany('INSERT INTO supported_units VALUES (?)',[(u,) for u in contract['supported_units']])
    con.execute('CREATE OR REPLACE TABLE reason_order(reason VARCHAR, ordinal INTEGER)')
    con.executemany('INSERT INTO reason_order VALUES (?,?)',[(r,i) for i,r in enumerate(contract['primary_reason_precedence'])])
    expected=['ingest_ts DESC','source_object ASC','source_row ASC']
    if contract['deduplication'] != {'business_key':['record_id'],'order':expected}:
        raise ValueError('Unsupported contract ordering; create a new implementation version')
    con.execute("CREATE OR REPLACE MACRO blank(x) AS nullif(regexp_replace(x, '^\\s+|\\s+$', '', 'g'), '')")

def engine(envelopes=None, contract=None):
    con=connect(); configure(con,contract or read_json(ROOT/'contract.json'))
    envelope_path=str(envelopes or ROOT/'restricted/envelopes.csv').replace("'","''")
    con.execute(f"CREATE TABLE raw_envelopes AS SELECT * FROM read_csv('{envelope_path}', header=true, all_varchar=true)")
    for name, file in [('registry','sensors.csv'),('qa_reference','qa_reference.csv')]:
        path=str(ROOT/'restricted/snapshot'/file).replace("'","''")
        con.execute(f"CREATE TABLE {name} AS SELECT * FROM read_csv('{path}', all_varchar=true)")
    con.execute((ROOT/'code/typed.sql').read_text())
    con.execute((ROOT/'code/curate.sql').read_text())
    return con

def schema_of(con, relation):
    return [(r[0],r[1]) for r in con.execute(f'DESCRIBE SELECT * FROM {relation}').fetchall()]

def logical_digest(con, relation):
    cols=[r[0] for r in schema_of(con,relation)]
    order=', '.join('"'+c+'"' for c in cols)
    return digest_bytes(canonical(con.execute(f'SELECT * FROM {relation} ORDER BY {order}').fetchall()))

def artifact(path, key=None, row_count=None):
    p=Path(path)
    return {'path':p.relative_to(ROOT).as_posix(), 'key':key,
        'sha256':sha(p), 'bytes':p.stat().st_size, 'row_count':row_count}
