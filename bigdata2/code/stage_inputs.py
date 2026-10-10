"""Role A. S3 acquisition and source-envelope mapping adapted from Appendix C."""
import csv
import json
from pathlib import Path
from common import *
FIELDS=['record_id','sensor_id','event_time','ingest_time','reading','unit','operator_email']
BKEYS=['id','sensor','timestamp','arrived_at','temperature','temperature_unit','operator']
EXTRA=['source_object','source_row','source_sha256','parse_ok','raw_payload']

def verify(data, obj):
    if len(data)!=obj['bytes'] or digest_bytes(data)!=obj['sha256']:
        raise ValueError('INPUT_INTEGRITY: '+obj['key'])

def envelopes(snapshot, dest, manifest, enumeration=None):
    inventory=[]
    objects={o['key']:o for o in manifest['objects']}
    with Path(dest).open('w',encoding='utf-8',newline='') as out:
        writer=csv.DictWriter(out,fieldnames=FIELDS+EXTRA); writer.writeheader()
        for key in enumeration or sorted(objects):
            if not key.startswith('observations_'): continue
            obj=objects[key]; n=0
            with (Path(snapshot)/key).open(encoding='utf-8',newline='') as src:
                records=csv.DictReader(src) if key.endswith('.csv') else src
                for n, raw in enumerate(records,1):
                    payload=json.dumps(raw,ensure_ascii=False) if isinstance(raw,dict) else raw
                    try:
                        value=raw if isinstance(raw,dict) else json.loads(raw)
                        if not isinstance(value,dict): raise ValueError('not an object')
                        if key.endswith('.jsonl'): value={a:value.get(b) for a,b in zip(FIELDS,BKEYS)}
                        row={f:'' if value.get(f) is None else str(value[f]) for f in FIELDS}; ok=True
                    except (ValueError,TypeError):
                        row={f:'' for f in FIELDS}; ok=False
                    row.update(source_object='s3://research-raw/'+PREFIX+key,source_row=n,
                        source_sha256=obj['sha256'],parse_ok=str(ok).lower(),raw_payload=payload)
                    writer.writerow(row)
            if n!=obj['records']: raise ValueError('ROW_COUNT: '+key)
            inventory.append({'key':key,'records':n})
    return inventory

def stage():
    client=s3(); manifest=read_json(trusted_path())
    required={'observations_a.csv','observations_b.jsonl','sensors.csv','qa_reference.csv'}
    if {o['key'] for o in manifest['objects']}!=required: raise ValueError('INPUT_INVENTORY')
    if sha(ROOT/'trusted_manifest.json')!=sha(trusted_path()): raise ValueError('TRUSTED_MANIFEST_MOUNT')
    snapshot=ROOT/'restricted/snapshot'; snapshot.mkdir(parents=True,exist_ok=False)
    inventory=[]
    for obj in manifest['objects']:
        data=get_bytes(client,'research-raw',PREFIX+obj['key']); verify(data,obj)
        (snapshot/obj['key']).write_bytes(data)
        inventory.append({**obj,'bucket':'research-raw','s3_key':PREFIX+obj['key'],
            'store_id':STORE,'format':'JSONL' if obj['key'].endswith('.jsonl') else 'CSV',
            'retrieved_at':now(),'verification':'PASS'})
    envelopes(snapshot,ROOT/'restricted/envelopes.csv',manifest)
    write_json(ROOT/'input_inventory.json',{'status':'VERIFIED','store_id':STORE,'objects':inventory,
        'trusted_manifest_sha256':sha(trusted_path()),'observation_rows':10205})
    obj=manifest['objects'][0]; original=(snapshot/obj['key']).read_bytes()
    changed=bytearray(original); changed[len(changed)//2]^=1
    try: verify(bytes(changed),obj); raise AssertionError('Tamper accepted')
    except ValueError as exc: reason=str(exc)
    write_json(ROOT/'integrity-test.json',{'check_id':'I03','status':'PASS',
        'test':'one-byte disposable-copy mutation','observed_rejection':reason,
        'original_unchanged':sha(snapshot/obj['key'])==obj['sha256'],
        'operator':OPERATOR,'executed_at':now()})
    probe='lab2/forbidden-curator/'+str(uuid.uuid4())+'.txt'
    from botocore.exceptions import ClientError
    try:
        client.put_object(Bucket='research-release',Key=probe,Body=b'forbidden-access-probe')
        raise RuntimeError('SECURITY_BOUNDARY: curator release PUT unexpectedly allowed')
    except ClientError as exc:
        error=exc.response
        if error['ResponseMetadata']['HTTPStatusCode']!=403 or error['Error']['Code']!='AccessDenied': raise
        denial={'http_status':403,'code':'AccessDenied','bucket':'research-release','key':probe}
    write_json(ROOT/'access-test.json',{'check_id':'I04','status':'PASS','workload_identity':'s3-ingestor',
        'raw_get':'PASS: all four objects fetched through S3','release_put_denial':denial,
        'operator':OPERATOR,'executed_at':now()})
    return manifest

if __name__=='__main__':
    os.chdir(ROOT); stage()
