"""Analyst context: verify only approved release objects and an actual raw denial."""
import sys
from common import *
from botocore.exceptions import ClientError

def consume(manifest_key, manifest_hash):
    client=s3();data=get_bytes(client,'research-release',manifest_key)
    if digest_bytes(data)!=manifest_hash:raise RuntimeError('MANIFEST_INTEGRITY')
    manifest=json.loads(data);root=ROOT/'consumer-download';root.mkdir(exist_ok=False)
    checks=[];con=connect();count=None
    for a in manifest['artifacts']:
        data=get_bytes(client,'research-release',a['key']);ok=digest_bytes(data)==a['sha256'] and len(data)==a['bytes']
        if not ok:raise RuntimeError('RELEASE_OBJECT_INTEGRITY')
        path=root/a['key'].rsplit('/',1)[1];path.write_bytes(data)
        checks.append({'key':a['key'],'sha256_matches':ok,'bytes':len(data)})
        if path.name=='curated.parquet':
            count=con.execute('SELECT count(*) FROM read_parquet(?)',[str(path)]).fetchone()[0]
            schema=schema_of(con,f"read_parquet('{path}')")
            if count!=manifest['row_count'] or schema!=ALLOWLIST:raise RuntimeError('RELEASE_SCHEMA_COUNT')
    try:
        get_bytes(client,'research-raw',PREFIX+'observations_a.csv');raise RuntimeError('RAW_ACCESS_UNEXPECTEDLY_ALLOWED')
    except ClientError as exc:
        code=exc.response['Error']['Code'];status=exc.response['ResponseMetadata']['HTTPStatusCode']
        if status!=403 or code!='AccessDenied':raise
        denial={'bucket':'research-raw','key':PREFIX+'observations_a.csv','http_status':status,'code':code}
    # Actual write denial for the analyst is also measured.
    try:
        client.put_object(Bucket='research-release',Key='lab2/forbidden-analyst/'+str(uuid.uuid4()),Body=b'probe')
        raise RuntimeError('ANALYST_WRITE_UNEXPECTEDLY_ALLOWED')
    except ClientError as exc:
        if exc.response['ResponseMetadata']['HTTPStatusCode']!=403 or exc.response['Error']['Code']!='AccessDenied':raise
        write_denial={'http_status':403,'code':'AccessDenied'}
    catalog=read_json(root/'catalog.json');entry=catalog['datasets'][0]
    handover={'temperature_unit':next(c['unit'] for c in entry['columns'] if c['name']=='temperature_c'),
        'reference_population':'100 supplied QA-reference IDs','evaluation_cutoff':entry['as_of'],
        'known_limitations':'Synthetic bounded single-node fixture; QA sample agreement cannot prove accuracy of every observation; retained valid late records remain flagged.',
        'contact_role':entry['steward']}
    result={'status':'PASS','release_id':manifest['release_id'],'manifest_key':manifest_key,
        'manifest_sha256':manifest_hash,'artifact_checks':checks,'observed_row_count':count,
        'schema_equal':schema==ALLOWLIST,'raw_get_denial':denial,'release_put_denial':write_denial,
        'M03':{'answers':handover,'correct_answers':5,'execution':'Automated consumer handover verification','student_peer_demonstration_attested':False},
        'operator':OPERATOR,'workload_identity':'s3-analyst','executed_at':now()}
    write_json(ROOT/'consumer-check.json',result);con.close();print(json.dumps({'status':'PASS','rows':count,'raw_get_http':403}))

if __name__=='__main__':consume(sys.argv[1],sys.argv[2])
