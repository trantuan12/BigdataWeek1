"""Role A setup executed with owner identity; uploads only missing input objects."""
from common import *
from stage_inputs import verify
from botocore.exceptions import ClientError

if __name__=='__main__':
    client=s3(); manifest=read_json(trusted_path()); result=[]
    for obj in manifest['objects']:
        data=(ROOT/'input-upload'/obj['key']).read_bytes(); verify(data,obj)
        try:
            existing=get_bytes(client,'research-raw',PREFIX+obj['key']); verify(existing,obj)
            action='existing bytes verified'
        except ClientError as exc:
            if exc.response['Error']['Code'] not in {'NoSuchKey','404'}: raise
            client.put_object(Bucket='research-raw',Key=PREFIX+obj['key'],Body=data)
            verify(get_bytes(client,'research-raw',PREFIX+obj['key']),obj); action='uploaded and verified'
        result.append({'key':PREFIX+obj['key'],'action':action,'sha256':obj['sha256']})
    write_json(ROOT/'evidence/bootstrap.json',{'operator':OPERATOR,'workload_identity':'s3-owner',
        'executed_at':now(),'objects':result})
    print(json.dumps({'status':'PASS','objects':len(result)}))
