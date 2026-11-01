"""Prepare an empty teaching bucket and verify the two worker services."""
import json
from pathlib import Path
import time
from urllib.request import urlopen

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

client = boto3.client('s3', endpoint_url='http://objects:9000',
                      region_name='us-east-1',
                      config=Config(s3={'addressing_style': 'path'},
                                    connect_timeout=3, read_timeout=3,
                                    retries={'max_attempts': 1}))
deadline = time.monotonic() + 90
while True:
    try:
        client.list_buckets()
        with urlopen('http://spark-master:8080/json/', timeout=3) as response:
            master = json.load(response)
        workers = [w for w in master['workers'] if w['state'] == 'ALIVE']
        if len(workers) != 2:
            raise RuntimeError('Waiting for exactly two ALIVE Spark workers')
        break
    except Exception:
        if time.monotonic() >= deadline:
            raise RuntimeError('Storage or both Spark workers are not ready')
        time.sleep(2)

bucket = 'research-lakehouse'
try:
    client.head_bucket(Bucket=bucket)
except ClientError as exc:
    if exc.response['Error']['Code'] not in ('404', 'NoSuchBucket'):
        raise
    client.create_bucket(Bucket=bucket)

settings = json.loads(Path('config/team.json').read_text(encoding='utf-8'))
evidence = Path(settings['evidence_dir'])
evidence.mkdir(parents=True, exist_ok=True)
(evidence / 'cluster_services.json').write_text(json.dumps({
    'deployment': 'Spark standalone; two worker containers; shared S3A object store',
    'placement_limit': 'Separate containers do not prove separate physical servers.',
    'master': 'spark://spark-master:7077',
    'workers': [{'host': w['host'], 'state': w['state'],
                 'cores': w['cores'], 'memory_mb': w['memory']} for w in workers],
    'object_endpoint': 'http://objects:9000',
    'bucket': bucket,
    'write_protocol': 'One driver at a time; native Delta smoke/merge checks required.',
}, indent=2) + '\n', encoding='utf-8')
for filename in ('requirements.lock.txt', 'jars.lock.json'):
    (evidence / filename).write_bytes((Path('/opt/lab3') / filename).read_bytes())
print('READY: object store and two Spark worker services; task probes still required.')
