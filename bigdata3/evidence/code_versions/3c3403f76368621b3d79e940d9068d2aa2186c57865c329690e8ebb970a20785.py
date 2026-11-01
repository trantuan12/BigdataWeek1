"""Export selected completed SQL scan metrics from Spark History Server."""
import argparse
import json
from pathlib import Path
import sys
from urllib.request import urlopen

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from common import read_json,write_json

parser=argparse.ArgumentParser()
parser.add_argument('--config',default='config/team.json')
parser.add_argument('--history-url',default='http://history:18080')
args=parser.parse_args()
config=read_json(args.config)
ev=Path(config['evidence_dir'])
runtime=read_json(ev/'runtime_benchmark.json')
report=read_json(ev/'benchmark_report.json')
base=args.history_url.rstrip('/')+'/api/v1/applications/'+runtime['application_id']+'/sql'


def get(url):
    with urlopen(url,timeout=15) as response:
        return json.load(response)


queries=get(base+'?offset=0&length=1000')
executions=[]
for query in queries:
    if not query['description'].startswith('collect at /workspace/src/run_lab.py:'):
        continue
    detail=get(base+'/'+str(query['id']))
    plan=detail['planDescription']
    if 'n_observations' not in plan or 'sum_temperature_c' not in plan:
        continue
    labels=[label for label,data in report['layout_details'].items() if data['path'] in plan]
    if len(labels)!=1:
        raise ValueError('AMBIGUOUS_QUERY_LAYOUT')
    nodes=[node for node in detail['nodes'] if node['nodeName'].startswith('Scan parquet')]
    if not nodes:
        raise ValueError('MISSING_DATA_SCAN_NODE')
    executions.append({'sql_execution_id':query['id'],'layout':labels[0],
        'status':detail['status'],'sql_duration_ms':detail['duration'],
        'scan_nodes':[{'name':node['nodeName'],'metrics':node['metrics']} for node in nodes],
        'partition_filter_lines':[line.strip() for line in plan.splitlines() if 'PartitionFilters:' in line]})
executions.sort(key=lambda row:row['sql_execution_id'])
if len(executions)!=8 or [r['layout'] for r in executions]!=list('ABABBAAB'):
    raise ValueError('EXPECTED_TWO_WARMUPS_AND_SIX_MEASURED_SQL_EXECUTIONS')
for index,execution in enumerate(executions):
    execution['phase']='warmup' if index<2 else 'measured'
    if index>=2:
        measurement=report['measurements'][index-2]
        execution['measurement_number']=index-1
        execution['round']=measurement['round']
        execution['driver_elapsed_seconds']=measurement['elapsed_seconds']
executors=get(args.history_url.rstrip('/')+'/api/v1/applications/'+runtime['application_id']+'/executors')
executor_fields=('id','hostPort','isActive','totalCores','maxTasks','totalTasks','failedTasks',
                 'completedTasks','totalDuration','totalGCTime','maxMemory')
write_json(ev/'benchmark_scan_metrics.json',{'application_id':runtime['application_id'],
    'executors':[{key:executor.get(key) for key in executor_fields} for executor in executors],
    'source':'Spark History Server SQL REST API backed by completed Spark event logs.',
    'metric_note':'Scan-node metrics are separate from Delta total-table metadata. SQL UI duration is separate from driver wall time.',
    'executions':executions})
print('Saved actual scan metrics for two warm-ups and six measured queries.')
