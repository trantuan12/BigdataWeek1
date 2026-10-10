"""Role D: query the submitted catalog without accessing raw payloads."""
from common import *

def queries(catalog):
    entries=catalog['datasets']
    return {'M01':[{k:e[k] for k in ['dataset_id','owner','steward','row_count','location','schema_version']}
        for e in entries if e['dataset_id']=='curated-candidate' and e['quality_report'] and
        any(c['name']=='temperature_c' and c['unit']=='Celsius' for c in e['columns'])],
        'M02':[{k:e[k] for k in ['dataset_id','classification','retention']}
        for e in entries if e['classification']=='RESTRICTED' and e['retention']['days']>0 and
        e['retention']['hold_rule'] and e['retention']['dependency_rule']]}

if __name__=='__main__':
    import sys
    sys.stdout.reconfigure(encoding='utf-8')
    print(json.dumps(queries(read_json(ROOT/'catalog.json')),ensure_ascii=False,indent=2))
