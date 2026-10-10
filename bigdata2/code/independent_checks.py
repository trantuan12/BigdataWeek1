"""Independent formulas on stored artifacts, executed through the automated pipeline."""
from common import *

def perform():
    con=connect();con.execute('CREATE TABLE c AS SELECT * FROM read_parquet(?)',[str(ROOT/'candidate/curated.parquet')])
    checks=[]
    trace=read_json(ROOT/'evidence/traces.json')
    trusted=read_json(trusted_path());hashes={o['key']:o['sha256'] for o in trusted['objects']}
    outcomes=[]
    for tag,record in [('accepted',trace['accepted']['output']),('quarantined',trace['quarantined'])]:
        filename=record['source_object'].rsplit('/',1)[1];path=ROOT/'restricted/snapshot'/filename
        valid_hash=sha(path)==record['source_sha256']==hashes[filename]
        with path.open(encoding='utf-8',newline='') as f:
            source=list(csv.DictReader(f)) if filename.endswith('.csv') else list(f)
        physical=source[record['source_row']-1]
        if isinstance(physical,str):
            try: physical=json.loads(physical)
            except ValueError: physical={'parse_failed':True}
        rid=physical.get('record_id',physical.get('id'))
        normalized=rid.strip().upper() if isinstance(rid,str) and rid.strip() else None
        outcomes.append({'trace':tag,'source_object':record['source_object'],'source_row':record['source_row'],
            'source_hash_match':valid_hash,'normalized_record_id':normalized,
            'record_id_matches':normalized==record.get('record_id')})
    checks.append({'role':'A','reviewed_role':'D','check':'Reproduce both stored traces from original source ordinals',
        'status':'PASS' if all(x['source_hash_match'] and x['record_id_matches'] for x in outcomes) else 'FAIL','observed':outcomes})
    def keys(path):
        with path.open(encoding='utf-8',newline='') as f:
            return [(r['source_object'],int(r['source_row'])) for r in csv.DictReader(f)]
    raw=keys(ROOT/'restricted/envelopes.csv');q=keys(ROOT/'restricted/quarantine.csv');d=keys(ROOT/'restricted/duplicates.csv')
    c=con.execute('SELECT source_object,source_row FROM c').fetchall();all_output=c+q+d
    conservation=len(raw)==len(all_output) and set(raw)==set(all_output) and len(all_output)==len(set(all_output))
    business=con.execute('SELECT count(*),count(DISTINCT record_id) FROM c').fetchone()
    checks.append({'role':'B','reviewed_role':'C','check':'Independent Python physical-set conservation and SQL business uniqueness',
        'status':'PASS' if conservation and business[0]==business[1] else 'FAIL',
        'observed':{'raw':len(raw),'curated':len(c),'quarantine':len(q),'duplicates':len(d),'union_equal':set(raw)==set(all_output),'disjoint_and_unique':len(all_output)==len(set(all_output)),'business_rows':business[0],'distinct_business_ids':business[1]}})
    obj=trusted['objects'][0];original=(ROOT/'restricted/snapshot'/obj['key']).read_bytes();changed=bytearray(original);changed[-1]^=1
    checks.append({'role':'C','reviewed_role':'A','check':'Independently compare a tampered byte array against trusted SHA-256',
        'status':'PASS' if digest_bytes(bytes(changed))!=obj['sha256'] and digest_bytes(original)==obj['sha256'] else 'FAIL',
        'observed':{'tampered_matches':digest_bytes(bytes(changed))==obj['sha256'],'original_matches':digest_bytes(original)==obj['sha256']}})
    p05=read_json(ROOT/'evidence/gate/P05.json')
    checks.append({'role':'D','reviewed_role':'E','check':'Inspect missing-steward gate case independently',
        'status':'PASS' if p05['actual_result']=='REJECT' and 'METADATA' in p05['measured_failures'] else 'FAIL','observed':{'P05':p05['actual_result'],'reasons':p05['measured_failures']}})
    with (ROOT/'restricted/snapshot/qa_reference.csv').open(encoding='utf-8',newline='') as f:ref={r['record_id']:Decimal(r['reference_c']) for r in csv.DictReader(f)}
    candidate=dict(con.execute('SELECT record_id,temperature_c FROM c').fetchall());matched=set(ref)&set(candidate)
    agree=sum(abs(candidate[k]-ref[k])<=Decimal('0.05') for k in matched)
    quality=read_json(ROOT/'quality_after.json');metric={m['rule_id']:m for m in quality['metrics']}
    qa_ok=len(matched)==metric['Q_QA_COVERAGE']['numerator']==100 and agree==metric['Q_QA_AGREEMENT']['numerator'] and len(matched)==metric['Q_QA_AGREEMENT']['denominator']
    checks.append({'role':'E','reviewed_role':'B','check':'Independent Decimal reference coverage/agreement',
        'status':'PASS' if qa_ok else 'FAIL','observed':{'coverage_n':len(matched),'coverage_d':len(ref),'agreement_n':agree,'agreement_d':len(matched)}})
    write_json(ROOT/'evidence/independent-checks.json',{'checks':checks,'status':'PASS' if all(c['status']=='PASS' for c in checks) else 'FAIL',
        'operator':OPERATOR,'student_hands_on_execution_attested':False,'executed_at':now()})
    con.close()
    return checks
