"""Role D: measured catalog, traceability and retention without a catalog server."""
from datetime import timedelta
from common import *

MEANINGS={
 'record_id':('Normalized business observation identifier',None),
 'sensor_id':('Normalized sensor identifier used to join the registry',None),
 'site':('Site derived exclusively from sensors.csv',None),
 'event_time_utc':('Measurement time, interpreted in UTC','UTC'),
 'ingest_time_utc':('Recorded arrival time, interpreted in UTC','UTC'),
 'temperature_c':('Selected observation converted to Celsius; validated before rounding','Celsius'),
 'is_late':('True when recorded arrival lag exceeds 900 seconds',None),
 'source_object':('Restricted input object identifier; does not grant access',None),
 'source_row':('One-based physical data-record ordinal; CSV header excluded',None),
 'source_sha256':('SHA-256 of the complete original source object',None),
 'event_time':('Original source measurement timestamp','UTC'),
 'ingest_time':('Original source arrival timestamp','UTC'),
 'reading':('Original source measured value','source unit'),
 'unit':('Original source C or F unit',None),
 'operator_email':('Optional restricted synthetic operator attribute',None),
 'parse_ok':('Whether the physical record could be decoded into an object',None),
 'raw_payload':('Restricted original physical record payload',None),
 'primary_reason':('First failing reason in the frozen contract precedence',None),
 'all_reasons':('All failing reasons joined by a vertical bar',None)}

def column_metadata(con,relation):
    result=[]
    for name,kind in schema_of(con,relation):
        meaning,unit=MEANINGS.get(name,('Recorded physical provenance attribute',None))
        nullable=bool(con.execute(f'SELECT count(*) FROM {relation} WHERE "{name}" IS NULL').fetchone()[0])
        result.append({'name':name,'type':kind,'meaning':meaning,'unit':unit,'nullable':nullable})
    return result

def make_catalog(con, run_id, bundle, inventory):
    stage='s3://research-raw/lab2/staging/'+run_id+'/'
    base={'purpose':'Curated synthetic research telemetry for the Big Data Lab 2 teaching exercise',
        'owner':'24022484 - Trần Anh Tuấn / Role E (designated release owner)',
        'steward':'24022277 - Lê Ngọc Minh Cường / Role D (designated metadata steward)',
        'allowed_use':'Synthetic teaching use only; no real-world sensor accuracy claim.',
        'schema_version':'research-telemetry-v1','as_of':'2026-02-09T00:00:00Z',
        'input_manifest_sha256':sha(trusted_path()),'contract_sha256':sha(ROOT/'contract.json'),
        'code_sha256':bundle['sha256'],'quality_report':stage+'quality_after.json',
        'provenance':{'store_id':STORE,'job':'curate','run_id':run_id,
            'run_record':stage+'run-record.json','duplicate_ledger':stage+'restricted/duplicates.csv'}}
    entries=[]
    specs=[('raw-observations','Raw observation collection','RESTRICTED','raw_envelopes',30,'retrieval_time',
        ['s3://research-raw/'+PREFIX+x for x in ['observations_a.csv','observations_b.jsonl']]),
       ('quarantine','Rejected physical observations','RESTRICTED','quarantine',7,'run_completion_time',[stage+'restricted/quarantine.csv']),
       ('curated-candidate','Curated telemetry candidate','INTERNAL_APPROVED_ON_PUBLICATION','curated',90,'publication_time',[stage+'candidate/curated.parquet'])]
    for did,title,classification,relation,days,basis,locations in specs:
        entries.append({**base,'dataset_id':did,'title':title,'classification':classification,
            'location':locations,'columns':column_metadata(con,relation),
            'row_count':con.execute(f'SELECT count(*) FROM {relation}').fetchone()[0],
            'retention':{'days':days,'start_basis':basis,'hold':False,'dependent_release_ids':[],
                'hold_rule':'Hold overrides expiry','dependency_rule':'Active approved release overrides expiry',
                'audit_min_days':90,'action':'dry-run only'}})
    entries[0]['source_aliases']={'observations_a.csv':{'record_id':'record_id','sensor_id':'sensor_id','event_time':'event_time','ingest_time':'ingest_time','reading':'reading','unit':'unit','operator_email':'operator_email'},
        'observations_b.jsonl':{'record_id':'id','sensor_id':'sensor','event_time':'timestamp','ingest_time':'arrived_at','reading':'temperature','unit':'temperature_unit','operator_email':'operator'}}
    entries[0]['provenance']={**entries[0]['provenance'],'retrieval_times':{o['key']:o['retrieved_at'] for o in inventory['objects']}}
    write_json(ROOT/'catalog.json',{'profile':'lab2-catalog-v1','datasets':entries})
    for entry in entries: validate_json(entry,ROOT/'schemas/metadata_catalog_schema.json')
    m01=[{k:e[k] for k in ['dataset_id','owner','steward','row_count','location','schema_version']}
         for e in entries if e['dataset_id']=='curated-candidate' and any(c['name']=='temperature_c' and c['unit']=='Celsius' for c in e['columns']) and e['quality_report']]
    m02=[{k:e[k] for k in ['dataset_id','classification','retention']} for e in entries if e['classification']=='RESTRICTED' and e['retention']['days']>0 and e['retention']['hold_rule'] and e['retention']['dependency_rule']]
    write_json(ROOT/'evidence/catalog-queries.json',{'M01':m01,'M02':m02,'release_has_email':False,
        'semantic_counts_and_hashes':'Recomputed separately by publisher gate G08','operator':OPERATOR,'executed_at':now()})
    return entries

def traces(con):
    accepted=rows(con,'SELECT * FROM curated ORDER BY record_id LIMIT 1')[0]
    rejected=rows(con,'SELECT record_id,source_object,source_row,source_sha256,primary_reason,all_reasons FROM quarantine ORDER BY source_object,source_row LIMIT 1')[0]
    a=accepted['source_object'];r=accepted['source_row']
    winner=rows(con,'SELECT record_id,source_object,source_row,version_rank,unit,value_num,temperature_unrounded FROM evaluated WHERE source_object=? AND source_row=?',[a,r])[0]
    result={'accepted':{'output':accepted,'selected_version':winner,'rule':'Trim/uppercase identifiers; exact UTC parsing; C/F conversion; registry-derived site; DECIMAL(8,2) after validation'},
        'quarantined':rejected,'operator':OPERATOR,'executed_at':now()}
    write_json(ROOT/'evidence/traces.json',result)
    return result

def retention_plan():
    t=datetime.now(timezone.utc)
    fixtures=[('expired-scratch',1,t-timedelta(days=3),False,[],'ELIGIBLE','EXPIRED'),
        ('held-quarantine',7,t-timedelta(days=10),True,[],'KEEP','HOLD'),
        ('dependent-raw',30,t-timedelta(days=40),False,['active-approved-release'],'KEEP','ACTIVE_DEPENDENCY'),
        ('unexpired',90,t-timedelta(days=1),False,[],'KEEP','NOT_EXPIRED')]
    decisions=[]
    for name,days,start,hold,deps,expect,reason in fixtures:
        observed='HOLD' if hold else 'ACTIVE_DEPENDENCY' if deps else 'NOT_EXPIRED' if t<start+timedelta(days=days) else 'EXPIRED'
        action='ELIGIBLE' if observed=='EXPIRED' else 'KEEP'
        decisions.append({'object':name,'clock_start':start.isoformat(),'evaluation_time':t.isoformat(),
            'days':days,'hold':hold,'active_dependent_release_ids':deps,'decision':action,
            'reason':observed,'expected':expect+'/'+reason,'passed':action==expect and observed==reason,'deleted':False})
    write_json(ROOT/'retention-plan.json',{'policy':{'raw':{'days':30,'start_basis':'retrieval_time'},
        'quarantine':{'days':7,'start_basis':'run_completion_time'},'approved_release':{'days':90,'start_basis':'publication_time'},
        'audit':{'minimum_days':90,'retain_through_dependent_release_expiry':True}},
        'precedence':['HOLD','ACTIVE_DEPENDENCY','NOT_EXPIRED','ELIGIBLE'],
        'mode':'dry-run only','decisions':decisions,'operator':OPERATOR,'executed_at':now()})
