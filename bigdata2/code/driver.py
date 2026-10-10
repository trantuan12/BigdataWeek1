"""Curator entry point. It cannot access the release bucket."""
from common import *
from stage_inputs import stage
from checks import partition, replay, edge_cases
from quality_report import collect, chart
from metadata import make_catalog, traces, retention_plan

def run():
    os.chdir(ROOT)
    (ROOT/'evidence').mkdir(exist_ok=True);(ROOT/'candidate').mkdir(exist_ok=True)
    if (ROOT/'lineage.jsonl').exists(): raise RuntimeError('Fresh work directory required for each run')
    contract=read_json(ROOT/'contract.json');validate_json(contract,ROOT/'schemas/observation_contract_schema.json')
    bundle=code_bundle();write_json(ROOT/'evidence/code-bundle.json',bundle)
    (ROOT/'trusted-manifest.sha256').write_text(sha(trusted_path())+'  trusted_manifest.json\n')
    run_id=str(uuid.uuid4());start=now();stageprefix='lab2/staging/'+run_id+'/'
    inputs=['s3://research-raw/'+PREFIX+x for x in ['observations_a.csv','observations_b.jsonl','sensors.csv']]+['urn:sha256:'+sha(ROOT/'contract.json')]
    outputs=['s3://research-raw/'+stageprefix+x for x in ['candidate/curated.parquet','restricted/quarantine.csv','restricted/duplicates.csv']]
    append_event(event('START',run_id,'curate',inputs,[]))
    try:
        manifest=stage();con=engine()
        counts=partition(con)
        if counts['status']!='PASS': raise RuntimeError('CONSERVATION')
        write_json(ROOT/'evidence/conservation.json',{'check_id':'C01',**counts,'operator':OPERATOR,'executed_at':now()})
        intake={'check_id':'I02','status':'PASS','rows':counts['counts']['raw_envelopes'],
            'physical_key_duplicates':con.execute('SELECT count(*) FROM (SELECT source_object,source_row,count(*) n FROM raw_envelopes GROUP BY ALL HAVING n>1)').fetchone()[0],
            'parse_failures_retained':con.execute('SELECT count(*) FROM raw_envelopes WHERE NOT cast(parse_ok AS BOOLEAN)').fetchone()[0],
            'by_source':rows(con,'SELECT source_object,count(*) AS records FROM raw_envelopes GROUP BY source_object ORDER BY source_object')}
        if intake['rows']!=10205 or intake['physical_key_duplicates']:raise RuntimeError('ENVELOPE_INVENTORY')
        write_json(ROOT/'evidence/envelope-check.json',intake)
        for relation,path,fmt in [('curated','candidate/curated.parquet','PARQUET'),('quarantine','restricted/quarantine.csv','CSV, HEADER'),('duplicates','restricted/duplicates.csv','CSV, HEADER')]:
            con.execute(f"COPY (SELECT * FROM {relation} ORDER BY source_object,source_row) TO '{path}' (FORMAT {fmt})")
        measured=schema_of(con,"read_parquet('candidate/curated.parquet')")
        schema={'check_id':'C03','status':'PASS' if measured==ALLOWLIST else 'FAIL','columns':[{'name':n,'type':t} for n,t in measured],
            'restricted_columns_absent':True,'operator':OPERATOR,'executed_at':now()}
        write_json(ROOT/'evidence/schema-check.json',schema)
        recheck=replay(con,manifest);write_json(ROOT/'evidence/replay-check.json',recheck)
        edges=edge_cases();write_json(ROOT/'evidence/edge-cases.json',edges)
        if any(x['status']!='PASS' for x in [schema,recheck,edges]):raise RuntimeError('TRANSFORMATION_CHECK')
        metrics,envelope=collect(con,run_id,contract,bundle);chart(metrics)
        make_catalog(con,run_id,bundle,read_json(ROOT/'input_inventory.json'));traces(con);retention_plan()
        end=now()
        art=[artifact(ROOT/path,stageprefix+path,counts['counts'][relation]) for relation,path in [
            ('curated','candidate/curated.parquet'),('quarantine','restricted/quarantine.csv'),('duplicates','restricted/duplicates.csv')]]
        write_json(ROOT/'candidate/artifact-manifest.json',{'run_id':run_id,'store_id':STORE,'artifacts':art})
        record={'run_id':run_id,'job':'curate','start_time':start,'end_time':end,'as_of':contract['as_of'],
            'store_id':STORE,'input_manifest_sha256':sha(trusted_path()),'contract_sha256':sha(ROOT/'contract.json'),
            'code_sha256':bundle['sha256'],'software':runtime(),'outputs':art,
            'operator':OPERATOR,'human_role_assignment':'24022463 - Đàm Quang Tiến / Role C',
            'reviewer':'Separate publisher process; human review is not attested by this record',
            'status':'COMPLETE','reason_codes':[],'stage_prefix':stageprefix,
            'execution_evidence':{'automated_execution':True,'student_live_demonstrations_attested':False}}
        write_json(ROOT/'run-record.json',record)
        client=s3();uploaded=[]
        for p in sorted(ROOT.rglob('*')):
            if not p.is_file() or '__pycache__' in p.parts or p.suffix=='.pyc':continue
            rel=p.relative_to(ROOT).as_posix()
            if rel.startswith(('input-upload/','environment/')) or rel in {'restricted/replay-envelopes.csv','restricted/edge-envelopes.csv'}:continue
            key=stageprefix+rel;data=p.read_bytes();client.put_object(Bucket='research-raw',Key=key,Body=data)
            if digest_bytes(get_bytes(client,'research-raw',key))!=digest_bytes(data):raise RuntimeError('STAGE_UPLOAD_HASH')
            uploaded.append({'key':key,'sha256':sha(p),'bytes':len(data)})
        record['end_time']=now();write_json(ROOT/'run-record.json',record)
        append_event(event('COMPLETE',run_id,'curate',inputs,outputs))
        for rel in ['run-record.json','lineage.jsonl']:
            p=ROOT/rel;key=stageprefix+rel;data=p.read_bytes()
            client.put_object(Bucket='research-raw',Key=key,Body=data)
            if digest_bytes(get_bytes(client,'research-raw',key))!=sha(p):raise RuntimeError('FINAL_AUDIT_UPLOAD_HASH')
            uploaded=[x for x in uploaded if x['key']!=key]+[{'key':key,'sha256':sha(p),'bytes':len(data)}]
        write_json(ROOT/'evidence/staging-upload.json',{'run_id':run_id,'status':'PASS','objects':uploaded,'executed_at':now()})
        print(json.dumps({'status':'COMPLETE','run_id':run_id,'stage_prefix':stageprefix,'counts':counts['counts']}))
        con.close()
    except Exception as exc:
        append_event(event('FAIL',run_id,'curate',inputs,[]));write_json(ROOT/'evidence/curation-failure.json',{'reason':str(exc),'time':now()});raise

if __name__=='__main__': run()
