"""Role E: each mutation uses a fresh relation/metadata/source copy."""
import copy
import shutil
from common import *
from gate import gate

def test_gate():
    cases=[];root=ROOT/'restricted/probes';root.mkdir(exist_ok=True)
    original_catalog=read_json(ROOT/'catalog.json')
    original_lineage=[json.loads(x) for x in (ROOT/'lineage.jsonl').read_text().splitlines() if x]
    for i in range(1,9):
        cid=f'P{i:02d}';folder=root/cid;folder.mkdir(exist_ok=False)
        probe_run=str(uuid.uuid4());probe_inputs=['urn:lab2:gate-probe:'+cid]
        append_event(event('START',probe_run,'gate-probe-'+cid,probe_inputs,[]),ROOT/'evidence/gate-lineage.jsonl')
        cpath=folder/'curated.parquet';shutil.copyfile(ROOT/'candidate/curated.parquet',cpath)
        catalog=copy.deepcopy(original_catalog);lineage=copy.deepcopy(original_lineage);sources=ROOT/'restricted/snapshot'
        changed='none';expected='PASS' if i==1 else 'REJECT'
        con=connect();con.execute('CREATE TABLE probe AS SELECT * FROM read_parquet(?)',[str(cpath)])
        if i==2:
            con.execute("UPDATE probe SET temperature_c=100.00 WHERE record_id=(SELECT min(record_id) FROM probe)");changed='temperature_c=100.00'
        elif i==3:
            con.execute("UPDATE probe SET temperature_c=NULL WHERE record_id=(SELECT min(record_id) FROM probe)");changed='required temperature_c=NULL'
        elif i==4:
            con.execute('INSERT INTO probe SELECT * FROM probe ORDER BY record_id LIMIT 1');changed='appended duplicate physical/business row'
        elif i==5:
            next(e for e in catalog['datasets'] if e['dataset_id']=='curated-candidate').pop('steward');changed='curated metadata steward removed'
        elif i==6:
            sources=folder/'source';shutil.copytree(ROOT/'restricted/snapshot',sources)
            p=sources/'observations_a.csv';data=bytearray(p.read_bytes());data[50]^=1;p.write_bytes(data);changed='one byte in disposable observation A'
        elif i==7:
            next(e for e in lineage if e['eventType']=='COMPLETE' and e['job']['name']=='curate')['outputs'].pop();changed='required curation output removed'
        elif i==8:
            qpath=str(ROOT/'restricted/snapshot/qa_reference.csv')
            rid=con.execute("SELECT c.record_id FROM probe c JOIN read_csv(?,all_varchar=true) q USING(record_id) WHERE abs(c.temperature_c-cast(q.reference_c AS DECIMAL(8,2)))>0.05 ORDER BY c.record_id LIMIT 1",[qpath]).fetchone()
            if not rid:raise RuntimeError('P08 requires a measured mismatching QA ID')
            con.execute('DELETE FROM probe WHERE record_id=?',[rid[0]]);changed='removed mismatching QA ID '+rid[0]
        if i in {2,3,4,8}:
            cpath.unlink();con.execute(f"COPY (SELECT * FROM probe ORDER BY source_object,source_row) TO '{cpath}' (FORMAT PARQUET)")
        con.close()
        result=gate(cpath,catalog,lineage,sources)
        anticipated={2:'VALUE',3:'REQUIRED',4:'RECONCILIATION',5:'METADATA',6:'INPUT_INTEGRITY',7:'LINEAGE',8:'COVERAGE'}.get(i)
        interpreted=result['result']=='PASS' if i==1 else result['result']=='REJECT' and anticipated in result['reasons']
        append_event(event('COMPLETE' if i==1 and interpreted else 'ABORT' if result['result']=='REJECT' else 'FAIL',
            probe_run,'gate-probe-'+cid,probe_inputs,[]),ROOT/'evidence/gate-lineage.jsonl')
        case={'case_id':cid,'changed_artifact':changed,'expected_result':expected,
            'probe_run_id':probe_run,
            'actual_result':result['result'],'correctly_interpreted':interpreted,
            'measured_failures':result['reasons'],'gate':result,'approved_manifest_created':False,
            'operator':OPERATOR,'executed_at':now()}
        write_json(ROOT/f'evidence/gate/{cid}.json',case);cases.append(case)
        print(json.dumps({'case_id':cid,'result':result['result'],'reasons':result['reasons'],'correct':interpreted}))
    summary={'cases':cases,'correct_cases':sum(c['correctly_interpreted'] for c in cases),
        'total_cases':8,'status':'PASS' if all(c['correctly_interpreted'] for c in cases) else 'FAIL',
        'mutated_raw_objects':False,'operator':OPERATOR,'executed_at':now()}
    write_json(ROOT/'gate-tests.json',summary)
    if summary['status']!='PASS':raise RuntimeError('GATE_HARNESS_FAILED')
    return summary
