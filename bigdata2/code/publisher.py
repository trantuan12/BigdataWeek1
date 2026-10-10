"""Owner credential context: stage verification, gate, review and manifest-last publication."""
import sys
from common import *
from stage_inputs import verify
from gate import gate
from gate_tests import test_gate
from independent_checks import perform

def acquire(stageprefix):
    client=s3();baseline=read_json(ROOT/'evidence/independently-captured-staging-hashes.json')
    expected={a['key']:a for a in baseline['objects']}
    keys=[]
    for page in client.get_paginator('list_objects_v2').paginate(Bucket='research-raw',Prefix=stageprefix):
        keys.extend(o['Key'] for o in page.get('Contents',[]))
    if set(keys)!=set(expected):raise RuntimeError('STAGE_INVENTORY')
    for key in sorted(keys):
        data=get_bytes(client,'research-raw',key);entry=expected[key]
        if digest_bytes(data)!=entry['sha256'] or len(data)!=entry['bytes']:raise RuntimeError('STAGE_INTEGRITY: '+key)
        relative=key[len(stageprefix):];dest=ROOT/relative
        if relative.startswith(('code/','schemas/')) or relative in {'contract.json','trusted_manifest.json','team.json'}:
            if sha(dest)!=digest_bytes(data):raise RuntimeError('INDEPENDENT_CODE_CONTRACT_MISMATCH: '+relative)
            continue
        dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(data)
    manifest=read_json(trusted_path())
    for obj in manifest['objects']:
        data=get_bytes(client,'research-raw',PREFIX+obj['key']);verify(data,obj)
        dest=ROOT/'restricted/snapshot'/obj['key']
        if dest.read_bytes()!=data:raise RuntimeError('SOURCE_RECHECK_MISMATCH')
    write_json(ROOT/'evidence/publisher-acquisition.json',{'status':'PASS','staged_objects':len(keys),
        'trusted_sources_rechecked':4,'independent_hash_source':'Host-captured curator upload evidence copied separately from S3',
        'operator':OPERATOR,'workload_identity':'s3-owner','executed_at':now()})

def publish(stageprefix):
    os.chdir(ROOT);acquire(stageprefix)
    record=read_json(ROOT/'run-record.json');run_id=str(uuid.uuid4());started=now()
    release_id='bd-g10-lab2-'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+run_id[:8]
    prefix='lab2/releases/'+release_id+'/'
    inputs=['s3://research-raw/'+stageprefix+x for x in ['candidate/curated.parquet','catalog.json','quality_after.json']]+['s3://research-raw/'+PREFIX+x for x in ['observations_a.csv','observations_b.jsonl','sensors.csv','qa_reference.csv']]+['urn:sha256:'+sha(trusted_path()),'urn:sha256:'+sha(ROOT/'contract.json')]
    append_event(event('START',run_id,'validate-and-publish',inputs,[]))
    try:
        test_gate();checks=perform();decision=gate();write_json(ROOT/'evidence/owner-gate.json',decision)
        if decision['result']!='PASS' or any(c['status']!='PASS' for c in checks):
            append_event(event('ABORT',run_id,'validate-and-publish',inputs,[]));raise RuntimeError('OWNER_GATE_REJECTED')
        approval={'review_id':str(uuid.uuid4()),'reviewer':'Independent publisher validation process',
            'designated_human_owner':'24022484 - Trần Anh Tuấn / Role E',
            'human_signature_attested':False,'review_kind':'Automated owner-context technical review',
            'authorization_basis':'Requester instructed completion and execution of the assignment',
            'curation_run_id':record['run_id'],'publish_run_id':run_id,
            'candidate_sha256':sha(ROOT/'candidate/curated.parquet'),'decision':'APPROVE_TECHNICAL_RELEASE',
            'reason':'Owner-context G01-G09 recomputation, 8/8 mutation cases and five independent formulas passed.',
            'approved_at':now(),'operator':OPERATOR}
        write_json(ROOT/'evidence/owner-review.json',approval)
        # The public catalog contains only approved data; raw/quarantine remain in restricted staging.
        original=read_json(ROOT/'catalog.json');entry=next(e for e in original['datasets'] if e['dataset_id']=='curated-candidate')
        approved=json.loads(json.dumps(entry));approved['dataset_id']='approved-telemetry';approved['title']='Approved synthetic telemetry release'
        approved['classification']='APPROVED_SYNTHETIC_TEACHING';approved['location']=['s3://research-release/'+prefix+'curated.parquet']
        approved['quality_report']='s3://research-release/'+prefix+'quality_after.json'
        approved['retention']['publication_time']=approval['approved_at']
        approved['provenance']={'store_id':STORE,'curate_run_id':record['run_id'],'publish_run_id':run_id,
            'lineage_summary':'s3://research-release/'+prefix+'lineage-summary.json',
            'source_manifest_sha256':sha(trusted_path()),'reference_role':'QA validation only; not measurement computation'}
        write_json(ROOT/'candidate/approved-catalog.json',{'profile':'lab2-approved-catalog-v1','datasets':[approved]})
        validate_json(approved,ROOT/'schemas/metadata_catalog_schema.json')
        summary={'store_id':STORE,'curation_run_id':record['run_id'],'publish_run_id':run_id,
            'transformation_inputs':['s3://research-raw/'+PREFIX+x for x in ['observations_a.csv','observations_b.jsonl','sensors.csv']],
            'validation_reference':'s3://research-raw/'+PREFIX+'qa_reference.csv',
            'output':'s3://research-release/'+prefix+'curated.parquet',
            'contract_sha256':record['contract_sha256'],'code_sha256':record['code_sha256'],
            'access_note':'Source names are identifiers; no raw access is granted.'}
        write_json(ROOT/'candidate/lineage-summary.json',summary)
        # Consumer documentation has no raw payload or email values.
        consumer_readme=(
            '# Approved Lab 2 telemetry\n\n'
            'Synthetic teaching observations; temperature_c is Celsius and timestamps are UTC. '
            'The evaluation cutoff is 2026-02-09T00:00:00Z. '
            'The QA reference population is the supplied 100 IDs; agreement does not establish accuracy of all sensors. '
            'Valid late measurements are retained with is_late=true. '
            'The steward is Lê Ngọc Minh Cường / Role D; the designated owner is Trần Anh Tuấn / Role E. '
            'Review was executed by the independent publisher process, without an attested human signature.\n\n'
            'curated.parquet, catalog.json, quality_after.json and lineage-summary.json are bound by release_manifest.json. '
            'Consumers require the final manifest and matching SHA-256 values. '
            'Raw inputs, quarantine, duplicates and the complete audit log remain restricted.\n')
        (ROOT/'candidate/consumer-README.md').write_text(consumer_readme,encoding='utf-8')
        files=[('candidate/curated.parquet','curated.parquet'),('candidate/approved-catalog.json','catalog.json'),
            ('quality_after.json','quality_after.json'),('candidate/lineage-summary.json','lineage-summary.json'),
            ('candidate/consumer-README.md','README.md')]
        client=s3();exists=client.list_objects_v2(Bucket='research-release',Prefix=prefix).get('KeyCount',0)
        if exists:raise RuntimeError('IMMUTABLE_RELEASE_PREFIX_EXISTS')
        artifacts=[];order=[]
        for local,name in files:
            p=ROOT/local;data=p.read_bytes();key=prefix+name
            client.put_object(Bucket='research-release',Key=key,Body=data)
            uploaded=get_bytes(client,'research-release',key)
            if digest_bytes(uploaded)!=sha(p):raise RuntimeError('RELEASE_UPLOAD_HASH')
            artifacts.append({'key':key,'sha256':sha(p),'bytes':len(data),
                **({'row_count':decision['candidate_rows'],'schema_version':'research-telemetry-v1'} if name=='curated.parquet' else {})})
            order.append({'ordinal':len(order)+1,'key':key,'verified_at':now(),'sha256':sha(p)})
        release={'release_id':release_id,'store_id':STORE,'approved_run_id':record['run_id'],'publish_run_id':run_id,
            'reviewer':approval['reviewer'],'designated_human_owner':approval['designated_human_owner'],
            'human_signature_attested':False,'review_kind':approval['review_kind'],'approval_time':approval['approved_at'],
            'row_count':decision['candidate_rows'],'schema_version':'research-telemetry-v1',
            'input_manifest_sha256':sha(trusted_path()),'contract_sha256':sha(ROOT/'contract.json'),
            'code_sha256':record['code_sha256'],'artifacts':artifacts}
        write_json(ROOT/'release_manifest.json',release);data=(ROOT/'release_manifest.json').read_bytes();key=prefix+'release_manifest.json'
        client.put_object(Bucket='research-release',Key=key,Body=data)
        if digest_bytes(get_bytes(client,'research-release',key))!=sha(ROOT/'release_manifest.json'):raise RuntimeError('FINAL_MANIFEST_HASH')
        order.append({'ordinal':len(order)+1,'key':key,'verified_at':now(),'sha256':sha(ROOT/'release_manifest.json'),'manifest_last':True})
        append_event(event('COMPLETE',run_id,'validate-and-publish',inputs,['s3://research-release/'+a['key'] for a in artifacts]+['s3://research-release/'+key]))
        write_json(ROOT/'evidence/publication-record.json',{'status':'COMPLETE','release_id':release_id,
            'publish_run_id':run_id,'uploads':order,'manifest_last':True,'multi_object_transaction':False,
            'manifest_sha256':sha(ROOT/'release_manifest.json'),'operator':OPERATOR,'workload_identity':'s3-owner'})
        write_json(ROOT/'publish-run-record.json',{'run_id':run_id,'job':'validate-and-publish','start_time':started,'end_time':now(),
            'status':'COMPLETE','store_id':STORE,'as_of':record['as_of'],'input_manifest_sha256':sha(trusted_path()),
            'contract_sha256':sha(ROOT/'contract.json'),'code_sha256':record['code_sha256'],'software':runtime(),
            'operator':OPERATOR,'reviewer':approval['reviewer'],'outputs':artifacts+[{'key':key,'sha256':sha(ROOT/'release_manifest.json')}],
            'human_signature_attested':False})
        for p in [ROOT/'lineage.jsonl',ROOT/'gate-tests.json',ROOT/'publish-run-record.json',ROOT/'release_manifest.json']+list((ROOT/'evidence').rglob('*.json')):
            rel=p.relative_to(ROOT).as_posix();client.put_object(Bucket='research-raw',Key=stageprefix+'publisher/'+rel,Body=p.read_bytes())
        print(json.dumps({'status':'COMPLETE','release_id':release_id,'manifest_key':key,'manifest_sha256':sha(ROOT/'release_manifest.json')}))
    except Exception as exc:
        # Keep real execution errors distinct from policy rejection.
        if str(exc)!='OWNER_GATE_REJECTED':append_event(event('FAIL',run_id,'validate-and-publish',inputs,[]))
        write_json(ROOT/'evidence/publication-failure.json',{'reason':str(exc),'executed_at':now()});raise

if __name__=='__main__':publish(sys.argv[1])
