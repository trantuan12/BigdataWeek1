"""Role E. Fail-closed release checks recomputed from actual bytes and relations."""
from common import *
from checks import partition

def gate(candidate_path=None, catalog=None, lineage=None, sources=None, expected=None):
    contract=read_json(ROOT/'contract.json');manifest=read_json(trusted_path())
    record=read_json(ROOT/'run-record.json');bundle=code_bundle()
    candidate_path=Path(candidate_path or ROOT/'candidate/curated.parquet')
    catalog=catalog if catalog is not None else read_json(ROOT/'catalog.json')
    lineage=lineage if lineage is not None else [json.loads(x) for x in (ROOT/'lineage.jsonl').read_text().splitlines() if x]
    sources=Path(sources or ROOT/'restricted/snapshot')
    expected=expected or read_json(ROOT/'candidate/artifact-manifest.json')
    observations=[]
    def check(cid, function):
        try:
            details,reasons=function()
            observations.append({'check_id':cid,'status':'PASS' if not reasons else 'REJECT','observed':details,'reasons':reasons})
        except Exception as exc:
            from jsonschema.exceptions import ValidationError
            validation_failure=cid=='G08' and isinstance(exc,(ValidationError,KeyError,StopIteration))
            observations.append({'check_id':cid,'status':'REJECT' if validation_failure else 'BLOCKED',
                'observed':{'error_type':type(exc).__name__,'message':str(exc)},
                'reasons':['METADATA' if validation_failure else 'CHECK_EXECUTION']})
    con=engine();path=str(candidate_path).replace("'","''")
    try:
        con.execute(f"CREATE TABLE submitted AS SELECT * FROM read_parquet('{path}')")
    except Exception as exc:
        con.close();return {'result':'BLOCKED','checks':[],'reasons':['CANDIDATE_UNREADABLE'],'error':str(exc),'evaluated_at':now()}
    n=con.execute('SELECT count(*) FROM submitted').fetchone()[0]
    def integrity():
        objects=[]
        for o in manifest['objects']:
            p=sources/o['key'];objects.append({'key':o['key'],'matches':p.stat().st_size==o['bytes'] and sha(p)==o['sha256']})
        staged=[]
        for a in expected['artifacts']:
            p=candidate_path if a['path']=='candidate/curated.parquet' else ROOT/a['path']
            staged.append({'path':a['path'],'matches':sha(p)==a['sha256'] and p.stat().st_size==a['bytes']})
        provenance=con.execute("SELECT count(*) FROM submitted c LEFT JOIN raw_envelopes r ON c.source_object=r.source_object AND c.source_row=cast(r.source_row AS BIGINT) WHERE r.source_object IS NULL OR c.source_sha256 IS DISTINCT FROM r.source_sha256").fetchone()[0]
        binding=record['input_manifest_sha256']==sha(trusted_path()) and record['contract_sha256']==sha(ROOT/'contract.json') and record['code_sha256']==bundle['sha256']
        reasons=[]
        if not all(x['matches'] for x in objects) or provenance:reasons.append('INPUT_INTEGRITY')
        if not all(x['matches'] for x in staged):reasons.append('STAGED_INTEGRITY')
        if not binding:reasons.append('EXECUTION_BINDING')
        return {'sources':objects,'staged_artifacts':staged,'provenance_mismatches':provenance,'run_bindings_match':binding},reasons
    check('G01',integrity)
    def conservation():
        result=partition(con,'submitted');return result,[] if result['status']=='PASS' else ['RECONCILIATION']
    check('G02',conservation)
    def schema():
        measured=schema_of(con,'submitted');ok=measured==ALLOWLIST and n>0
        return {'columns':measured,'rows':n,'allowlist_equal':measured==ALLOWLIST},[] if ok else ['SCHEMA']
    check('G03',schema)
    def required():
        nulls={name:con.execute(f'SELECT count(*) FROM submitted WHERE "{name}" IS NULL').fetchone()[0] for name,_ in ALLOWLIST}
        blank_values=con.execute("SELECT count(*) FROM submitted WHERE blank(record_id) IS NULL OR blank(sensor_id) IS NULL OR blank(site) IS NULL OR blank(source_object) IS NULL OR blank(source_sha256) IS NULL").fetchone()[0]
        original=con.execute('SELECT count(*) FROM submitted s LEFT JOIN evaluated e ON s.source_object=e.source_object AND s.source_row=e.source_row WHERE NOT coalesce(e.complete_ok,false)').fetchone()[0]
        return {'nulls':nulls,'blank_required_strings':blank_values,'original_required_failures':original},['REQUIRED'] if any(nulls.values()) or blank_values or original else []
    check('G04',required)
    def values():
        bad=con.execute('SELECT count(*) FROM submitted WHERE NOT coalesce(isfinite(temperature_c) AND temperature_c BETWEEN ? AND ?,false)',[contract['temperature_range_c']['min'],contract['temperature_range_c']['max']]).fetchone()[0]
        duplicate=n-con.execute('SELECT count(DISTINCT record_id) FROM submitted').fetchone()[0]
        unknown=con.execute('SELECT count(*) FROM submitted c LEFT JOIN registry r ON c.sensor_id=upper(blank(r.sensor_id)) WHERE r.sensor_id IS NULL OR c.site IS DISTINCT FROM r.site').fetchone()[0]
        # This compares submitted values with conversion from the selected source, without QA repair.
        transformed=con.execute('SELECT count(*) FROM submitted s LEFT JOIN curated c ON s.source_object=c.source_object AND s.source_row=c.source_row WHERE c.record_id IS NULL OR s.record_id IS DISTINCT FROM c.record_id OR s.temperature_c IS DISTINCT FROM c.temperature_c OR s.sensor_id IS DISTINCT FROM c.sensor_id').fetchone()[0]
        reasons=[]
        if bad or transformed:reasons.append('VALUE')
        if duplicate:reasons.append('UNIQUENESS')
        if unknown:reasons.append('REFERENCE')
        return {'invalid_values':bad,'duplicate_business_keys':duplicate,'unknown_sensor_or_wrong_site':unknown,'source_conversion_mismatches':transformed},reasons
    check('G05',values)
    def times():
        valid=con.execute('SELECT count(*) FROM submitted WHERE event_time_utc<=ingest_time_utc AND ingest_time_utc<=?', [contract['as_of']]).fetchone()[0]
        timely=con.execute('SELECT count(*) FROM submitted WHERE event_time_utc<=ingest_time_utc AND ingest_time_utc<=? AND date_diff(\'second\',event_time_utc,ingest_time_utc)<=?',[contract['as_of'],contract['late_threshold_seconds']]).fetchone()[0]
        flag=con.execute('SELECT count(*) FROM submitted WHERE is_late IS DISTINCT FROM (date_diff(\'second\',event_time_utc,ingest_time_utc)>?)',[contract['late_threshold_seconds']]).fetchone()[0]
        source_time=con.execute('SELECT count(*) FROM submitted s JOIN curated c ON s.source_object=c.source_object AND s.source_row=c.source_row WHERE s.event_time_utc IS DISTINCT FROM c.event_time_utc OR s.ingest_time_utc IS DISTINCT FROM c.ingest_time_utc').fetchone()[0]
        ratio=timely/valid if valid else None
        ok=valid==n and ratio is not None and ratio>=contract['release_thresholds']['timeliness'] and flag==0 and source_time==0
        return {'valid':valid,'rows':n,'timely':timely,'denominator':valid,'value':ratio,'late_flag_errors':flag,'source_timestamp_mismatches':source_time},[] if ok else ['TIME']
    check('G06',times)
    def reference():
        matched=con.execute('SELECT count(DISTINCT q.record_id) FROM qa_reference q JOIN submitted c USING(record_id)').fetchone()[0]
        total=con.execute('SELECT count(*) FROM qa_reference').fetchone()[0]
        agree=con.execute('SELECT count(DISTINCT q.record_id) FROM qa_reference q JOIN submitted c USING(record_id) WHERE abs(c.temperature_c-cast(q.reference_c AS DECIMAL(8,2)))<=0.05').fetchone()[0]
        ratio=agree/matched if matched else None;reasons=[]
        if total!=100 or matched!=100:reasons.append('COVERAGE')
        if ratio is None or ratio<contract['release_thresholds']['qa_agreement']:reasons.append('AGREEMENT')
        return {'matched':matched,'reference_population':total,'coverage':matched/total if total else None,'agree':agree,'agreement_denominator':matched,'agreement':ratio,'tolerance_c':'0.05'},reasons
    check('G07',reference)
    def metadata():
        entries=catalog['datasets'];errors=[]
        actual={'raw-observations':('raw_envelopes',10205),'quarantine':('quarantine',con.execute('SELECT count(*) FROM quarantine').fetchone()[0]),'curated-candidate':('submitted',n)}
        if {e['dataset_id'] for e in entries}!=set(actual):errors.append('dataset inventory')
        for e in entries:
            validate_json(e,ROOT/'schemas/metadata_catalog_schema.json')
            relation,count=actual[e['dataset_id']]
            if e['row_count']!=count:errors.append(e['dataset_id']+': row count')
            if [(c['name'],c['type']) for c in e['columns']]!=schema_of(con,relation):errors.append(e['dataset_id']+': schema')
            if len({c['name'] for c in e['columns']})!=len(e['columns']):errors.append('column uniqueness')
            for c in e['columns']:
                actual_nullable=bool(con.execute(f'SELECT count(*) FROM {relation} WHERE "{c["name"]}" IS NULL').fetchone()[0])
                if c['nullable']!=actual_nullable:errors.append(c['name']+': measured nullability')
            if e['as_of']!=contract['as_of']:errors.append('cutoff')
            for key,value in [('input_manifest_sha256',sha(trusted_path())),('contract_sha256',sha(ROOT/'contract.json')),('code_sha256',bundle['sha256'])]:
                if e[key]!=value:errors.append(key)
            expected_locations=([f's3://research-raw/{PREFIX}observations_a.csv',f's3://research-raw/{PREFIX}observations_b.jsonl'] if e['dataset_id']=='raw-observations' else ['s3://research-raw/'+record['stage_prefix']+('restricted/quarantine.csv' if e['dataset_id']=='quarantine' else 'candidate/curated.parquet')])
            if e['location']!=expected_locations:errors.append('locations')
            if e['quality_report']!='s3://research-raw/'+record['stage_prefix']+'quality_after.json':errors.append('quality link')
            if e['provenance']['run_id']!=record['run_id'] or e['provenance']['store_id']!=STORE:errors.append('provenance')
            if not e['retention'].get('hold_rule') or not e['retention'].get('dependency_rule'):errors.append('retention policy')
        curated=next(e for e in entries if e['dataset_id']=='curated-candidate')
        temp=next(c for c in curated['columns'] if c['name']=='temperature_c')
        if temp['unit']!='Celsius':errors.append('temperature unit')
        report=read_json(ROOT/'quality_after.json')
        if report['run_id']!=record['run_id'] or report['counts']['curated']!=n:errors.append('quality report identity/count')
        # Recompute the submitted metric records instead of accepting reported booleans.
        con.execute('CREATE OR REPLACE VIEW actual_curated AS SELECT * FROM curated')
        con.execute('CREATE OR REPLACE VIEW curated AS SELECT * FROM submitted')
        con.execute((ROOT/'code/quality.sql').read_text())
        observed={r['rule_id']:(r['numerator'],r['denominator']) for r in rows(con,"SELECT * FROM metric_counts WHERE phase='after'")}
        for m in report['metrics']:
            if observed.get(m['rule_id'])!=(m['numerator'],m['denominator']):errors.append(m['rule_id']+': quality counters')
        return {'dataset_records':len(entries),'semantic_errors':errors,'measured_rows':n},['METADATA'] if errors else []
    check('G08',metadata)
    def lineage_replay():
        for e in lineage:validate_json(e,ROOT/'schemas/OpenLineage-2-0-2.json')
        events=[e for e in lineage if e['run']['runId']==record['run_id'] and e['job']['name']=='curate']
        reasons=[]
        if len(events)!=2 or [e['eventType'] for e in events]!=['START','COMPLETE']:reasons.append('curation pair')
        expected_outputs={'s3://research-raw/'+record['stage_prefix']+x for x in ['candidate/curated.parquet','restricted/quarantine.csv','restricted/duplicates.csv']}
        completed=next((e for e in events if e['eventType']=='COMPLETE'),None)
        if not completed or {d['name'] for d in completed['outputs']}!=expected_outputs:reasons.append('curation outputs')
        expected_inputs={'s3://research-raw/'+PREFIX+x for x in ['observations_a.csv','observations_b.jsonl','sensors.csv']}|{'urn:sha256:'+sha(ROOT/'contract.json')}
        if not all({d['name'] for d in e['inputs']}==expected_inputs for e in events):reasons.append('curation inputs')
        if not all(d['namespace']=='urn:bigdata:'+STORE and d['name'] for e in lineage for d in e['inputs']+e['outputs']):reasons.append('store identity')
        if not all(e['job']['namespace']=='vnu-uet.bigdata.lab2' and e['job']['name'] for e in lineage):reasons.append('job identity')
        if completed and (events[0]['eventTime']>completed['eventTime']):reasons.append('event chronology')
        replay=read_json(ROOT/'evidence/replay-check.json')
        if replay['status']!='PASS' or not all(c['original_logical_sha256']==c['reversed_and_shuffled_logical_sha256'] for c in replay['comparisons']):reasons.append('replay evidence')
        # Publisher independently executes the same replay, comparing bytes-derived relations.
        from checks import replay as execute_replay
        publisher_replay=execute_replay(engine(),manifest)
        if publisher_replay['status']!='PASS':reasons.append('publisher replay')
        approved_digests={c['relation']:c['original_logical_sha256'] for c in replay['comparisons']}
        if logical_digest(con,'submitted')!=approved_digests['curated']:reasons.append('submitted logical equivalence')
        if record['code_sha256']!=bundle['sha256'] or record['contract_sha256']!=sha(ROOT/'contract.json'):reasons.append('code or contract')
        tr=read_json(ROOT/'evidence/traces.json')
        for r in [tr['accepted']['output'],tr['quarantined']]:
            if con.execute('SELECT count(*) FROM raw_envelopes WHERE source_object=? AND cast(source_row AS BIGINT)=? AND source_sha256=?',[r['source_object'],r['source_row'],r['source_sha256']]).fetchone()[0]!=1:reasons.append('trace')
        return {'curation_events':len(events),'required_outputs':sorted(expected_outputs),'publisher_replay':publisher_replay['status'],'semantic_errors':reasons},['LINEAGE'] if reasons else []
    check('G09',lineage_replay)
    con.close()
    outcome='PASS' if len(observations)==9 and all(o['status']=='PASS' for o in observations) else 'BLOCKED' if any(o['status']=='BLOCKED' for o in observations) else 'REJECT'
    return {'result':outcome,'candidate_sha256':sha(candidate_path),'candidate_rows':n,
        'run_id':record['run_id'],'store_id':STORE,'checks':observations,
        'reasons':sorted({r for o in observations for r in o['reasons']}),
        'operator':OPERATOR,'workload_identity':'s3-owner','evaluated_at':now()}
