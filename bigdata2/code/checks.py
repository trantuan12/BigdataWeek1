"""Role C verification: set partition, replay, schema and executable counterexamples."""
import random
from common import *
from stage_inputs import envelopes, FIELDS, EXTRA

def partition(con, candidate='curated'):
    counts={r:con.execute(f'SELECT count(*) FROM {r}').fetchone()[0]
        for r in ['raw_envelopes',candidate,'quarantine','duplicates']}
    outputs=f'(SELECT source_object,source_row FROM {candidate} UNION ALL SELECT source_object,source_row FROM quarantine UNION ALL SELECT source_object,source_row FROM duplicates)'
    def count(sql): return con.execute(sql).fetchone()[0]
    missing=count(f'SELECT count(*) FROM (SELECT source_object,source_row FROM raw_envelopes EXCEPT SELECT * FROM {outputs})')
    invented=count(f'SELECT count(*) FROM (SELECT * FROM {outputs} EXCEPT SELECT source_object,source_row FROM raw_envelopes)')
    overlaps=count(f'SELECT count(*) FROM (SELECT source_object,source_row,count(*) AS n FROM {outputs} GROUP BY ALL HAVING n<>1)')
    repeated={r:count(f'SELECT count(*) FROM (SELECT source_object,source_row,count(*) n FROM {r} GROUP BY ALL HAVING n>1)')
        for r in [candidate,'quarantine','duplicates']}
    business=count(f'SELECT count(*)-count(DISTINCT record_id) FROM {candidate}')
    ok=counts['raw_envelopes']==sum(counts[x] for x in [candidate,'quarantine','duplicates']) and not any([missing,invented,overlaps,business,*repeated.values()])
    return {'status':'PASS' if ok else 'FAIL','counts':counts,'missing_physical_ids':missing,
        'invented_physical_ids':invented,'overlapping_or_repeated_physical_ids':overlaps,
        'duplicate_physical_ids_per_output':repeated,'duplicate_business_keys':business}

def replay(con, manifest):
    dest=ROOT/'restricted/replay-envelopes.csv'
    envelopes(ROOT/'restricted/snapshot',dest,manifest,list(reversed(sorted(o['key'] for o in manifest['objects']))))
    with dest.open(encoding='utf-8',newline='') as f: source=list(csv.DictReader(f))
    random.Random(24022463).shuffle(source)
    with dest.open('w',encoding='utf-8',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=FIELDS+EXTRA);writer.writeheader();writer.writerows(source)
    other=engine(dest)
    comparisons=[]
    for relation in ['curated','quarantine','duplicates']:
        a=logical_digest(con,relation); b=logical_digest(other,relation)
        comparisons.append({'relation':relation,'original_logical_sha256':a,
            'reversed_and_shuffled_logical_sha256':b,'equal':a==b})
    other.close()
    return {'check_id':'C02','status':'PASS' if all(x['equal'] for x in comparisons) else 'FAIL',
        'seed':24022463,'reversed_source_enumeration':True,'shuffled_envelopes':True,
        'physical_source_ordinals_preserved':True,'comparisons':comparisons,
        'parquet_byte_equality_claimed':False,'operator':OPERATOR,'executed_at':now()}

def edge_cases():
    dest=ROOT/'restricted/edge-envelopes.csv'
    specs=[
        ('fahrenheit','R990001','32','F','2026-02-01T00:01:00Z',''),
        ('unsupported','R990002','273','K','2026-02-01T00:01:00Z',''),
        ('nonfinite','R990003','NaN','C','2026-02-01T00:01:00Z',''),
        ('optional_blank','R990004','20','C','2026-02-01T00:01:00Z',''),
        ('older_valid','R990005','20','C','2026-02-01T00:01:00Z',''),
        ('newer_invalid','R990005','100','C','2026-02-01T00:02:00Z',''),
        ('late_valid','R990006','20','C','2026-02-01T00:16:00Z',''),
        ('strict_time','R990007','20','C','2026-02-01T00:01:00Z','bad-time'),
        ('prekey_both','invalid-id','20','C','invalid-arrival','')]
    with dest.open('w',encoding='utf-8',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=FIELDS+EXTRA);writer.writeheader()
        for i,(label,rid,value,unit,arrival,event_override) in enumerate(specs,1):
            writer.writerow(dict(record_id=rid,sensor_id='S01',event_time=event_override or '2026-02-01T00:00:00Z',
                ingest_time=arrival,reading=value,unit=unit,operator_email='',source_object='fixture://edge-cases',
                source_row=i,source_sha256='0'*64,parse_ok='true',raw_payload='{}'))
    con=engine(dest)
    dispositions=rows(con,"SELECT source_row,'CURATED' AS disposition,cast(temperature_c AS VARCHAR) AS detail,is_late FROM curated UNION ALL SELECT source_row,'QUARANTINE',all_reasons,NULL FROM quarantine UNION ALL SELECT source_row,'DUPLICATE',reason,NULL FROM duplicates ORDER BY source_row")
    by={r['source_row']:r for r in dispositions}
    checks=[
        {'case':'32 F -> 0.00 C','pass':by[1]['disposition']=='CURATED' and by[1]['detail']=='0.00'},
        {'case':'unsupported K and nonfinite numeric quarantined','pass':by[2]['disposition']=='QUARANTINE' and 'UNIT' in by[2]['detail'] and by[3]['disposition']=='QUARANTINE' and 'NUMERIC' in by[3]['detail']},
        {'case':'blank optional email retained and column omitted','pass':by[4]['disposition']=='CURATED' and 'operator_email' not in [x[0] for x in schema_of(con,'curated')]},
        {'case':'invalid newest quarantined; older valid superseded','pass':by[5]['disposition']=='DUPLICATE' and by[6]['disposition']=='QUARANTINE'},
        {'case':'valid late retained','pass':by[7]['disposition']=='CURATED' and by[7]['is_late'] is True},
        {'case':'strict UTC timestamp format','pass':by[8]['disposition']=='QUARANTINE' and 'TIME_PARSE' in by[8]['detail']},
        {'case':'two pre-key reasons retained','pass':by[9]['detail']=='KEY|INGEST_PARSE'}]
    con.close()
    return {'check_id':'C04','status':'PASS' if all(x['pass'] for x in checks) else 'FAIL',
        'core_counterexamples':4,'additional_checks':3,'cases':checks,'observed_dispositions':dispositions,
        'operator':OPERATOR,'executed_at':now()}
