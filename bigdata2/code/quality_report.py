"""Role B: serialize SQL metric observations and render their labelled chart."""
from common import *
RULES={'Q_REQUIRED':'required_completeness','Q_UNIQUE':'business_key_uniqueness',
       'Q_VALUE':'value_validity','Q_SENSOR':'sensor_consistency','Q_TIME':'chronology',
       'Q_TIMELY':'timeliness','Q_QA_COVERAGE':'qa_coverage','Q_QA_AGREEMENT':'qa_agreement'}

def collect(con, run_id, contract, bundle):
    con.execute((ROOT/'code/quality.sql').read_text())
    metrics=[]
    for row in rows(con,'SELECT * FROM metric_counts ORDER BY phase,rule_id'):
        n,d=row['numerator'],row['denominator'];threshold=contract['release_thresholds'][RULES[row['rule_id']]]
        value=n/d if d else None
        metrics.append({**row,'dataset_id':'curated-candidate' if row['phase']=='after' else 'intake-winners',
            'dataset_version':run_id,'run_id':run_id,'contract_version':contract['schema_version'],
            'value':value,'threshold':threshold,'severity':'BLOCK',
            'status':'NOT_EVALUATED' if not d else 'PASS' if value>=threshold else 'FAIL'})
    intake=con.execute("SELECT count(*),count(*) FILTER (WHERE NOT parse_ok),count(*) FILTER (WHERE parse_ok AND (record_id IS NULL OR sensor_id IS NULL OR event_time IS NULL OR ingest_time IS NULL OR reading IS NULL OR unit IS NULL)) FROM typed").fetchone()
    counts={name:con.execute(f'SELECT count(*) FROM {name}').fetchone()[0]
        for name in ['raw_envelopes','key_eligible','winners','curated','quarantine','duplicates']}
    exclusion={'parse_failures':intake[1],'required_field_failures_in_parsed_intake':intake[2],
        'prekey_rejected':counts['raw_envelopes']-counts['key_eligible'],
        'duplicate_excess':counts['duplicates'],
        'timeliness_before_excluded':counts['winners']-con.execute('SELECT count(*) FROM evaluated WHERE chronology_ok').fetchone()[0],
        'retained_late_rows':con.execute('SELECT count(*) FROM curated WHERE is_late').fetchone()[0]}
    reasons=rows(con,'SELECT primary_reason,count(*) AS records FROM quarantine GROUP BY primary_reason ORDER BY records DESC,primary_reason')
    samples=rows(con,'SELECT primary_reason,source_object,source_row,record_id,all_reasons FROM quarantine QUALIFY row_number() OVER (PARTITION BY primary_reason ORDER BY source_object,source_row)<=3 ORDER BY primary_reason,source_row')
    envelope={'run_id':run_id,'store_id':STORE,'as_of':contract['as_of'],
        'input_manifest_sha256':sha(trusted_path()),'contract_sha256':sha(ROOT/'contract.json'),
        'code_sha256':bundle['sha256'],'counts':counts,'exclusions':exclusion,
        'primary_reason_counts':reasons,'defect_samples':samples,
        'interpretation':'Completeness increases because invalid winners are rejected, not imputed. Timeliness excludes invalid chronology. QA agreement concerns only the supplied 100-reference sample.',
        'retained_fraction':counts['curated']/counts['raw_envelopes'],
        'quarantined_fraction':counts['quarantine']/counts['raw_envelopes'],
        'superseded_fraction':counts['duplicates']/counts['raw_envelopes']}
    for phase in ['before','after']:
        write_json(ROOT/f'quality_{phase}.json',{**envelope,'phase':phase,'metrics':[x for x in metrics if x['phase']==phase]})
    return metrics,envelope

def chart(metrics):
    labels={'Q_REQUIRED':'Required completeness','Q_UNIQUE':'Business-key uniqueness','Q_VALUE':'Value validity',
        'Q_SENSOR':'Sensor consistency','Q_TIME':'Temporal consistency','Q_TIMELY':'Timeliness'}
    parts=['<svg xmlns="http://www.w3.org/2000/svg" width="1250" height="690" viewBox="0 0 1250 690">',
        '<rect width="1250" height="690" fill="#f8fafc"/>',
        '<g font-family="Arial, sans-serif" fill="#0f172a">',
        '<text x="40" y="48" font-size="25" font-weight="bold">Quality before and after curation</text>',
        '<text x="40" y="80" font-size="15">Each bar is its own numerator / denominator; populations differ as specified below.</text>']
    by={(m['phase'],m['rule_id']):m for m in metrics}
    for i,(rule,label) in enumerate(labels.items()):
        y=130+i*80
        parts.append(f'<text x="40" y="{y+18}" font-size="16">{label}</text>')
        for j,(phase,color) in enumerate([('before','#64748b'),('after','#0d9488')]):
            m=by[phase,rule];v=m['value'] or 0;yy=y+j*26
            parts.append(f'<rect x="275" y="{yy}" width="600" height="18" fill="#e2e8f0"/>')
            parts.append(f'<rect x="275" y="{yy}" width="{600*v:.2f}" height="18" fill="{color}"/>')
            parts.append(f'<text x="890" y="{yy+15}" font-size="14">{phase}: {m["numerator"]}/{m["denominator"]} ({v:.2%})</text>')
    parts.extend(['<text x="40" y="638" font-size="14">Before: W for required/value/sensor/time; key-eligible intake for uniqueness; chronological W for timeliness.</text>',
        '<text x="40" y="664" font-size="14">After: curated rows; chronological curated rows for timeliness. No combined quality score is computed.</text>','</g></svg>'])
    (ROOT/'evidence/quality-before-after.svg').write_text('\n'.join(parts),encoding='utf-8')
