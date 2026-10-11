"""Independent Python checks on delivered artifacts; no curation SQL is imported."""
import argparse
from collections import Counter
import csv
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import platform
import sys
import uuid
import duckdb

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def read_json(path):
    return json.loads(path.read_text(encoding='utf-8'))

def read_csv(path):
    with path.open(encoding='utf-8', newline='') as stream:
        return list(csv.DictReader(stream))

def physical(row):
    return row['source_object'], int(row['source_row'])

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--ledgers', type=Path)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    root = args.root.resolve()
    ledgers = args.ledgers or root / 'restricted'
    output = args.output or root / 'evidence/task2-check.json'
    run = read_json(root / 'run-record.json')
    contract = read_json(root / 'contract.json')
    candidate = root / 'candidate/curated.parquet'
    con = duckdb.connect()
    cursor = con.execute('SELECT * FROM read_parquet(?)', [str(candidate)])
    columns = [x[0] for x in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    registry = {r['sensor_id'].strip().upper(): r['site'] for r in read_csv(root / 'snapshot/sensors.csv')}
    reference = read_csv(root / 'snapshot/qa_reference.csv')
    by_id = {r['record_id']: r for r in rows}
    as_of = datetime.strptime(contract['as_of'], '%Y-%m-%dT%H:%M:%SZ')
    lo = Decimal(str(contract['temperature_range_c']['min']))
    hi = Decimal(str(contract['temperature_range_c']['max']))
    chronology = [r for r in rows if r['event_time_utc'] is not None and r['ingest_time_utc'] is not None and r['event_time_utc'] <= r['ingest_time_utc'] <= as_of]
    timely = [r for r in chronology if (r['ingest_time_utc'] - r['event_time_utc']).total_seconds() <= contract['late_threshold_seconds']]
    matched = [(q, by_id[q['record_id']]) for q in reference if q['record_id'] in by_id]
    pairs = {
        'Q_REQUIRED': (sum(all(r[c] is not None for c in columns) for r in rows), len(rows)),
        'Q_UNIQUE': (len(by_id), len(rows)),
        'Q_VALUE': (sum(r['temperature_c'] is not None and r['temperature_c'].is_finite() and lo <= r['temperature_c'] <= hi for r in rows), len(rows)),
        'Q_SENSOR': (sum(r['sensor_id'] in registry for r in rows), len(rows)),
        'Q_TIME': (len(chronology), len(rows)),
        'Q_TIMELY': (len(timely), len(chronology)),
        'Q_QA_COVERAGE': (len({r['record_id'] for _, r in matched}), len(reference)),
        'Q_QA_AGREEMENT': (sum(abs(r['temperature_c'] - Decimal(q['reference_c'])) <= Decimal('0.05') for q, r in matched), len(matched)),
    }
    saved = {m['rule_id']: m for m in read_json(root / 'quality_after.json')['metrics']}
    metrics = []
    for rule, (n, d) in pairs.items():
        old = saved[rule]
        value = n / d if d else None
        status = 'NOT_EVALUATED' if not d else 'PASS' if value >= old['threshold'] else 'FAIL'
        metrics.append({'rule_id': rule, 'numerator': n, 'denominator': d, 'value': value,
                        'threshold': old['threshold'], 'status': status,
                        'matches_saved_counts': (n, d) == (old['numerator'], old['denominator'])})
    expected = next(x for x in run['outputs'] if x['path'] == 'candidate/curated.parquet')
    checks = {
        'candidate_historical_hash': sha(candidate) == expected['sha256'],
        'business_key_uniqueness': len(by_id) == len(rows),
        'all_after_counts_match_saved_report': all(m['matches_saved_counts'] for m in metrics),
        'all_after_thresholds_pass': all(m['status'] == 'PASS' for m in metrics),
        'registry_derived_site': all(registry.get(r['sensor_id']) == r['site'] for r in rows),
        'late_flag_consistency': all(r['is_late'] == ((r['ingest_time_utc'] - r['event_time_utc']).total_seconds() > contract['late_threshold_seconds']) for r in chronology),
    }
    raw = read_csv(root / 'envelopes.csv')
    partitions = {'raw': raw, 'curated': rows}
    for name in ['quarantine', 'duplicates']:
        partitions[name] = read_csv(ledgers / (name + '.csv'))
    counters = {name: Counter(map(physical, data)) for name, data in partitions.items()}
    sets = {name: set(c) for name, c in counters.items()}
    union = sets['curated'] | sets['quarantine'] | sets['duplicates']
    conservation = {
        'counts': {name: len(data) for name, data in partitions.items()},
        'missing_physical_ids': len(sets['raw'] - union),
        'invented_physical_ids': len(union - sets['raw']),
        'overlaps': {a + '/' + b: len(sets[a] & sets[b]) for a, b in [('curated', 'quarantine'), ('curated', 'duplicates'), ('quarantine', 'duplicates')]},
        'repeated_physical_ids': {name: sum(n - 1 for n in c.values()) for name, c in counters.items()},
    }
    checks['count_reconciliation'] = len(raw) == sum(len(partitions[k]) for k in ['curated', 'quarantine', 'duplicates'])
    checks['physical_set_conservation'] = not conservation['missing_physical_ids'] and not conservation['invented_physical_ids'] and not any(conservation['overlaps'].values()) and not any(conservation['repeated_physical_ids'].values())
    artifacts = []
    paths = [root / 'run-record.json', root / 'quality_before.json', root / 'quality_after.json', candidate,
             root / 'contract.json', root / 'trusted_manifest.json', root / 'envelopes.csv',
             root / 'snapshot/sensors.csv', root / 'snapshot/qa_reference.csv',
             ledgers / 'quarantine.csv', ledgers / 'duplicates.csv']
    for p in paths:
        artifacts.append({'path': p.relative_to(root).as_posix() if p.is_relative_to(root) else 'supplemental-ledgers/' + p.name,
                          'sha256': sha(p), 'bytes': p.stat().st_size})
    source_integrity = []
    for entry in read_json(root / 'trusted_manifest.json')['objects']:
        p = root / 'snapshot' / entry['key']
        source_integrity.append({'key': entry['key'], 'bytes': p.stat().st_size, 'expected_bytes': entry['bytes'],
                                'matches_trusted_bytes': sha(p) == entry['sha256'] and p.stat().st_size == entry['bytes']})
    checks['source_integrity'] = all(x['matches_trusted_bytes'] for x in source_integrity)
    technical_checks = {k: v for k, v in checks.items() if k != 'source_integrity'}
    result = {
        'check_id': 'ROLE_B_TASK2_CROSS_CHECK', 'check_run_id': str(uuid.uuid4()),
        'executed_at': datetime.now(timezone.utc).isoformat(), 'source_run_id': run['run_id'],
        'operator': 'Automated local verification', 'assigned_student': '24022466 - Lê Toàn',
        'reviewer': None, 'human_demo_attested': False, 'exit_response_attested': False,
        'runtime': {'python': platform.python_version(), 'duckdb': duckdb.__version__, 'platform': sys.platform},
        'script_sha256': sha(Path(__file__)), 'command': 'python -X utf8 code/check_task2.py',
        'method': 'Read actual Parquet with DuckDB; recompute after metrics with Python/Decimal and physical conservation with Counter/set; no production SQL imported',
        'task2_artifact_check_status': 'PASS' if all(technical_checks.values()) else 'FAIL',
        'overall_status': 'PASS_TASK2_ARTIFACT_CHECKS_ONLY' if all(checks.values()) else 'BLOCKED_SOURCE_INTEGRITY',
        'checks': checks, 'after_metrics': metrics, 'conservation': conservation,
        'source_integrity': source_integrity, 'artifacts': artifacts,
        'ledger_provenance': read_json(ledgers / 'reconstruction-record.json') if (ledgers / 'reconstruction-record.json').exists() else None,
        'limitations': ['Reconstructed ledgers are not asserted to be original historical files.',
                        'After-value checks concern exported DECIMAL values; original pre-rounding validity is not independently proved here.',
                        'No human demonstration, contract review, owner approval or publication is attested by this check.'],
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print(json.dumps({'task2_artifact_check_status': result['task2_artifact_check_status'],
                      'overall_status': result['overall_status'], 'checks': checks, 'conservation': conservation}, indent=2))
    return 0 if all(checks.values()) else 1

if __name__ == '__main__':
    raise SystemExit(main())
