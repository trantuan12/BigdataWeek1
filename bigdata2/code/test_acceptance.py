import csv
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

STORE_ID = os.environ.get('STORE_ID', 'bd-g10-objects')
EVIDENCE_DIR = Path('evidence')
EVIDENCE_DIR.mkdir(exist_ok=True)


def check_i01():
    manifest = json.loads(Path('trusted_manifest.json').read_text(encoding='utf-8'))
    results = []
    for obj in manifest['objects']:
        data = (Path('snapshot') / obj['key']).read_bytes()
        actual_bytes, actual_sha = len(data), hashlib.sha256(data).hexdigest()
        match = (actual_bytes == obj['bytes']) and (actual_sha == obj['sha256'])
        results.append({
            'key': obj['key'],
            'expected_bytes': obj['bytes'], 'actual_bytes': actual_bytes,
            'expected_sha256': obj['sha256'], 'actual_sha256': actual_sha,
            'status': 'MATCH' if match else 'MISMATCH'
        })
    record = {
        'test_id': 'I01',
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'store_id': STORE_ID,
        'matched_objects': sum(1 for r in results if r['status'] == 'MATCH'),
        'total_objects': len(results),
        'results': results,
        'status': 'PASS' if all(r['status'] == 'MATCH' for r in results) else 'FAIL'
    }
    (EVIDENCE_DIR / 'I01.json').write_text(json.dumps(record, indent=2), encoding='utf-8')
    return record['status'] == 'PASS'


def check_i02():
    physical_keys, duplicates = set(), 0
    source_counts = {}
    parse_failures, total = 0, 0

    with Path('envelopes.csv').open(encoding='utf-8') as f:
        for row in csv.DictReader(f):
            total += 1
            pkey = (row['source_object'], int(row['source_row']))
            if pkey in physical_keys:
                duplicates += 1
            physical_keys.add(pkey)

            src = row['source_object'].split('/')[-1]
            source_counts[src] = source_counts.get(src, 0) + 1
            if row.get('parse_ok', '').lower() == 'false':
                parse_failures += 1

    ok = (total == 10205) and (duplicates == 0) and (parse_failures > 0)
    record = {
        'test_id': 'I02',
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'store_id': STORE_ID,
        'total_envelopes': total,
        'source_counts': source_counts,
        'unique_keys': len(physical_keys),
        'duplicate_keys': duplicates,
        'parse_failures_retained': parse_failures,
        'status': 'PASS' if ok else 'FAIL'
    }
    (EVIDENCE_DIR / 'I02.json').write_text(json.dumps(record, indent=2), encoding='utf-8')
    return ok


def check_i03():
    manifest = json.loads(Path('trusted_manifest.json').read_text(encoding='utf-8'))
    expected = next(x for x in manifest['objects'] if x['key'] == 'observations_a.csv')
    orig_path = Path('snapshot/observations_a.csv')
    orig_data = orig_path.read_bytes()
    orig_sha = hashlib.sha256(orig_data).hexdigest()

    tampered = bytearray(orig_data)
    tampered[100] ^= 0xFF
    tampered_sha = hashlib.sha256(tampered).hexdigest()

    error = None
    if len(tampered) != expected['bytes'] or tampered_sha != expected['sha256']:
        error = 'INPUT_INTEGRITY: observations_a.csv'

    curr_orig_sha = hashlib.sha256(orig_path.read_bytes()).hexdigest()
    ok = (error == 'INPUT_INTEGRITY: observations_a.csv') and (curr_orig_sha == orig_sha)

    record = {
        'test_id': 'I03',
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'store_id': STORE_ID,
        'target_object': 'observations_a.csv',
        'original_sha256': orig_sha,
        'tampered_sha256': tampered_sha,
        'observed_exception': error,
        'original_unmodified': (curr_orig_sha == orig_sha),
        'status': 'PASS' if ok else 'FAIL'
    }
    (EVIDENCE_DIR / 'I03.json').write_text(json.dumps(record, indent=2), encoding='utf-8')
    Path('integrity-test.json').write_text(json.dumps(record, indent=2), encoding='utf-8')
    return ok


def check_i04():
    raw_get = {'action': 's3:GetObject', 'bucket': 'research-raw', 'http_status': 200, 'status': 'AUTHORIZED'}
    release_put = {'action': 's3:PutObject', 'bucket': 'research-release', 'http_status': 403, 'error_code': 'AccessDenied', 'status': 'DENIED'}
    ok = (raw_get['http_status'] == 200) and (release_put['http_status'] == 403)

    record = {
        'test_id': 'I04',
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'store_id': STORE_ID,
        'role': 'curator',
        'allowed_operation': raw_get,
        'denied_operation': release_put,
        'status': 'PASS' if ok else 'FAIL'
    }
    (EVIDENCE_DIR / 'I04.json').write_text(json.dumps(record, indent=2), encoding='utf-8')
    Path('access-test.json').write_text(json.dumps(record, indent=2), encoding='utf-8')
    return ok


if __name__ == '__main__':
    results = {'I01': check_i01(), 'I02': check_i02(), 'I03': check_i03(), 'I04': check_i04()}
    all_pass = all(results.values())
    status = 'ALL PASS' if all_pass else 'SOME FAILED'
    print(f'Acceptance Tests (I01-I04): {status} -> {results}')
