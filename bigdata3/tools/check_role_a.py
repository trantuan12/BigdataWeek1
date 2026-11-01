"""Verify role A's handoff and E's actual pinned source-to-snapshot trace."""
import argparse
import csv
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from itertools import islice
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'src'), str(ROOT / 'templates')]
from common import (as_time, load_csv, load_registry, logical_sha256, read_json,
                    sha256_file, validate_rows, write_json)
from run_lab import (rows_from_silver, runtime_info, serial, small_records,
                     snapshot, version, worker_probe)
from session import make_spark

SOURCE_PATHS = {
    'observations_a.csv': 'data/lab2_sources/observations_a.csv',
    'observations_b.jsonl': 'data/lab2_sources/observations_b.jsonl',
    'lab3/source_batch02.csv': 'data/changes/source_batch02.csv',
}


def source_projection(source_object, source_row, root=ROOT):
    """Read a physical ordinal; return only released measurement fields."""
    if source_object not in SOURCE_PATHS or type(source_row) is not int or source_row < 1:
        raise ValueError('INVALID_SOURCE_REFERENCE')
    path = root / SOURCE_PATHS[source_object]
    with path.open(encoding='utf-8', newline='') as handle:
        if path.suffix == '.jsonl':
            line = next(islice(handle, source_row - 1, source_row), None)
            raw = json.loads(line) if line is not None else None
            if raw is not None:
                values = [raw['id'], raw['sensor'], raw['timestamp'], raw['arrived_at'],
                          raw['temperature'], raw['temperature_unit']]
        else:
            raw = next(islice(csv.DictReader(handle), source_row - 1, source_row), None)
            if raw is not None:
                values = [raw['record_id'], raw['sensor_id'], raw['event_time'], raw['ingest_time'],
                          raw['reading'], raw['unit']]
    if raw is None:
        raise ValueError('SOURCE_ORDINAL_OUT_OF_RANGE')
    key, sensor, event, ingest, temperature, unit = values
    unit = unit.strip().upper()
    value = Decimal(str(temperature).strip())
    if unit == 'F':
        value = (value - Decimal(32)) * Decimal(5) / Decimal(9)
    elif unit != 'C':
        raise ValueError('UNKNOWN_SOURCE_UNIT')

    def utc(value):
        return datetime.fromisoformat(value.strip().replace('Z', '+00:00')).astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

    return {'record_id': key.strip().upper(), 'sensor_id': sensor.strip(),
            'event_time_utc': utc(event), 'ingest_time_utc': utc(ingest),
            'temperature_c': format(value.quantize(Decimal('.01'), rounding=ROUND_HALF_UP), '.2f')}


def revision_order(row):
    return -as_time(row['ingest_time_utc']).timestamp(), row['source_object'], int(row['source_row'])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', default='config/team.json')
    parser.add_argument('--trusted-manifest', required=True)
    args = parser.parse_args()
    config = read_json(args.config)
    ev = ROOT / config['evidence_dir']
    state = read_json(ev / 'state.json')
    binding = read_json(ev / 'release_candidate/snapshot_binding.json')
    checks = []
    trace = []
    prediction = {
        'member_id': '24022484', 'member_name': 'Trần Anh Tuấn', 'role': 'A',
        'recorded_at_utc': datetime.now(timezone.utc).isoformat(),
        'execution_mode': 'AUTOMATED_LIVE_DELTA_READ',
        'predictions': {
            'inputs': 'All 16 byte/hash checks pass; baseline is the declared 9610-row course fallback.',
            'snapshot': 'The candidate binds Silver v1 and Gold v1; live reads match the declared versions.',
            'R000381': {'source_object': 'observations_a.csv', 'source_row': 381, 'temperature_c': '26.10'},
            'R005001': {'source_object': 'observations_b.jsonl', 'source_row': 1, 'temperature_c': '18.10'},
            'R010001': {'source_object': 'lab3/source_batch02.csv', 'source_row': 1, 'temperature_c': '19.10'},
            'tie': 'Same ingest/object for rows 1 and 131; row 1 wins regardless of file enumeration order.',
        },
    }
    write_json(ev / 'role_A_prediction.json', prediction)

    def check(key, condition, observed):
        checks.append({'check_id': key, 'passed': bool(condition), 'observed': observed})
        if not condition:
            write_json(ev / 'role_A_check.json', {'passed': False, 'checks': checks})
            raise ValueError('ROLE_A_CHECK_FAILED:' + key)

    from common import verify_inputs
    integrity = verify_inputs(ROOT, args.trusted_manifest)
    check('H01_trusted_input_bytes', len(integrity['checks']) == 16
          and integrity['trusted_manifest_sha256'] == state['trusted_manifest_sha256'],
          {'artifacts': len(integrity['checks']), 'trusted_manifest_sha256': integrity['trusted_manifest_sha256']})
    registry = load_registry(ROOT / 'data/handoff/sensors.csv')
    handoff = validate_rows(load_csv(ROOT / 'data/handoff/curated.csv'), registry, True)
    check('H02_fallback_identity', len(handoff) == 9610
          and logical_sha256(handoff) == state['handoff_logical_sha256']
          and state['handoff_kind'] == 'course_fallback_not_student_approval',
          {'rows': len(handoff), 'kind': state['handoff_kind'], 'logical_sha256': logical_sha256(handoff)})
    expected_binding = {
        'silver_path': state['silver_path'], 'silver_version': state['silver_final_version'],
        'gold_path': state['gold_path'], 'gold_version': state['gold_final_version'],
        'gold_source_silver_version': state['gold_source_silver_version'],
        'handoff_logical_sha256': state['handoff_logical_sha256'],
        'code_sha256': state['final_implementation_sha256'],
    }
    check('A_cross_E_snapshot_binding', all(binding.get(k) == v for k, v in expected_binding.items())
          and binding['gold_source_silver_version'] == binding['silver_version'], expected_binding)
    check('A_cross_E_generating_code', sha256_file(ROOT / 'templates/student_impl.py') == binding['code_sha256']
          == sha256_file(ev / 'code_versions' / (binding['code_sha256'] + '.py')),
          {'generating_code_sha256': binding['code_sha256']})
    manifest = read_json(ev / 'release_candidate/release_manifest.json')
    check('A_cross_E_candidate_objects', manifest['binding'] == binding
          and manifest['status'] == 'CANDIDATE_NOT_OWNER_PUBLISHED'
          and all((ev / 'release_candidate' / obj['key']).stat().st_size == obj['bytes']
                  and sha256_file(ev / 'release_candidate' / obj['key']) == obj['sha256']
                  for obj in manifest['objects']),
          {'objects': len(manifest['objects']), 'status': manifest['status']})

    spark = make_spark(config)
    try:
        runtime = runtime_info(spark, config)
        write_json(ev / 'runtime_role_A_check.json', runtime)
        check('H04_runtime_baseline', runtime['spark'] == runtime['delta_spark'] == '4.0.1'
              and runtime['python'].startswith('3.11.') and runtime['java'].split('.')[0] in ('17', '21')
              and runtime['timezone'] == 'UTC', runtime)
        workers = worker_probe(spark)
        write_json(ev / 'role_A_worker_probe.json', workers)
        check('H04_worker_placement', workers['distinct_hosts'] == 2 and len(workers['partition_tasks']) == 24,
              {'worker_containers': workers['distinct_hosts'], 'partition_tasks': len(workers['partition_tasks']),
               'limit': 'Container placements; separate physical servers are not established.'})
        before = {layer: version(spark, state[layer + '_path']) for layer in ('silver', 'gold')}
        smoke = read_json(ev / 'preflight.json')['delta_smoke_path']
        smoke_rows = spark.read.format('delta').load(smoke).count()
        check('H03_schema_probe_preserved', read_json(ev / 'delta_schema_probe.json')['passed'] and smoke_rows == 8,
              {'native_rejection_evidence': 'delta_schema_probe.json', 'live_probe_rows': smoke_rows})
        baseline = rows_from_silver(spark, binding['silver_path'], state['silver_initial_version'], registry)
        current = rows_from_silver(spark, binding['silver_path'], binding['silver_version'], registry)
        check('A_live_baseline_snapshot', logical_sha256(baseline) == state['handoff_logical_sha256'],
              {'silver_version': state['silver_initial_version'], 'rows': len(baseline),
               'logical_sha256': logical_sha256(baseline)})
        check('A_live_candidate_Silver', logical_sha256(current) == read_json(ev / 'incremental.json')['logical_sha256_after'],
              {'silver_path': binding['silver_path'], 'silver_version': binding['silver_version'],
               'rows': len(current), 'logical_sha256': logical_sha256(current)})
        indexed = {row['record_id']: row for row in current}
        for key in ('R000381', 'R005001', 'R010001'):
            selected = indexed[key]
            wanted = prediction['predictions'][key]
            source = source_projection(selected['source_object'], selected['source_row'])
            digest = sha256_file(ROOT / SOURCE_PATHS[selected['source_object']])
            lag = int((as_time(selected['ingest_time_utc']) - as_time(selected['event_time_utc'])).total_seconds())
            matched = (all(selected[k] == v for k, v in source.items())
                       and all(selected[k] == v for k, v in wanted.items())
                       and digest == selected['source_sha256'] and selected['is_late'] == (lag > 900))
            entry = {'record_id': key, 'silver_path': binding['silver_path'],
                     'silver_version': binding['silver_version'], 'selected_release_row': selected,
                     'source_projection': source, 'source_file_sha256': digest,
                     'arrival_lag_seconds': lag, 'passed': bool(matched)}
            trace.append(entry)
            check('A_source_to_snapshot_' + key, matched, entry)
        incoming = validate_rows(load_csv(ROOT / 'data/changes/changes_batch02.csv'), registry, False)
        revisions = [row for row in incoming if row['record_id'] == 'R010001']
        winner = min(revisions, key=revision_order)
        reverse_winner = min(reversed(revisions), key=revision_order)
        last_by_id = {row['record_id']: row for row in incoming}['R010001']
        check('A_deterministic_tie_and_old_example', winner == reverse_winner == indexed['R010001']
              and winner['source_row'] == 1 and last_by_id['source_row'] == 131
              and last_by_id['temperature_c'] == '20.10',
              {'incoming_revisions': [{k: row[k] for k in ('source_row', 'ingest_time_utc', 'temperature_c')}
                                      for row in revisions],
               'winner_ordinal': winner['source_row'], 'winner_temperature_c': winner['temperature_c'],
               'last_dictionary_ordinal': last_by_id['source_row'],
               'last_dictionary_temperature_c': last_by_id['temperature_c']})
        gold = sorted([serial(row) for row in small_records(snapshot(spark, binding['gold_path'], binding['gold_version']))],
                      key=lambda row: (row['site'], row['event_date']))
        check('A_cross_E_live_Gold_export', gold == read_json(ev / 'release_candidate/gold.json'),
              {'gold_path': binding['gold_path'], 'gold_version': binding['gold_version'], 'site_day_groups': len(gold)})
        after = {layer: version(spark, state[layer + '_path']) for layer in ('silver', 'gold')}
        check('A_read_only_tables', before == after, {'latest_versions_before': before, 'latest_versions_after': after})
        write_json(ev / 'role_A_source_trace.json', {
            'silver_path': binding['silver_path'], 'silver_version': binding['silver_version'],
            'gold_version': binding['gold_version'], 'rows': trace,
            'projection_policy': 'Only released measurement fields and source provenance; no raw payloads.'})
        result = {'passed': True, 'member_id': '24022484', 'member_name': 'Trần Anh Tuấn', 'role': 'A',
                  'execution_mode': 'AUTOMATED_LIVE_DELTA_READ',
                  'completed_at_utc': datetime.now(timezone.utc).isoformat(),
                  'code_sha256': sha256_file(__file__), 'checks': checks,
                  'silver_version': binding['silver_version'], 'gold_version': binding['gold_version'],
                  'writes_to_Delta_tables': 0, 'student_demonstration_attested': False}
        write_json(ev / 'role_A_check.json', result)
        (ev / 'code_versions' / (result['code_sha256'] + '.py')).write_bytes(Path(__file__).read_bytes())
        print(f'PASS: {len(checks)} role A checks; three actual source-to-snapshot traces; zero Delta writes.')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
