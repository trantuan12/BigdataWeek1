"""Review an actual release candidate and independently cross-check role A."""
import argparse
from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'src'), str(ROOT / 'templates')]
from common import (AS_OF, emit_lineage, load_csv, load_registry, logical_sha256,
                    read_json, sha256_file, validate_rows, write_json)
from run_lab import rows_from_silver, runtime_info, serial, small_records, snapshot, version
from session import make_spark
from student_impl import validate_release_binding
from verify_results import aggregate

BINDING_KEYS = ('handoff_logical_sha256', 'code_sha256', 'silver_path', 'silver_version',
                'gold_path', 'gold_version', 'gold_source_silver_version')


def expected_binding(state):
    """Expected values come from recorded pipeline state, not a candidate copy."""
    return {'handoff_logical_sha256': state['handoff_logical_sha256'],
            'code_sha256': state['final_implementation_sha256'],
            'silver_path': state['silver_path'], 'silver_version': state['silver_final_version'],
            'gold_path': state['gold_path'], 'gold_version': state['gold_final_version'],
            'gold_source_silver_version': state['gold_source_silver_version']}


def catalog_issues(catalog, binding, state, silver_count, gold_count, silver_columns, gold_columns):
    """Check the catalog against measured data and trusted schema metadata."""
    if not isinstance(catalog, dict):
        return ['CATALOG_TYPE']
    issues = []
    expected = {'schema_version': 'lab3-catalog-v1', 'owner': binding['reviewer'],
                'steward': binding['steward'], 'allowed_use': 'Synthetic teaching and assessment only',
                'as_of': AS_OF, 'trusted_manifest_sha256': state['trusted_manifest_sha256'],
                'upstream_handoff_kind': state['handoff_kind'],
                'upstream_logical_sha256': state['handoff_logical_sha256']}
    issues.extend(key for key, value in expected.items() if catalog.get(key) != value)
    datasets = catalog.get('datasets')
    if not isinstance(datasets, list) or len(datasets) != 2 or not all(isinstance(d, dict) for d in datasets):
        return issues + ['DATASETS']
    by_id = {d.get('dataset_id'): d for d in datasets}
    if set(by_id) != {'silver_observations', 'gold_site_day'}:
        return issues + ['DATASET_IDENTITIES']
    for dataset_id, layer, count, columns, classification in (
        ('silver_observations', 'silver', silver_count, silver_columns, 'RESTRICTED_WORKING_TABLE'),
        ('gold_site_day', 'gold', gold_count, gold_columns, 'APPROVED_ONLY_AFTER_OWNER_REVIEW')):
        data = by_id[dataset_id]
        wanted = {'location': state[layer + '_path'], 'version': state[layer + '_final_version'],
                  'row_count': count, 'columns': columns, 'classification': classification}
        if layer == 'gold':
            wanted['source_silver_version'] = state['silver_final_version']
        for key, value in wanted.items():
            if data.get(key) != value or (type(value) is int and type(data.get(key)) is not int):
                issues.append(dataset_id + ':' + key)
    retention = catalog.get('retention', {})
    if not isinstance(retention, dict):
        return issues + ['RETENTION']
    if type(retention.get('published_release_days')) is not int or retention.get('published_release_days') != 90:
        issues.append('RETENTION_DAYS')
    if retention.get('time_basis') != 'actual publication time, not simulated AS_OF':
        issues.append('RETENTION_TIME_BASIS')
    rule = retention.get('dependency_rule', '')
    if not isinstance(rule, str) or 'referenced table versions/data files' not in rule or 'No VACUUM' not in rule:
        issues.append('RETENTION_DEPENDENCIES')
    return issues


def lineage_issues(events, state):
    """Validate event pairing and the concrete release snapshot dependencies."""
    issues = []
    pairs = defaultdict(list)
    for event in events:
        pairs[event['run']['runId']].append(event)
    completed = set()
    releases = []
    for run_id, pair in pairs.items():
        if len(pair) != 2 or pair[0]['eventType'] != 'START' or pair[-1]['eventType'] not in ('COMPLETE', 'FAIL', 'ABORT'):
            issues.append('RUN_PAIR:' + run_id)
            continue
        start, end = pair
        if start['run'] != end['run'] or start['job'] != end['job']:
            issues.append('RUN_IDENTITY:' + run_id)
        if datetime.fromisoformat(start['eventTime'].replace('Z', '+00:00')) > datetime.fromisoformat(end['eventTime'].replace('Z', '+00:00')):
            issues.append('RUN_TIME:' + run_id)
        if end['eventType'] == 'COMPLETE':
            completed.add(end['job']['name'])
            if end['job']['name'] == 'release':
                releases.append(end)
    required = {'preflight', 'bootstrap', 'incremental', 'analytics', 'benchmark', 'release'}
    if not required <= completed:
        issues.append('REQUIRED_TASKS')
    wanted = {state['silver_path'] + '@' + str(state['silver_final_version']),
              state['gold_path'] + '@' + str(state['gold_final_version'])}
    if not releases or not any(wanted <= {d['name'] for d in event['inputs']} and event['outputs'] for event in releases):
        issues.append('RELEASE_SNAPSHOT_INPUTS')
    return issues, {'events': len(events), 'runs': len(pairs), 'completed_jobs': sorted(completed)}


def gate_cases(binding, expected):
    """Run the five mandatory cases plus refusal/immutability edge cases."""
    required = []
    edge = []

    def run(target, name, draft, wanted, reason=None, values=None):
        source = expected if values is None else values
        before = deepcopy((draft, source))
        passed, reasons = validate_release_binding(draft, source)
        correct = (passed is wanted and (reason is None or reason in reasons)
                   and before == (draft, source) and len(reasons) == len(set(reasons)))
        target.append({'case': name, 'expected_pass': wanted, 'observed_pass': passed,
                       'reasons': reasons, 'test_passed': correct})
        if not correct:
            raise ValueError('RELEASE_REVIEW_GATE:' + name)

    run(required, 'R01_valid', deepcopy(binding), True)
    run(required, 'R02_mixed_version', dict(binding, gold_source_silver_version=0), False, 'VERSION_BINDING')
    run(required, 'R03_self_review', dict(binding, reviewer=binding['author']), False, 'REVIEW')
    missing = deepcopy(binding)
    missing.pop('steward')
    run(required, 'R04_missing_steward', missing, False, 'METADATA')
    run(required, 'R05_wrong_code', dict(binding, code_sha256='0' * 64), False, 'CODE_BINDING')
    for key in (*BINDING_KEYS, 'author', 'reviewer', 'steward'):
        draft = deepcopy(binding)
        draft.pop(key)
        reason = ('METADATA' if key in ('author', 'reviewer', 'steward') else 'CODE_BINDING' if key == 'code_sha256'
                  else 'HANDOFF_BINDING' if key == 'handoff_logical_sha256' else 'VERSION_BINDING')
        run(edge, 'missing_draft_' + key, draft, False, reason)
    for key in BINDING_KEYS:
        values = deepcopy(expected)
        values.pop(key)
        run(edge, 'missing_expected_' + key, deepcopy(binding), False, values=values)
    for key in ('silver_version', 'gold_version', 'gold_source_silver_version'):
        for value in (True, -1, '1', 1.0):
            run(edge, 'invalid_' + key + '_' + type(value).__name__, dict(binding, **{key: value}), False, 'VERSION_BINDING')
    for key in ('silver_path', 'gold_path'):
        run(edge, 'wrong_' + key, dict(binding, **{key: 's3a://unrelated/table'}), False, 'VERSION_BINDING')
    run(edge, 'wrong_handoff_digest', dict(binding, handoff_logical_sha256='0' * 64), False, 'HANDOFF_BINDING')
    run(edge, 'normalized_self_review', dict(binding, reviewer='  ' + binding['author'].upper() + '  '), False, 'REVIEW')
    mixed = dict(binding, gold_source_silver_version=0)
    run(edge, 'matching_but_mixed_expected', mixed, False, 'VERSION_BINDING', dict(expected, gold_source_silver_version=0))
    run(edge, 'malformed_draft', None, False)
    return required, edge


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', default='config/team.json')
    parser.add_argument('--trusted-manifest', required=True)
    args = parser.parse_args()
    config = read_json(args.config)
    ev = ROOT / config['evidence_dir']
    state = read_json(ev / 'state.json')
    candidate = ev / 'release_candidate'
    binding = read_json(candidate / 'snapshot_binding.json')
    reviewer = next(member for member in read_json(ROOT / 'team.json') if member['role'] == 'E')
    expected = expected_binding(state)
    run_id = str(uuid.uuid4())
    inputs = [(config['store_id'], state[layer + '_path'] + '@' + str(state[layer + '_final_version'])) for layer in ('silver', 'gold')]
    lineage_path = ev / 'role_E_review_lineage.jsonl'
    emit_lineage(lineage_path, 'START', run_id, 'role_E_review', inputs, [])
    prediction = {'member_id': reviewer['student_id'], 'member_name': reviewer['name'], 'role': 'E',
                  'recorded_at_utc': datetime.now(timezone.utc).isoformat(), 'expected_binding': expected,
                  'predictions': ['Valid binding accepts; mixed snapshots/self-review/missing steward/wrong code reject.',
                                  'All 16 trusted inputs and the baseline logical digest match A handoff identity.',
                                  'Every field of live pinned Gold equals an independent Decimal recomputation from pinned Silver.'],
                  'execution_mode': 'AUTOMATED_LIVE_DELTA_READ'}
    write_json(ev / 'role_E_prediction.json', prediction)
    checks = []
    spark = None

    def check(name, condition, observed):
        checks.append({'check_id': name, 'passed': bool(condition), 'observed': observed})
        if not condition:
            raise ValueError('ROLE_E_CHECK_FAILED:' + name)

    try:
        from common import verify_inputs
        integrity = verify_inputs(ROOT, args.trusted_manifest)
        check('E_cross_A_trusted_inputs', len(integrity['checks']) == 16
              and integrity['trusted_manifest_sha256'] == state['trusted_manifest_sha256'] == binding['trusted_manifest_sha256'],
              {'artifacts': len(integrity['checks']), 'trusted_manifest_sha256': integrity['trusted_manifest_sha256']})
        registry = load_registry(ROOT / 'data/handoff/sensors.csv')
        handoff = validate_rows(load_csv(ROOT / 'data/handoff/curated.csv'), registry, True)
        check('E_cross_A_logical_handoff', len(handoff) == 9610 and logical_sha256(handoff) == state['handoff_logical_sha256']
              and state['handoff_kind'] == binding['handoff_kind'] == 'course_fallback_not_student_approval',
              {'rows': len(handoff), 'handoff_kind': state['handoff_kind'], 'logical_sha256': logical_sha256(handoff)})
        paths = {layer: config['lakehouse_root'].rstrip('/') + '/' + config['group_id'] + '/' + config['run_label'] + '/tables/' + layer
                 for layer in ('silver', 'gold')}
        check('E_concrete_expected_paths', all(state[layer + '_path'] == path for layer, path in paths.items()), paths)
        required, edge = gate_cases(binding, expected)
        write_json(ev / 'role_E_gate_tests.json', required)
        write_json(ev / 'role_E_edge_cases.json', edge)
        check('E_R01_R05_required_gates', all(r['test_passed'] for r in required), {'cases': len(required), 'expected_values_source': 'state.json'})
        check('E_missing_keys_and_edge_cases', all(r['test_passed'] for r in edge), {'cases': len(edge), 'arguments_unchanged': True})
        check('E_reviewer_assignment', binding['reviewer'].startswith(reviewer['student_id'] + ' - ')
              and binding['reviewer'] != binding['author'],
              {'assigned_reviewer': binding['reviewer'], 'transformation_author': binding['author'], 'steward': binding['steward']})
        implementation = sha256_file(ROOT / 'templates/student_impl.py')
        check('E_generating_and_validator_code', implementation == expected['code_sha256'] == binding['code_sha256']
              == binding['release_validator_sha256']
              == sha256_file(ev / 'code_versions' / (expected['code_sha256'] + '.py')),
              {'generating_code_sha256': expected['code_sha256'], 'validator_sha256': binding['release_validator_sha256']})
        manifest = read_json(candidate / 'release_manifest.json')
        expected_objects = {'gold.json', 'snapshot_binding.json', 'catalog.json'}
        check('E_candidate_manifest_hashes', manifest['binding'] == binding
              and manifest['status'] == 'CANDIDATE_NOT_OWNER_PUBLISHED'
              and {r['key'] for r in manifest['objects']} == expected_objects
              and all((candidate / r['key']).stat().st_size == r['bytes']
                      and sha256_file(candidate / r['key']) == r['sha256'] for r in manifest['objects']),
              {'objects': manifest['objects'], 'status': manifest['status']})
        from jsonschema import Draft202012Validator, FormatChecker
        schema_path = ROOT / 'schemas/openlineage/OpenLineage-2-0-2.json'
        schema = read_json(schema_path)
        validator = Draft202012Validator(dict(schema, **{'$ref': '#/$defs/RunEvent'}), format_checker=FormatChecker())
        events = [json.loads(line) for line in (ev / 'lineage.jsonl').read_text(encoding='utf-8').splitlines() if line]
        for event in events:
            validator.validate(event)
        issues, lineage_summary = lineage_issues(events, state)
        write_json(ev / 'role_E_lineage_check.json', dict(lineage_summary, passed=not issues, issues=issues,
                   official_schema_sha256=sha256_file(schema_path)))
        check('E_official_lineage_and_snapshot_IO', not issues, lineage_summary)

        spark = make_spark(config)
        runtime = runtime_info(spark, config)
        write_json(ev / 'runtime_role_E_check.json', runtime)
        check('E_runtime_baseline', runtime['spark'] == runtime['delta_spark'] == '4.0.1'
              and runtime['python'].startswith('3.11.') and runtime['java'].split('.')[0] in ('17', '21')
              and runtime['timezone'] == 'UTC', runtime)
        before = {layer: version(spark, state[layer + '_path']) for layer in ('silver', 'gold')}
        baseline = rows_from_silver(spark, state['silver_path'], state['silver_initial_version'], registry)
        current = rows_from_silver(spark, binding['silver_path'], binding['silver_version'], registry)
        check('E_cross_A_actual_baseline', logical_sha256(baseline) == logical_sha256(handoff),
              {'silver_initial_version': state['silver_initial_version'], 'rows': len(baseline), 'logical_sha256': logical_sha256(baseline)})
        check('E_pinned_Silver_matches_update', logical_sha256(current) == read_json(ev / 'incremental.json')['logical_sha256_after'],
              {'silver_version': binding['silver_version'], 'rows': len(current), 'logical_sha256': logical_sha256(current)})
        reference = aggregate(current)
        silver_frame = snapshot(spark, binding['silver_path'], binding['silver_version'])
        gold_frame = snapshot(spark, binding['gold_path'], binding['gold_version'])
        gold = sorted([serial(row) for row in small_records(gold_frame)], key=lambda r: (r['site'], r['event_date']))
        exported = read_json(candidate / 'gold.json')
        check('E_independent_Gold_recomputation', reference == gold == exported,
              {'silver_rows': len(current), 'gold_groups': len(gold), 'fields_compared_per_group': 11,
               'method': 'Independent Decimal/count/distinct computation from actual pinned Silver; no build_gold call.'})
        silver_columns = read_json(ROOT / 'schemas/silver_schema.json')['columns']
        gold_columns = read_json(ROOT / 'schemas/gold_schema.json')['columns']
        check('E_actual_table_schema', dict(silver_frame.dtypes) == {r['name']: r['type'].lower() for r in silver_columns}
              and dict(gold_frame.dtypes) == {r['name']: r['type'].lower() for r in gold_columns},
              {'silver_columns': len(silver_frame.columns), 'gold_columns': len(gold_frame.columns)})
        catalog = read_json(candidate / 'catalog.json')
        issues = catalog_issues(catalog, binding, state, len(current), len(gold), silver_columns, gold_columns)
        write_json(ev / 'role_E_catalog_check.json', {'passed': not issues, 'issues': issues,
                   'silver_rows': len(current), 'gold_groups': len(gold), 'schema_metadata_compared': True})
        check('E_catalog_values_units_and_retention', not issues, {'issues': issues, 'allowed_use': catalog['allowed_use'], 'retention': catalog['retention']})
        after = {layer: version(spark, state[layer + '_path']) for layer in ('silver', 'gold')}
        check('E_read_only_table_versions', before == after, {'before': before, 'after': after})
        rationale = ['All mandatory refusal cases and missing-key cases behaved as required.',
                     'Pinned Gold matches every independent recomputed field from pinned Silver.',
                     'Trusted input bytes, baseline identity, archived code and candidate object hashes agree.',
                     'Catalog metadata and paired lineage describe the measured snapshots and their limits.']
        write_json(ev / 'release_review.json', {
            'status': 'TECHNICAL_CANDIDATE_REVIEW_PASSED', 'release_id': binding['release_id'],
            'assigned_reviewer': binding['reviewer'], 'review_run_id': run_id,
            'review_completed_at_utc': datetime.now(timezone.utc).isoformat(), 'snapshot_binding': expected,
            'rationale': rationale, 'required_gate_cases': len(required), 'additional_refusal_cases': len(edge),
            'check_count': len(checks), 'writes_to_Delta_tables': 0, 'owner_publication_claimed': False,
            'human_signature_claimed': False, 'student_demonstration_attested': False})
        result = {'passed': True, 'member_id': reviewer['student_id'], 'member_name': reviewer['name'], 'role': 'E',
                  'execution_mode': 'AUTOMATED_LIVE_DELTA_READ', 'completed_at_utc': datetime.now(timezone.utc).isoformat(),
                  'run_id': run_id, 'code_sha256': sha256_file(__file__), 'checks': checks,
                  'silver_version': binding['silver_version'], 'gold_version': binding['gold_version'],
                  'writes_to_Delta_tables': 0, 'owner_publication_claimed': False}
        write_json(ev / 'role_E_check.json', result)
        (ev / 'code_versions' / (result['code_sha256'] + '.py')).write_bytes(Path(__file__).read_bytes())
        outputs = [('local-evidence', str(ev / name)) for name in ('role_E_check.json', 'release_review.json', 'role_E_gate_tests.json', 'role_E_catalog_check.json', 'role_E_lineage_check.json')]
        emit_lineage(lineage_path, 'COMPLETE', run_id, 'role_E_review', inputs, outputs)
        print(f'PASS: {len(checks)} role E checks; {len(required)} required gates; {len(edge)} refusal edge cases; zero Delta writes.')
    except Exception as exc:
        write_json(ev / 'role_E_check.json', {'passed': False, 'checks': checks, 'error_type': type(exc).__name__})
        emit_lineage(lineage_path, 'FAIL', run_id, 'role_E_review', inputs, [])
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == '__main__':
    main()
