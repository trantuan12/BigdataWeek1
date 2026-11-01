"""Independent Decimal checks for role D and a technical review of roles A/E."""
import argparse
from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'src'), str(ROOT / 'templates')]
from common import read_json, write_json, sha256_file
from student_impl import validate_release_binding


def fixed(value, places=6):
    return format(Decimal(value).quantize(Decimal(10) ** -places, rounding=ROUND_HALF_UP), f'.{places}f')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', default='config/team.json')
    args = parser.parse_args()
    config = read_json(args.config)
    ev = Path(config['evidence_dir'])
    state = read_json(ev / 'state.json')
    write_json(ev / 'role_D_prediction.json', {
        'member_id': '24022463', 'member_name': 'Đàm Quang Tiến', 'role': 'D',
        'recorded_at_utc': datetime.now(timezone.utc).isoformat(),
        'prediction': 'Gold count/sum match retained Silver; daily mean uses total observation count. '
                      'The date-partitioned layout prunes to the selected date and all six answers agree.',
        'execution_mode': 'AUTOMATED_READ_ONLY',
    })
    checks = []

    def check(key, condition, observed):
        checks.append({'check_id': key, 'passed': bool(condition), 'observed': observed})
        if not condition:
            write_json(ev / 'role_D_check.json', {'passed': False, 'checks': checks})
            raise ValueError('ROLE_D_CHECK_FAILED:' + key)

    import json
    silver = [json.loads(line) for line in (ev / 'silver_after_batch02.jsonl').read_text(encoding='utf-8').splitlines()]
    gold = read_json(ev / 'gold_after_batch02.json')
    n = len(silver)
    total = sum(Decimal(row['temperature_c']) for row in silver)
    check('D_B_conservation', sum(row['n_observations'] for row in gold) == n
          and sum(Decimal(row['sum_temperature_c']) for row in gold) == total,
          {'silver_rows': n, 'gold_observations': sum(row['n_observations'] for row in gold),
           'silver_temperature_sum_c': fixed(total, 2), 'gold_groups': len(gold)})
    days = defaultdict(list)
    for row in silver:
        days[row['event_time_utc'][:10]].append(row)
    q2 = read_json(ev / 'Q2_weighted_day.json')['rows']
    weighted_days = []
    for row in q2:
        day = row['event_date']
        numerator = sum(Decimal(value['temperature_c']) for value in days[day])
        denominator = len(days[day])
        expected = fixed(numerator / denominator)
        groups = [g for g in gold if g['event_date'] == day]
        naive = fixed(sum(Decimal(g['mean_temperature_c']) for g in groups) / len(groups))
        check('D_B_weighted_' + day, row['n_observations'] == denominator
              and row['weighted_mean_c'] == expected and row['naive_mean_of_means_c'] == naive,
              {'sum_temperature_c': fixed(numerator, 2), 'denominator_observations': denominator,
               'weighted_mean_c': expected, 'naive_mean_of_means_c': naive,
               'site_counts': {g['site']: g['n_observations'] for g in groups}})
        weighted_days.append({'event_date': day, 'sum_temperature_c': fixed(numerator, 2),
                             'n_observations': denominator, 'weighted_mean_c': expected,
                             'naive_mean_of_means_c': naive})
    q3 = read_json(ev / 'Q3_above25_rate.json')['rows']
    groups = {(g['site'], g['event_date']): g for g in gold}
    check('D_Q3_strict_threshold_denominator', all(
        row['n_above_25'] == groups[row['site'], row['event_date']]['n_above_25']
        and row['above25_rate'] == fixed(Decimal(row['n_above_25']) / row['n_observations'])
        for row in q3), {'groups': len(q3), 'predicate': 'temperature_c > 25',
                       'denominator': 'all retained observations in the same site/day'})
    bench = read_json(ev / 'benchmark_report.json')
    expected = [{'site': g['site'], 'n_observations': g['n_observations'] * bench['benchmark_copies'],
                 'sum_temperature_c': fixed(Decimal(g['sum_temperature_c']) * bench['benchmark_copies'], 2),
                 'n_late': g['n_late'] * bench['benchmark_copies']}
                for g in gold if g['event_date'] == '2026-02-06']
    expected.sort(key=lambda row: row['site'])
    check('D_A_B_results', bench['query_result'] == expected
          and len(bench['measurements']) == 6 and all(r['result_equal'] for r in bench['measurements']),
          {'expected_from_operational_gold': expected, 'six_equivalent_evaluations': True})
    metrics = read_json(ev / 'benchmark_scan_metrics.json')
    scan_counts = {}
    for execution in metrics['executions']:
        counts = [metric['value'] for node in execution['scan_nodes'] for metric in node['metrics']
                  if metric['name'] == 'number of files read']
        scan_counts.setdefault(execution['layout'], []).append(counts)
    plan_a = (ev / 'plan_A.txt').read_text(encoding='utf-8')
    plan_b = (ev / 'plan_B.txt').read_text(encoding='utf-8')
    check('D_partition_filters_and_scan_metrics',
          ('PartitionFilters:' not in plan_a or 'PartitionFilters: []' in plan_a)
          and 'PushedFilters:' in plan_a and '2026-02-06' in plan_a
          and 'PartitionFilters:' in plan_b and '2026-02-06' in plan_b
          and len(metrics['executions']) == 8 and all(scan_counts.get(label) for label in ('A', 'B')),
          {'actual_scan_file_counts': scan_counts,
           'total_table_files': {label: detail['total_table_data_files'] for label, detail in bench['layout_details'].items()},
           'metrics_source': metrics['source']})
    check('D_no_operational_replica_inflation', n == bench['source_rows']
          and bench['expanded_rows'] == n * 100 and sum(g['n_observations'] for g in gold) == n,
          {'operational_observations': n, 'benchmark_rows': bench['expanded_rows']})
    result = {'passed': True, 'member_id': '24022463', 'member_name': 'Đàm Quang Tiến', 'role': 'D',
              'execution_mode': 'AUTOMATED_READ_ONLY', 'completed_at_utc': datetime.now(timezone.utc).isoformat(),
              'silver_version': state['silver_final_version'], 'gold_version': state['gold_final_version'],
              'checks': checks, 'daily_denominators': weighted_days}
    write_json(ev / 'role_D_check.json', result)

    integrity = read_json(ev / 'input_integrity.json')
    worker = read_json(ev / 'worker_probe.json')
    gates = read_json(ev / 'release_gate_tests.json')
    binding = read_json(ev / 'release_candidate/snapshot_binding.json')
    keys = ('handoff_logical_sha256', 'code_sha256', 'silver_path', 'silver_version',
            'gold_path', 'gold_version', 'gold_source_silver_version')
    expected_binding = {key: binding[key] for key in keys}
    missing_key_results = {}
    for key in (*keys, 'author', 'reviewer', 'steward'):
        candidate = deepcopy(binding)
        candidate.pop(key)
        passed, reasons = validate_release_binding(candidate, expected_binding)
        if passed or not reasons:
            raise ValueError('E_MISSING_KEY_ACCEPTED:' + key)
        missing_key_results[key] = reasons
    review = {
        'A': {'passed': len(integrity['checks']) == 16 and all(r['passed'] for r in integrity['checks'])
              and read_json(ev / 'delta_schema_probe.json')['passed'] and worker['distinct_hosts'] == 2,
              'trusted_input_checks': len(integrity['checks']), 'worker_containers': worker['distinct_hosts'],
              'handoff_kind': state['handoff_kind'],
              'notes': 'Current run revalidated original ZIP bytes and uses Java 17/21 baseline. '
                       'Container placements do not establish separate physical servers.'},
        'E': {'passed': len(gates) == 5 and all(gate['test_passed'] for gate in gates)
              and read_json(ev / 'release_check.json')['snapshot_recompute_equal'],
              'gate_cases': gates, 'missing_key_rejections': missing_key_results,
              'snapshot_pair': {key: binding[key] for key in ('silver_version', 'gold_version', 'gold_source_silver_version')},
              'status': 'CANDIDATE_NOT_OWNER_PUBLISHED'},
    }
    write_json(ev / 'role_A_E_review.json', review)
    if not all(section['passed'] for section in review.values()):
        raise ValueError('A_OR_E_TECHNICAL_REVIEW_FAILED')
    for path in (Path(__file__), ROOT / 'tools/analytics_and_dashboard.py', ROOT / 'src/session.py', ROOT / 'src/run_lab.py'):
        (ev / 'code_versions' / (sha256_file(path) + '.py')).write_bytes(path.read_bytes())
    print(f'PASS: {len(checks)} role D checks; roles A/E technical checks pass.')


if __name__ == '__main__':
    main()
