"""Independent verification for Role C (Task 3 M01-M08) and cross-check of Role D (C_CHECK_D)."""
import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
import json
from pathlib import Path
import random
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'src'), str(ROOT / 'templates'), str(ROOT / 'tools')]
from common import (as_time, load_csv, load_registry, logical_sha256,
                    read_json, sha256_file, validate_rows, write_json)
from check_role_a import SOURCE_PATHS, source_projection
from run_lab import mutation_tests
from verify_results import aggregate


def fixed(value, places=2):
    return format(Decimal(value).quantize(Decimal(10) ** -places, rounding=ROUND_HALF_UP), f'.{places}f')


def revision_order(row):
    """Deterministic ordering key matching choose_revisions and merge_silver."""
    return (-int(as_time(row['ingest_time_utc']).timestamp()),
            row['source_object'],
            int(row['source_row']))


def select_incoming_winners(rows):
    """Pure-Python reference for choose_revisions over arbitrary input row order."""
    grouped = defaultdict(list)
    for row in rows:
        grouped[row['record_id']].append(row)
    winners = {}
    superseded = []
    for key, candidates in sorted(grouped.items()):
        ordered = sorted(candidates, key=revision_order)
        winners[key] = ordered[0]
        superseded.extend(ordered[1:])
    return winners, superseded


def precedes_in_order(incoming_row, stored_row):
    """Strict update predicate matching merge_silver."""
    return revision_order(incoming_row) < revision_order(stored_row)


def apply_merge_reference(baseline_rows, incoming_rows):
    """Pure-Python reference for choose_revisions followed by merge_silver."""
    winners, superseded = select_incoming_winners(incoming_rows)
    merged = {row['record_id']: dict(row) for row in baseline_rows}
    counts = Counter()
    traces = {}
    for key, winner in sorted(winners.items()):
        previous = merged.get(key)
        if previous is None:
            decision = 'inserted'
            merged[key] = dict(winner)
        elif precedes_in_order(winner, previous):
            decision = ('updated_newer'
                        if winner['ingest_time_utc'] > previous['ingest_time_utc']
                        else 'updated_tied')
            merged[key] = dict(winner)
        else:
            decision = ('unchanged_equal'
                        if revision_order(winner) == revision_order(previous)
                        else 'unchanged_stale')
        counts[decision] += 1
        traces[key] = {
            'record_id': key,
            'decision': decision,
            'previous': previous,
            'incoming_winner': winner,
            'selected': merged[key],
        }
    return merged, winners, superseded, dict(counts), traces


def expected_benchmark_from_gold(gold_rows, copies, target_date='2026-02-06'):
    """Compute the exact expected A/B benchmark aggregate from operational Gold."""
    result = [
        {
            'site': row['site'],
            'n_observations': int(row['n_observations']) * int(copies),
            'sum_temperature_c': fixed(Decimal(row['sum_temperature_c']) * int(copies), 2),
            'n_late': int(row['n_late']) * int(copies),
        }
        for row in gold_rows
        if row['event_date'] == target_date
    ]
    result.sort(key=lambda item: item['site'])
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', default='config/team.json')
    args = parser.parse_args()
    config = read_json(ROOT / args.config if not Path(args.config).is_absolute() else args.config)
    ev = ROOT / config['evidence_dir']
    state = read_json(ev / 'state.json')
    member_c = next(member for member in read_json(ROOT / 'team.json') if member['role'] == 'C')

    prediction = {
        'member_id': member_c['student_id'],
        'member_name': member_c['name'],
        'role': 'C',
        'recorded_at_utc': datetime.now(timezone.utc).isoformat(),
        'execution_mode': 'AUTOMATED_READ_ONLY',
        'predictions': {
            'M01_cardinality': '140 physical revisions reduce to 130 winners and 10 superseded same-key rows.',
            'M02_decisions': '100 inserted, 20 updated_newer, 10 unchanged_stale; R010001 picks row 1 (19.10 C) over row 131 (20.10 C); R003001 keeps baseline row 3001 (18.10 C) over stale row 121 (17.10 C); R002001 updates from row 2001 (18.10 C) to row 101 (20.10 C).',
            'M03_determinism': 'Winner values and provenance are invariant to row order; last-row dictionary lookup fails on all 10 tied keys.',
            'M04_M05_replay': 'Bronze replay keeps 9750 receipts; Silver replay v2 adds a no-op commit with 0 inserts/updates and identical logical digest d0e76f8b... to Silver v1.',
            'M06_M07_M08': 'Silver versionAsOf=0 equals the 9610-row handoff; 3 invalid batch probes reject with 0 writes; Gold v1 matches all 20 recomputed site/day groups.',
            'C_CHECK_D': 'All 6 A/B benchmark evaluations in A-B-B-A-A-B order return identical 4-site aggregates equal to 100x operational Gold on 2026-02-06, while operational Gold retains 9710 observations.',
        },
    }
    write_json(ev / 'role_C_prediction.json', prediction)
    checks = []

    def check(key, condition, observed):
        checks.append({'check_id': key, 'passed': bool(condition), 'observed': observed})
        if not condition:
            write_json(ev / 'role_C_check.json', {'passed': False, 'checks': checks})
            raise ValueError('ROLE_C_CHECK_FAILED:' + key)

    registry = load_registry(ROOT / 'data/handoff/sensors.csv')
    baseline = validate_rows(load_csv(ROOT / 'data/handoff/curated.csv'), registry, True)
    incoming = validate_rows(load_csv(ROOT / 'data/changes/changes_batch02.csv'), registry, False)

    def load_jsonl(name):
        return [json.loads(line) for line in (ev / name).read_text(encoding='utf-8').splitlines() if line]

    silver_v0 = load_jsonl('silver_baseline.jsonl')
    silver_v1 = load_jsonl('silver_after_batch02.jsonl')
    silver_v2 = load_jsonl('silver_replay.jsonl')
    silver_tt = load_jsonl('silver_time_travel.jsonl')
    inc = read_json(ev / 'incremental.json')
    decisions_doc = read_json(ev / 'revision_decisions.json')
    trace_doc = read_json(ev / 'row_trace.json')
    live_doc = read_json(ev / 'live_table_verification.json')

    merged, winners, superseded, decision_counts, traces = apply_merge_reference(baseline, incoming)
    superseded_summary = [
        {
            'record_id': row['record_id'],
            'superseded_row': int(row['source_row']),
            'superseded_temp_c': row['temperature_c'],
            'winner_row': int(winners[row['record_id']]['source_row']),
            'winner_temp_c': winners[row['record_id']]['temperature_c'],
            'ingest_time_utc': row['ingest_time_utc'],
        }
        for row in sorted(superseded, key=lambda r: (r['record_id'], int(r['source_row'])))
    ]
    check('C_M01_incoming_winner_cardinality',
          len(incoming) == 140
          and len(winners) == inc['incoming_winners'] == 130
          and len(superseded) == 10
          and decisions_doc['summary']['superseded_incoming_rows'] == 10,
          {
              'incoming_physical_rows': len(incoming),
              'incoming_winners': len(winners),
              'superseded_rows': len(superseded),
              'superseded_details': superseded_summary,
          })

    actual_by_id = {row['record_id']: row for row in silver_v1}
    check('C_M02_all_revision_decisions',
          actual_by_id == merged
          and decision_counts == {'updated_newer': 20, 'unchanged_stale': 10, 'inserted': 100}
          and decision_counts == decisions_doc['summary']['decisions']
          and len(silver_v1) == 9710,
          {
              'baseline_rows': len(baseline),
              'post_merge_rows': len(silver_v1),
              'decisions': decision_counts,
          })

    trace_examples = {item['record_id']: item for item in trace_doc['examples']}
    representative_traces = {}
    for key, expected_decision, expected_row, expected_temp in (
        ('R010001', 'inserted', 1, '19.10'),
        ('R003001', 'unchanged_stale', 3001, '18.10'),
        ('R002001', 'updated_newer', 101, '20.10'),
    ):
        selected = actual_by_id[key]
        src_proj = source_projection(selected['source_object'], int(selected['source_row']))
        src_hash = sha256_file(ROOT / SOURCE_PATHS[selected['source_object']])
        example = trace_examples[key]
        ok = (traces[key]['decision'] == example['decision'] == expected_decision
              and selected == example['selected']
              and int(selected['source_row']) == expected_row
              and selected['temperature_c'] == expected_temp
              and all(selected[k] == v for k, v in src_proj.items())
              and selected['source_sha256'] == src_hash)
        representative_traces[key] = {
            'decision': expected_decision,
            'previous': traces[key]['previous'],
            'incoming_winner': traces[key]['incoming_winner'],
            'selected': selected,
            'source_projection': src_proj,
        }
        check('C_M02_trace_' + key, ok, representative_traces[key])

    reversed_winners, _ = select_incoming_winners(list(reversed(incoming)))
    rng = random.Random(20261108)
    shuffled_input = list(incoming)
    rng.shuffle(shuffled_input)
    shuffled_winners, _ = select_incoming_winners(shuffled_input)
    last_row_dict = {row['record_id']: row for row in incoming}
    wrong_on_ties = [
        key for key in winners
        if len([r for r in incoming if r['record_id'] == key]) > 1
        and last_row_dict[key] != winners[key]
    ]
    check('C_M03_deterministic_ties_and_reordering',
          inc['winner_reordering_passed']
          and winners == reversed_winners == shuffled_winners
          and len(wrong_on_ties) == 10,
          {
              'forward_equals_reversed': winners == reversed_winners,
              'forward_equals_shuffled': winners == shuffled_winners,
              'tied_keys_count': len(wrong_on_ties),
              'naive_last_row_wrong_keys': sorted(wrong_on_ties),
          })

    bronze_hist = {entry['version']: entry for entry in live_doc['tables']['bronze']['history']}
    check('C_M04_bronze_receipt_replay',
          inc['bronze_before_replay'] == inc['bronze_after_replay'] == len(baseline) + len(incoming) == 9750
          and inc['bronze_receipt_sha256_before_replay'] == inc['bronze_receipt_sha256_after_replay']
          and inc['bronze_after_version'] == inc['bronze_replay_version'] == 1
          and bronze_hist[0]['operationMetrics']['numOutputRows'] == '9610'
          and bronze_hist[1]['operationMetrics']['numTargetRowsInserted'] == '140',
          {
              'bronze_receipts': inc['bronze_after_replay'],
              'bronze_receipt_sha256': inc['bronze_receipt_sha256_after_replay'],
              'bronze_version_after': inc['bronze_after_version'],
              'bronze_version_replay': inc['bronze_replay_version'],
          })

    silver_hist = {entry['version']: entry for entry in live_doc['tables']['silver']['history']}
    v1_metrics = silver_hist[1]['operationMetrics']
    v2_metrics = silver_hist[2]['operationMetrics']
    second_merged, _, _, second_counts, _ = apply_merge_reference(silver_v1, incoming)
    check('C_M05_silver_merge_replay_idempotence',
          silver_v1 == silver_v2
          and logical_sha256(silver_v1) == logical_sha256(silver_v2) == inc['logical_sha256_after'] == inc['logical_sha256_replay']
          and inc['silver_after_version'] == 1 and inc['silver_replay_version'] == 2
          and v1_metrics['numTargetRowsInserted'] == '100'
          and v1_metrics['numTargetRowsMatchedUpdated'] == '20'
          and v1_metrics['numTargetRowsCopied'] == '9590'
          and v2_metrics['numTargetRowsInserted'] == '0'
          and v2_metrics['numTargetRowsMatchedUpdated'] == '0'
          and v2_metrics['numTargetRowsCopied'] == '9710'
          and second_merged == actual_by_id
          and second_counts == {'unchanged_equal': 120, 'unchanged_stale': 10},
          {
              'logical_sha256': inc['logical_sha256_after'],
              'silver_v1_metrics': {k: v1_metrics[k] for k in ('numSourceRows', 'numTargetRowsInserted', 'numTargetRowsMatchedUpdated', 'numTargetRowsCopied', 'numOutputRows')},
              'silver_v2_replay_metrics': {k: v2_metrics[k] for k in ('numSourceRows', 'numTargetRowsInserted', 'numTargetRowsMatchedUpdated', 'numTargetRowsCopied', 'numOutputRows')},
              'second_application_decisions': second_counts,
          })

    check('C_M06_historical_time_travel',
          inc['time_travel_passed']
          and silver_tt == silver_v0
          and logical_sha256(silver_tt) == logical_sha256(baseline) == state['handoff_logical_sha256']
          and len(silver_tt) == 9610,
          {
              'historical_version': state['silver_initial_version'],
              'rows': len(silver_tt),
              'logical_sha256': logical_sha256(silver_tt),
          })

    recorded_mutations = read_json(ev / 'input_contract_tests.json')
    recomputed_mutations = mutation_tests(incoming, registry)
    check('C_M07_invalid_batch_refusal_probes',
          recorded_mutations == recomputed_mutations
          and len(recorded_mutations) == 3
          and all(item['passed'] and item['writes_performed'] == 0 for item in recorded_mutations),
          {'probes': recorded_mutations})

    gold_v0 = read_json(ev / 'gold_baseline.json')
    gold_v1 = read_json(ev / 'gold_after_batch02.json')
    gold_candidate = read_json(ev / 'release_candidate/gold.json')
    recomputed_gold_v0 = aggregate(silver_v0)
    recomputed_gold_v1 = aggregate(silver_v1)
    check('C_M08_gold_post_merge_consistency',
          gold_v0 == recomputed_gold_v0
          and gold_v1 == recomputed_gold_v1 == gold_candidate
          and len(gold_v1) == 20
          and sum(r['n_observations'] for r in gold_v1) == len(silver_v1) == 9710
          and sum(Decimal(r['sum_temperature_c']) for r in gold_v1) == sum(Decimal(r['temperature_c']) for r in silver_v1),
          {
              'baseline_groups': len(gold_v0),
              'post_merge_groups': len(gold_v1),
              'post_merge_observations': sum(r['n_observations'] for r in gold_v1),
              'post_merge_temperature_sum_c': fixed(sum(Decimal(r['sum_temperature_c']) for r in gold_v1), 2),
          })

    # Cross-check Role D: result-equivalence evidence across Layout A and Layout B
    bench = read_json(ev / 'benchmark_report.json')
    scan = read_json(ev / 'benchmark_scan_metrics.json')
    expected_bench = expected_benchmark_from_gold(gold_v1, bench['benchmark_copies'], '2026-02-06')
    silver_feb06 = [row for row in silver_v1 if row['event_time_utc'][:10] == '2026-02-06']
    silver_feb06_by_site = defaultdict(list)
    for row in silver_feb06:
        silver_feb06_by_site[row['site']].append(row)
    expected_from_silver = [
        {
            'site': site,
            'n_observations': len(rows) * int(bench['benchmark_copies']),
            'sum_temperature_c': fixed(sum(Decimal(r['temperature_c']) for r in rows) * int(bench['benchmark_copies']), 2),
            'n_late': sum(1 for r in rows if r['is_late']) * int(bench['benchmark_copies']),
        }
        for site, rows in sorted(silver_feb06_by_site.items())
    ]
    run_sequence = [m['layout'] for m in bench['measurements']]
    check('C_cross_D_benchmark_result_equivalence',
          bench['source_silver_version'] == state['silver_final_version'] == 1
          and bench['source_rows'] == len(silver_v1) == 9710
          and bench['benchmark_copies'] == 100
          and bench['expanded_rows'] == 971000
          and run_sequence == list('ABBAAB')
          and len(bench['measurements']) == 6
          and all(m['result_equal'] and m['elapsed_seconds'] > 0 for m in bench['measurements'])
          and bench['query_result'] == expected_bench == expected_from_silver,
          {
              'source_silver_version': bench['source_silver_version'],
              'run_sequence': ''.join(run_sequence),
              'six_runs_equal': all(m['result_equal'] for m in bench['measurements']),
              'query_result': bench['query_result'],
              'operational_feb06_rows': len(silver_feb06),
              'expanded_feb06_workload_rows': sum(r['n_observations'] for r in bench['query_result']),
          })

    plan_a = (ev / 'plan_A.txt').read_text(encoding='utf-8')
    plan_b = (ev / 'plan_B.txt').read_text(encoding='utf-8')
    measured_execs = [e for e in scan['executions'] if e.get('measurement_number') is not None]
    scan_files_by_layout = {}
    for exe in measured_execs:
        files = [int(m['value']) for node in exe['scan_nodes'] for m in node['metrics'] if m['name'] == 'number of files read']
        scan_files_by_layout.setdefault(exe['layout'], []).extend(files)
    check('C_cross_D_plan_metrics_and_gold_isolation',
          ('PartitionFilters:' not in plan_a or 'PartitionFilters: []' in plan_a)
          and 'PushedFilters: [IsNotNull(event_date), EqualTo(event_date,2026-02-06)]' in plan_a
          and 'PartitionFilters:' in plan_b and '2026-02-06' in plan_b
          and scan_files_by_layout == {'A': [32, 32, 32], 'B': [1, 1, 1]}
          and sum(r['n_observations'] for r in gold_v1) == 9710,
          {
              'layout_A_table_files': bench['layout_details']['A']['total_table_data_files'],
              'layout_B_table_files': bench['layout_details']['B']['total_table_data_files'],
              'measured_files_read': scan_files_by_layout,
              'median_A_s': bench['median_seconds']['A'],
              'median_B_s': bench['median_seconds']['B'],
              'A_over_B_ratio': bench['A_median_over_B_median'],
              'operational_gold_observations': sum(r['n_observations'] for r in gold_v1),
          })

    result = {
        'passed': True,
        'member_id': member_c['student_id'],
        'member_name': member_c['name'],
        'role': 'C',
        'execution_mode': 'AUTOMATED_READ_ONLY',
        'completed_at_utc': datetime.now(timezone.utc).isoformat(),
        'code_sha256': sha256_file(__file__),
        'silver_initial_version': state['silver_initial_version'],
        'silver_final_version': state['silver_final_version'],
        'silver_replay_version': state['silver_replay_version'],
        'gold_final_version': state['gold_final_version'],
        'decision_counts': decision_counts,
        'superseded_rows': superseded_summary,
        'representative_traces': representative_traces,
        'cross_check_d_query_result': bench['query_result'],
        'checks': checks,
    }
    write_json(ev / 'role_C_check.json', result)
    (ev / 'code_versions' / (result['code_sha256'] + '.py')).write_bytes(Path(__file__).read_bytes())
    print(f"PASS: {len(checks)} role C checks (M01-M08 and C_CHECK_D result equivalence).")


if __name__ == '__main__':
    main()
