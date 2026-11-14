"""Role B evidence: Silver/Gold modeling checks (B01-B04) and cross-check of Role C
stale/tied revision results (B_CHECK_C).

Read-only: never writes a Delta table. Uses only the standard library so it can be run
on the driver container or on any workstation that holds the evidence directory:

    python tools/check_role_b.py --config config/team.json

The prediction file is written BEFORE any assertion is evaluated.
"""
import argparse
import ast
from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from functools import cmp_to_key
import hashlib
import json
from pathlib import Path
import random
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'src'), str(ROOT / 'tools')]
from common import (COLUMNS, load_csv, load_registry, logical_sha256, read_json,
                    sha256_file, validate_rows, write_json)
from verify_results import aggregate

IMPL = ROOT / 'templates' / 'student_impl.py'
GOLD_FIELDS = ['site', 'event_date', 'n_observations', 'sum_temperature_c', 'mean_temperature_c',
               'min_temperature_c', 'max_temperature_c', 'n_late', 'late_rate', 'n_above_25', 'n_sensors']
SQL_TO_SPARK = {'STRING': 'string', 'DATE': 'date', 'TIMESTAMP': 'timestamp', 'BOOLEAN': 'boolean',
                'BIGINT': 'long', 'DECIMAL(18,2)': 'decimal(18,2)', 'DECIMAL(18,6)': 'decimal(18,6)',
                'DECIMAL(8,2)': 'decimal(8,2)'}

STALE_KEYS = [f'R0030{i:02d}' for i in range(1, 11)]
TIED_EXISTING_KEYS = [f'R00200{i}' for i in range(1, 6)]
TIED_NEW_KEYS = [f'R01000{i}' for i in range(1, 6)]


def fixed(value, places=6):
    return format(Decimal(value).quantize(Decimal(10) ** -places, rounding=ROUND_HALF_UP), f'.{places}f')


# --------------------------------------------------------------------------------------
# Independent ordering model written from the handout's strict update predicate.
# Deliberately NOT a tuple sort (Role C's reference uses a tuple key): an explicit
# if-chain makes the precedence of the three components visible and testable.
# --------------------------------------------------------------------------------------
def precedes(incoming, current):
    """True iff `incoming` is strictly earlier than `current` in the winner ordering:
    ingest_time_utc DESC, source_object ASC, source_row ASC."""
    if incoming['ingest_time_utc'] != current['ingest_time_utc']:
        return incoming['ingest_time_utc'] > current['ingest_time_utc']   # same UTC text format
    if incoming['source_object'] != current['source_object']:
        return incoming['source_object'] < current['source_object']
    return int(incoming['source_row']) < int(current['source_row'])


def compare(a, b):
    if precedes(a, b):
        return -1
    if precedes(b, a):
        return 1
    return 0


def pick_winner(candidates):
    return sorted(candidates, key=cmp_to_key(compare))[0]


def identity(row):
    return (row['source_object'], int(row['source_row']), row['source_sha256'])


def merge_predicate_sql():
    """Extract the actual `precedes` SQL text used by Role C's merge_silver (no Spark import)."""
    tree = ast.parse(IMPL.read_text(encoding='utf-8'))
    function = next(node for node in tree.body
                    if isinstance(node, ast.FunctionDef) and node.name == 'merge_silver')
    for node in ast.walk(function):
        if (isinstance(node, ast.Assign) and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name) and node.targets[0].id == 'precedes'):
            return ast.literal_eval(node.value)
    raise ValueError('merge_silver predicate not found')


def build_gold_static_audit():
    """Static audit of build_gold: forbidden driver-side or UDF constructs must be absent."""
    tree = ast.parse(IMPL.read_text(encoding='utf-8'))
    function = next(node for node in tree.body
                    if isinstance(node, ast.FunctionDef) and node.name == 'build_gold')
    called = set()
    for node in ast.walk(function):
        if isinstance(node, ast.Call):
            target = node.func
            called.add(target.attr if isinstance(target, ast.Attribute)
                       else getattr(target, 'id', ''))
    forbidden = {'collect', 'toPandas', 'udf', 'pandas_udf', 'toLocalIterator', 'join', 'take'}
    source = ast.get_source_segment(IMPL.read_text(encoding='utf-8'), function)
    return {'calls': sorted(called), 'forbidden_calls_found': sorted(called & forbidden),
            'groupBy_keys_site_event_date': "groupBy('site', 'event_date')" in source,
            'strict_threshold_gt_25': "> F.lit(25)" in source,
            'count_distinct_sensor': "countDistinct('sensor_id')" in source}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', default='config/team.json')
    args = parser.parse_args()
    config = read_json(ROOT / args.config if not Path(args.config).is_absolute() else args.config)
    ev = ROOT / config['evidence_dir']
    state = read_json(ev / 'state.json')
    member = next(m for m in read_json(ROOT / 'team.json') if m['role'] == 'B')

    # ---------------------------------------------------------------- prediction first
    prediction = {
        'member_id': member['student_id'], 'member_name': member['name'], 'role': 'B',
        'recorded_at_utc': datetime.now(timezone.utc).isoformat(),
        'execution_mode': 'AUTOMATED_READ_ONLY',
        'basis': 'Derived from the handout ordering rule and the raw changes_batch02.csv / curated.csv rows, '
                 'before reading Role C Silver exports.',
        'predictions': {
            'B_C01_stale': 'All 10 keys R003001-R003010 arrive with an OLDER ingest time than the current Silver '
                           'row; Silver v1 keeps the baseline observations_a.csv rows unchanged. Example R003005: '
                           'keep row 3005 (18.50 C, ingest 01:03:30Z), reject batch row 125 (17.50 C, 01:03:00Z).',
            'B_C02_tied': '10 keys carry two batch revisions with identical ingest time and source_object; the '
                          'smaller source_row wins. R002005: row 105 (20.50 C) beats row 140 (21.50 C) and, being '
                          'newer than baseline row 2005 (18.50 C), updates Silver. R010005: new key, row 5 wins over 135.',
            'B_C03_order': 'Winners are identical for every input order (both orders of every tied pair, reversed '
                           'and 200 random shuffles).',
            'B_C04_predicate': 'The actual merge_silver SQL predicate updates only on newer ingest, or equal ingest '
                               'with smaller source_object, or equal ingest+object with smaller source_row; equal '
                               'identity and stale rows (even with smaller object/row) are no-ops.',
            'B_C05_replay': 'Second application: stale keys stay unchanged_stale, tied winners become '
                            'unchanged_equal; Silver v2 rows for these 20 keys are identical to v1.',
            'B_C06_gold': 'Each stale batch value is 1.00 C below the kept value and each superseded tied value '
                          'is 1.00 C above its winner. Stale-overwrite would therefore lower 2026-02-03 sums by '
                          '5.00 for zone-1 (S01-S05) and zone-2 (S06-S10): 16046.00->16041.00, 16406.00->16401.00. '
                          'Last-row-wins would raise zone-1 2026-02-02 by 5.00 (13972.50->13977.50) and zone-1 '
                          '2026-02-06 by 5.00 (532.50->537.50). Actual Gold v1 shows neither.',
            'B_DEMO_weighted_2026_02_02': 'Gold components: 57283.50 / 2489 = 23.014665 C; '
                                          'naive AVG(site means) = 23.016459 C.',
        },
    }
    write_json(ev / 'role_B_prediction.json', prediction)

    checks = []

    def check(key, condition, observed, out_name):
        checks.append({'check_id': key, 'passed': bool(condition), 'observed': observed})
        if not condition:
            write_json(ev / out_name, {'passed': False, 'checks': checks})
            raise ValueError('ROLE_B_CHECK_FAILED:' + key)

    def load_jsonl(name):
        return [json.loads(line) for line in (ev / name).read_text(encoding='utf-8').splitlines() if line]

    registry = load_registry(ROOT / 'data/handoff/sensors.csv')
    baseline = validate_rows(load_csv(ROOT / 'data/handoff/curated.csv'), registry, True)
    incoming = validate_rows(load_csv(ROOT / 'data/changes/changes_batch02.csv'), registry, False)
    silver_v0 = load_jsonl('silver_baseline.jsonl')
    silver_v1 = load_jsonl('silver_after_batch02.jsonl')
    silver_v2 = load_jsonl('silver_replay.jsonl')
    gold_v0 = read_json(ev / 'gold_baseline.json')
    gold_v1 = read_json(ev / 'gold_after_batch02.json')
    live = read_json(ev / 'live_table_verification.json')
    gold_schema = read_json(ROOT / 'schemas/gold_schema.json')
    silver_schema = read_json(ROOT / 'schemas/silver_schema.json')

    # ================================================================ PART 1: own work B01-B04
    model_out = 'role_B_gold_model.json'
    tables = live['tables']
    check('B01_three_delta_tables_and_versions',
          all(tables[t]['latest_detail']['format'] == 'delta' for t in ('bronze', 'silver', 'gold'))
          and tables['silver']['pinned_version'] == state['silver_final_version']
          and tables['gold']['pinned_version'] == state['gold_final_version']
          and live['source_silver_version_for_gold'] == state['gold_source_silver_version']
          == state['silver_final_version'],
          {t: {'path': tables[t]['path'], 'pinned_version': tables[t]['pinned_version'],
               'rows_at_pinned_version': tables[t]['rows_at_pinned_version'],
               'history': [(h['version'], h['operation']) for h in tables[t]['history']]}
           for t in ('bronze', 'silver', 'gold')} | {'gold_source_silver_version': state['gold_source_silver_version']},
          model_out)

    def spark_types(table):
        return [(f['name'], f['type']) for f in tables[table]['schema']['fields']]

    expected_gold_types = [(c['name'], SQL_TO_SPARK[c['type']]) for c in gold_schema['columns']]
    expected_silver_types = [(c['name'], SQL_TO_SPARK[c['type']]) for c in silver_schema['columns']]
    check('B01_exact_schema_types',
          spark_types('gold') == expected_gold_types and spark_types('silver') == expected_silver_types
          and [c['name'] for c in gold_schema['columns']] == GOLD_FIELDS
          and all(list(row) == GOLD_FIELDS for row in gold_v1),
          {'gold': spark_types('gold'), 'silver': spark_types('silver')}, model_out)

    # The reviewed build_gold must be the code that actually produced Gold v1. A Windows
    # checkout may convert LF to CRLF, so compare the LF-normalized bytes with the
    # recorded executed hash and the archived code_versions copy.
    impl_bytes = IMPL.read_bytes()
    impl_lf_sha = hashlib.sha256(impl_bytes.replace(b'\r\n', b'\n')).hexdigest()
    archived = ev / 'code_versions' / (state['final_implementation_sha256'] + '.py')
    archived_lf_sha = hashlib.sha256(archived.read_bytes().replace(b'\r\n', b'\n')).hexdigest()
    check('B01_executed_build_gold_code_binding',
          impl_lf_sha == archived_lf_sha == state['final_implementation_sha256']
          == state['initial_implementation_sha256'],
          {'executed_implementation_sha256': state['final_implementation_sha256'],
           'working_copy_raw_sha256': hashlib.sha256(impl_bytes).hexdigest(),
           'working_copy_lf_normalized_sha256': impl_lf_sha,
           'crlf_lines_in_working_copy': impl_bytes.count(b'\r\n'),
           'note': 'Raw hash differs only by CRLF line endings introduced by checkout; '
                   'LF-normalized bytes equal the executed implementation.'}, model_out)

    # B02: Silver baseline is logically the handoff; event_date is the UTC calendar date.
    silver_dates = {r['event_time_utc'][:10] for r in silver_v1}
    gold_dates = {g['event_date'] for g in gold_v1}
    utc7 = timezone(timedelta(hours=7))
    shifted = [r for r in silver_v1
               if datetime.strptime(r['event_time_utc'], '%Y-%m-%dT%H:%M:%SZ')
               .replace(tzinfo=timezone.utc).astimezone(utc7).date().isoformat() != r['event_time_utc'][:10]]
    gold_if_utc7 = aggregate([dict(r, event_time_utc=datetime.strptime(r['event_time_utc'], '%Y-%m-%dT%H:%M:%SZ')
                                   .replace(tzinfo=timezone.utc).astimezone(utc7).strftime('%Y-%m-%dT%H:%M:%SZ'))
                              for r in silver_v1])
    utc_index = {(g['site'], g['event_date']): g for g in gold_v1}
    utc7_changed_groups = sum(1 for g in gold_if_utc7 if utc_index.get((g['site'], g['event_date'])) != g)
    live_checks = {c['check_id']: c['passed'] for c in live['checks']}
    check('B02_silver_equals_handoff_and_utc_event_date',
          logical_sha256(silver_v0) == logical_sha256(baseline) == state['handoff_logical_sha256']
          and len(silver_v0) == len(baseline) == 9610
          and len({r['record_id'] for r in silver_v1}) == len(silver_v1)
          and silver_dates == gold_dates
          and live_checks['LIVE_Silver_key_and_UTC_date'] and live_checks['LIVE_initial_handoff']
          and sum(r['is_late'] for r in silver_v0) == sum(r['is_late'] for r in baseline)
          and utc7_changed_groups > 0,
          {'handoff_logical_sha256': state['handoff_logical_sha256'],
           'silver_v0_rows': len(silver_v0), 'late_rows_retained_v0': sum(r['is_late'] for r in silver_v0),
           'utc_event_dates': sorted(silver_dates),
           'rows_whose_date_would_change_under_UTC+7': len(shifted),
           'gold_groups_if_wrongly_computed_in_UTC+7': len(gold_if_utc7),
           'gold_groups_whose_values_would_differ_in_UTC+7': utc7_changed_groups,
           'gold_dates_if_UTC+7': sorted({g['event_date'] for g in gold_if_utc7}),
           'gold_groups_utc': len(gold_v1)}, model_out)

    # B03: conservation of additive components for both Gold snapshots.
    conservation = {}
    for label, silver, gold in (('v0', silver_v0, gold_v0), ('v1', silver_v1, gold_v1)):
        conservation[label] = {
            'silver_rows': len(silver), 'gold_n_observations': sum(g['n_observations'] for g in gold),
            'silver_temp_sum': fixed(sum(Decimal(r['temperature_c']) for r in silver), 2),
            'gold_temp_sum': fixed(sum(Decimal(g['sum_temperature_c']) for g in gold), 2),
            'silver_late': sum(r['is_late'] for r in silver), 'gold_n_late': sum(g['n_late'] for g in gold),
            'silver_above_25': sum(Decimal(r['temperature_c']) > 25 for r in silver),
            'gold_n_above_25': sum(g['n_above_25'] for g in gold),
            'groups': len(gold), 'gold_equals_independent_recompute': gold == aggregate(silver)}
    check('B03_conservation_counts_and_sums',
          all(c['silver_rows'] == c['gold_n_observations'] and c['silver_temp_sum'] == c['gold_temp_sum']
              and c['silver_late'] == c['gold_n_late'] and c['silver_above_25'] == c['gold_n_above_25']
              and c['gold_equals_independent_recompute'] for c in conservation.values()),
          conservation, model_out)

    # Every Gold field re-derived from its own additive components (internal consistency).
    internal = all(
        g['mean_temperature_c'] == fixed(Decimal(g['sum_temperature_c']) / g['n_observations'])
        and g['late_rate'] == fixed(Decimal(g['n_late']) / g['n_observations'])
        and Decimal(g['min_temperature_c']) <= Decimal(g['mean_temperature_c']) <= Decimal(g['max_temperature_c'])
        and 1 <= g['n_sensors'] <= g['n_observations'] and g['n_above_25'] <= g['n_observations']
        for g in gold_v0 + gold_v1)
    static = build_gold_static_audit()
    check('B03_gold_internal_formulas_and_static_audit',
          internal and not static['forbidden_calls_found'] and static['groupBy_keys_site_event_date']
          and static['strict_threshold_gt_25'] and static['count_distinct_sensor'],
          {'mean_equals_sum_over_count_all_groups': internal, 'static_audit': static}, model_out)

    # B04 / demonstration: weighted daily mean from Gold components only vs naive mean of means.
    by_day = defaultdict(list)
    for g in gold_v1:
        by_day[g['event_date']].append(g)
    daily = []
    for day, groups in sorted(by_day.items()):
        total = sum(Decimal(g['sum_temperature_c']) for g in groups)
        count = sum(g['n_observations'] for g in groups)
        naive = sum(Decimal(g['mean_temperature_c']) for g in groups) / len(groups)
        daily.append({'event_date': day, 'site_counts': {g['site']: g['n_observations'] for g in groups},
                      'sum_temperature_c': fixed(total, 2), 'n_observations': count,
                      'weighted_mean_c': fixed(total / count), 'naive_mean_of_means_c': fixed(naive),
                      'difference_c': fixed(Decimal(fixed(naive)) - Decimal(fixed(total / count))),
                      'equal_site_counts': len({g['n_observations'] for g in groups}) == 1})
    q2 = {r['event_date']: r for r in read_json(ev / 'Q2_weighted_day.json')['rows']}
    all_total = sum(Decimal(g['sum_temperature_c']) for g in gold_v1)
    all_count = sum(g['n_observations'] for g in gold_v1)
    check('B04_weighted_vs_naive_from_gold_components',
          all(q2[d['event_date']]['weighted_mean_c'] == d['weighted_mean_c']
              and q2[d['event_date']]['naive_mean_of_means_c'] == d['naive_mean_of_means_c']
              and q2[d['event_date']]['n_observations'] == d['n_observations'] for d in daily)
          and any(d['difference_c'] != '0.000000' for d in daily)
          and all(d['difference_c'] == '0.000000' for d in daily if d['equal_site_counts']),
          {'daily': daily,
           'whole_period_weighted_c': fixed(all_total / all_count),
           'whole_period_avg_of_20_site_day_means_c':
               fixed(sum(Decimal(g['mean_temperature_c']) for g in gold_v1) / len(gold_v1))}, model_out)

    model_checks = list(checks)
    model = {'passed': True, 'member_id': member['student_id'], 'member_name': member['name'], 'role': 'B',
             'execution_mode': 'AUTOMATED_READ_ONLY', 'completed_at_utc': datetime.now(timezone.utc).isoformat(),
             'code_sha256': sha256_file(__file__), 'implementation_sha256': impl_lf_sha,
             'silver_version': state['silver_final_version'], 'gold_version': state['gold_final_version'],
             'gold_source_silver_version': state['gold_source_silver_version'],
             'checks': model_checks, 'daily_weighted_vs_naive': daily}
    write_json(ev / model_out, model)
    own_checks = len(model_checks)
    checks.clear()

    # ================================================================ PART 2: cross-check Role C
    out = 'role_B_check.json'
    base_by_id = {r['record_id']: r for r in baseline}
    v1_by_id = {r['record_id']: r for r in silver_v1}
    v2_by_id = {r['record_id']: r for r in silver_v2}
    groups = defaultdict(list)
    for row in incoming:
        groups[row['record_id']].append(row)
    v1_identities = {identity(r) for r in silver_v1}

    stale_rows = {}
    for key in STALE_KEYS:
        candidates = groups[key]
        current = base_by_id[key]
        stale_rows[key] = {
            'current_silver_v0': current, 'incoming_candidates': candidates, 'silver_v1': v1_by_id[key],
            'all_incoming_older': all(c['ingest_time_utc'] < current['ingest_time_utc'] for c in candidates),
            'v1_equals_baseline': v1_by_id[key] == current,
            'incoming_identity_absent_from_silver': all(identity(c) not in v1_identities for c in candidates)}
    bronze_hist = {h['version']: h for h in tables['bronze']['history']}
    check('B_C01_stale_revisions_never_overwrite',
          all(s['all_incoming_older'] and s['v1_equals_baseline'] and s['incoming_identity_absent_from_silver']
              for s in stale_rows.values())
          and tables['bronze']['rows_at_pinned_version'] == len(baseline) + len(incoming) == 9750
          and bronze_hist[1]['operationMetrics']['numTargetRowsInserted'] == str(len(incoming)),
          {'stale_keys': STALE_KEYS, 'example_R003005': stale_rows['R003005'],
           'bronze_keeps_stale_receipts': {'bronze_rows': tables['bronze']['rows_at_pinned_version'],
                                           'inserted_by_batch02': bronze_hist[1]['operationMetrics']['numTargetRowsInserted']}},
          out)

    tied = {}
    for key in TIED_EXISTING_KEYS + TIED_NEW_KEYS:
        candidates = groups[key]
        same_time = len({c['ingest_time_utc'] for c in candidates}) == 1
        same_object = len({c['source_object'] for c in candidates}) == 1
        expected = min(candidates, key=lambda c: int(c['source_row']))
        loser = max(candidates, key=lambda c: int(c['source_row']))
        previous = base_by_id.get(key)
        tied[key] = {
            'candidates': [(int(c['source_row']), c['temperature_c']) for c in candidates],
            'same_ingest_and_object': same_time and same_object,
            'expected_winner_row': int(expected['source_row']), 'expected_winner_temp': expected['temperature_c'],
            'superseded_row': int(loser['source_row']),
            'previous_silver': None if previous is None else (previous['source_object'], previous['source_row'],
                                                              previous['temperature_c']),
            'decision': 'inserted' if previous is None else ('updated' if precedes(expected, previous) else 'unchanged'),
            'silver_v1': (v1_by_id[key]['source_object'], v1_by_id[key]['source_row'], v1_by_id[key]['temperature_c']),
            'ok': (same_time and same_object and v1_by_id[key] == expected
                   and identity(loser) not in v1_identities)}
    check('B_C02_same_time_ties_smaller_source_row_wins',
          all(t['ok'] for t in tied.values())
          and all(tied[k]['decision'] == 'updated' for k in TIED_EXISTING_KEYS)
          and all(tied[k]['decision'] == 'inserted' for k in TIED_NEW_KEYS),
          {'tied_keys': tied}, out)

    reference = {key: pick_winner(rows) for key, rows in groups.items()}
    order_ok = True
    for key in TIED_EXISTING_KEYS + TIED_NEW_KEYS:
        a, b = groups[key]
        order_ok &= pick_winner([a, b]) == pick_winner([b, a]) == v1_by_id[key]
    rng = random.Random(24022277)
    shuffles = 0
    for _ in range(200):
        rows = list(incoming)
        rng.shuffle(rows)
        regrouped = defaultdict(list)
        for row in rows:
            regrouped[row['record_id']].append(row)
        order_ok &= {k: pick_winner(v) for k, v in regrouped.items()} == reference
        shuffles += 1
    reversed_groups = defaultdict(list)
    for row in reversed(incoming):
        reversed_groups[row['record_id']].append(row)
    last_row_wins = {row['record_id']: row for row in incoming}
    naive_wrong = sorted(k for k in groups if last_row_wins[k] != reference[k])
    check('B_C03_order_invariance_independent_comparator',
          order_ok and {k: pick_winner(v) for k, v in reversed_groups.items()} == reference
          and len(reference) == 130 and naive_wrong == sorted(TIED_EXISTING_KEYS + TIED_NEW_KEYS),
          {'winners': len(reference), 'random_shuffles_checked': shuffles, 'pairwise_both_orders': 10,
           'naive_last_row_policy_wrong_on': naive_wrong}, out)

    # Execute the ACTUAL merge_silver predicate text against a truth table (covers the
    # incoming-vs-current tie branch that the real batch does not contain).
    predicate = merge_predicate_sql()
    t0 = {'ingest_time_utc': '2026-02-06 00:30:00', 'source_object': 'b.csv', 'source_row': 5}
    cases = [
        ('newer_ingest_even_with_larger_object_and_row', dict(t0, ingest_time_utc='2026-02-06 00:31:00', source_object='z.csv', source_row=99), True),
        ('stale_ingest_even_with_smaller_object_and_row', dict(t0, ingest_time_utc='2026-02-06 00:29:00', source_object='a.csv', source_row=1), False),
        ('tie_time_smaller_object_larger_row', dict(t0, source_object='a.csv', source_row=99), True),
        ('tie_time_larger_object_smaller_row', dict(t0, source_object='c.csv', source_row=1), False),
        ('tie_time_object_smaller_row', dict(t0, source_row=4), True),
        ('tie_time_object_larger_row', dict(t0, source_row=6), False),
        ('equal_identity_noop', dict(t0), False),
        ('lexicographic_object_not_numeric', dict(t0, source_object='b.csv0', source_row=1), False),
    ]
    db = sqlite3.connect(':memory:')
    db.execute('CREATE TABLE t(ingest_time_utc TEXT, source_object TEXT, source_row INTEGER)')
    db.execute('CREATE TABLE s(ingest_time_utc TEXT, source_object TEXT, source_row INTEGER)')
    db.execute('INSERT INTO t VALUES (?,?,?)', (t0['ingest_time_utc'], t0['source_object'], t0['source_row']))
    truth = []
    for name, incoming_row, expected_update in cases:
        db.execute('DELETE FROM s')
        db.execute('INSERT INTO s VALUES (?,?,?)',
                   (incoming_row['ingest_time_utc'], incoming_row['source_object'], incoming_row['source_row']))
        actual_update = bool(db.execute(f'SELECT ({predicate}) FROM s, t').fetchone()[0])
        python_model = precedes(incoming_row, t0)
        truth.append({'case': name, 'expected_update': expected_update,
                      'merge_silver_sql_update': actual_update, 'independent_model_update': python_model})
    check('B_C04_actual_merge_predicate_truth_table',
          all(c['expected_update'] == c['merge_silver_sql_update'] == c['independent_model_update'] for c in truth)
          and 'whenMatchedUpdateAll(condition=precedes)' in IMPL.read_text(encoding='utf-8'),
          {'predicate_sql': ' '.join(predicate.split()), 'engine': 'sqlite3 (predicate text executed verbatim)',
           'note': 'changes_batch02 has 0 incoming-vs-current equal-ingest cases; this table covers that branch.',
           'cases': truth}, out)

    replay = {}
    for key in STALE_KEYS + TIED_EXISTING_KEYS + TIED_NEW_KEYS:
        winner = reference[key]
        current = v1_by_id[key]
        second = ('unchanged_equal' if compare(winner, current) == 0
                  else 'updated' if precedes(winner, current) else 'unchanged_stale')
        replay[key] = {'second_application_decision': second, 'v2_equals_v1': v2_by_id[key] == current}
    check('B_C05_replay_keeps_stale_and_tied_results',
          all(replay[k]['second_application_decision'] == 'unchanged_stale' for k in STALE_KEYS)
          and all(replay[k]['second_application_decision'] == 'unchanged_equal'
                  for k in TIED_EXISTING_KEYS + TIED_NEW_KEYS)
          and all(r['v2_equals_v1'] for r in replay.values())
          and logical_sha256(silver_v1) == logical_sha256(silver_v2) == read_json(ev / 'incremental.json')['logical_sha256_replay']
          and state['silver_replay_version'] > state['silver_final_version'] == state['gold_source_silver_version'],
          {'per_key': replay, 'silver_v1_v2_logical_sha256': logical_sha256(silver_v1),
           'gold_still_bound_to_silver_version': state['gold_source_silver_version'],
           'replay_version': state['silver_replay_version']}, out)

    # Gold sensitivity: wrong revision policies would change concrete Gold fields.
    def gold_index(rows):
        return {(g['site'], g['event_date']): g for g in rows}

    def differing(alternative_silver):
        alt, act = gold_index(aggregate(alternative_silver)), gold_index(gold_v1)
        return {f'{k[0]}/{k[1]}': {f: (act[k][f], alt[k][f]) for f in GOLD_FIELDS[2:] if act[k][f] != alt[k][f]}
                for k in act if act[k] != alt.get(k)}

    stale_overwrite = deepcopy(v1_by_id)
    for key in STALE_KEYS:
        stale_overwrite[key] = pick_winner(groups[key])
    last_tie = deepcopy(v1_by_id)
    for key in TIED_EXISTING_KEYS + TIED_NEW_KEYS:
        last_tie[key] = last_row_wins[key]
    diff_stale = differing(list(stale_overwrite.values()))
    diff_tie = differing(list(last_tie.values()))
    act = gold_index(gold_v1)
    check('B_C06_gold_reflects_correct_stale_and_tie_decisions',
          gold_v1 == aggregate(silver_v1)
          and {k: v['sum_temperature_c'] for k, v in diff_stale.items()}
          == {'zone-1/2026-02-03': ('16046.00', '16041.00'), 'zone-2/2026-02-03': ('16406.00', '16401.00')}
          and {k: v['sum_temperature_c'] for k, v in diff_tie.items()}
          == {'zone-1/2026-02-02': ('13972.50', '13977.50'), 'zone-1/2026-02-06': ('532.50', '537.50')}
          and not any('n_observations' in v for v in (*diff_stale.values(), *diff_tie.values()))
          and act['zone-1', '2026-02-03']['sum_temperature_c'] == '16046.00'
          and act['zone-1', '2026-02-02']['sum_temperature_c'] == '13972.50',
          {'if_stale_rows_overwrote_silver': diff_stale, 'if_last_tied_row_won': diff_tie}, out)

    decisions_doc = {r['record_id']: r['decision'] for r in read_json(ev / 'revision_decisions.json')['rows']}
    check('B_C07_role_C_decision_record_consistent',
          all(decisions_doc[k] == 'unchanged_stale' for k in STALE_KEYS)
          and all(decisions_doc[k] == 'updated_newer' for k in TIED_EXISTING_KEYS)
          and all(decisions_doc[k] == 'inserted' for k in TIED_NEW_KEYS)
          and 'updated_tied' not in set(decisions_doc.values()),
          {'stale': 'unchanged_stale x10', 'tied_existing': 'updated_newer x5', 'tied_new': 'inserted x5',
           'updated_tied_count': 0,
           'interpretation': 'Ties in this batch occur among incoming rows only; no incoming winner ties the '
                             'current Silver ingest time, so no updated_tied decision is expected.'}, out)

    cross_checks = list(checks)
    result = {'passed': True, 'member_id': member['student_id'], 'member_name': member['name'], 'role': 'B',
              'reviewed_role': 'C', 'execution_mode': 'AUTOMATED_READ_ONLY',
              'completed_at_utc': datetime.now(timezone.utc).isoformat(),
              'code_sha256': sha256_file(__file__), 'implementation_sha256': impl_lf_sha,
              'silver_initial_version': state['silver_initial_version'],
              'silver_final_version': state['silver_final_version'],
              'silver_replay_version': state['silver_replay_version'],
              'gold_final_version': state['gold_final_version'],
              'gold_source_silver_version': state['gold_source_silver_version'],
              'writes_performed': 0,
              'daily_weighted_vs_naive': daily,
              'stale_revisions': stale_rows,
              'tied_revisions': tied,
              'predicate_truth_table': truth,
              'gold_sensitivity': {'if_stale_rows_overwrote_silver': diff_stale,
                                   'if_last_tied_row_won': diff_tie},
              'model_checks': model_checks,
              'cross_checks': cross_checks,
              'checks': model_checks + cross_checks}
    write_json(ev / out, result)
    (ev / 'code_versions' / (result['code_sha256'] + '.py')).write_bytes(Path(__file__).read_bytes())
    print(f'PASS: {own_checks} role B model checks (B01-B04) and {len(cross_checks)} cross-checks of role C '
          f'({len(result["checks"])} total).')


if __name__ == '__main__':
    main()
