"""Independent bounded checks of executed exports; never writes lakehouse tables."""
import argparse
from collections import Counter, defaultdict
import csv
from decimal import Decimal, ROUND_HALF_UP
import json
from pathlib import Path
import statistics
import sys
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from common import (read_json, write_json, load_csv, load_registry, validate_rows,
                    logical_sha256, sha256_file, verify_inputs)


def rounded(value, places=6):
    return f'{Decimal(value).quantize(Decimal(10) ** -places, rounding=ROUND_HALF_UP):.{places}f}'


def order(row):
    timestamp = int(datetime.fromisoformat(row['ingest_time_utc'].replace('Z', '+00:00')).timestamp())
    return -timestamp, row['source_object'], row['source_row']


def aggregate(rows):
    groups = defaultdict(list)
    for row in rows:
        groups[row['site'], row['event_time_utc'][:10]].append(row)
    answer = []
    for (site, day), values in sorted(groups.items()):
        temperatures = [Decimal(row['temperature_c']) for row in values]
        count = len(values)
        total = sum(temperatures)
        late = sum(row['is_late'] for row in values)
        answer.append(dict(site=site, event_date=day, n_observations=count,
            sum_temperature_c=rounded(total, 2), mean_temperature_c=rounded(total / count),
            min_temperature_c=rounded(min(temperatures), 2), max_temperature_c=rounded(max(temperatures), 2),
            n_late=late, late_rate=rounded(Decimal(late) / count),
            n_above_25=sum(t > 25 for t in temperatures),
            n_sensors=len({row['sensor_id'] for row in values})))
    return answer


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', default='config/team.json')
    parser.add_argument('--trusted-manifest', required=True)
    args = parser.parse_args()
    config = read_json(args.config)
    ev = Path(config['evidence_dir'])
    checks = []

    def check(name, passed, detail=None):
        checks.append(dict(check_id=name, passed=bool(passed), detail=detail))
        if not passed:
            write_json(ev / 'independent_checks.json', dict(passed=False, checks=checks))
            raise ValueError('VERIFICATION_FAILED:' + name)

    integrity = verify_inputs(ROOT, args.trusted_manifest)
    check('H01_trusted_input_bytes', len(integrity['checks']) == 16)
    registry = load_registry(ROOT / 'data/handoff/sensors.csv')
    baseline = validate_rows(load_csv(ROOT / 'data/handoff/curated.csv'), registry, True)
    incoming = validate_rows(load_csv(ROOT / 'data/changes/changes_batch02.csv'), registry, False)
    state = read_json(ev / 'state.json')

    def exported(name):
        return [json.loads(line) for line in (ev / name).read_text(encoding='utf-8').splitlines() if line]

    actual_baseline = exported('silver_baseline.jsonl')
    actual = exported('silver_after_batch02.jsonl')
    replay = exported('silver_replay.jsonl')
    historical = exported('silver_time_travel.jsonl')
    check('H02_baseline_logical_identity', logical_sha256(baseline) == logical_sha256(actual_baseline)
          == state['handoff_logical_sha256'])
    check('H03_schema_rejection', read_json(ev / 'delta_schema_probe.json')['passed'])
    worker = read_json(ev / 'worker_probe.json')
    check('H04_two_worker_placements', worker['distinct_hosts'] == 2,
          {'hosts': sorted({r['host'] for r in worker['partition_tasks']}),
           'limit': 'Worker container placements; no claim about separate physical servers.'})

    incoming_groups = defaultdict(list)
    for row in incoming:
        incoming_groups[row['record_id']].append(row)
    winners = {key: min(rows, key=order) for key, rows in incoming_groups.items()}
    expected = {row['record_id']: row for row in baseline}
    counts = Counter()
    traces = []
    for key, winner in sorted(winners.items()):
        previous = expected.get(key)
        if previous is None:
            decision = 'inserted'
        elif order(winner) < order(previous):
            decision = 'updated_newer' if winner['ingest_time_utc'] > previous['ingest_time_utc'] else 'updated_tied'
        else:
            decision = 'unchanged_equal' if order(winner) == order(previous) else 'unchanged_stale'
        counts[decision] += 1
        if decision.startswith(('inserted', 'updated')):
            expected[key] = winner
        traces.append(dict(record_id=key, decision=decision, incoming_physical_rows=len(incoming_groups[key]),
                           incoming_candidates=sorted(incoming_groups[key],key=order),
                           previous=previous, incoming_winner=winner, selected=expected[key]))
    inc = read_json(ev / 'incremental.json')
    check('M01_incoming_winners', len(winners) == inc['incoming_winners'],
          dict(physical_rows=len(incoming), winners=len(winners), superseded=len(incoming)-len(winners)))
    check('M02_all_revision_decisions', {r['record_id']: r for r in actual} == expected, dict(counts))
    check('M03_reordering', inc['winner_reordering_passed'])
    check('M04_receipt_replay', inc['bronze_before_replay'] == inc['bronze_after_replay']
          == len(baseline) + len(incoming)
          and inc['bronze_receipt_sha256_before_replay'] == inc['bronze_receipt_sha256_after_replay'])
    check('M05_full_silver_replay', logical_sha256(actual) == logical_sha256(replay)
          == inc['logical_sha256_after'] == inc['logical_sha256_replay'])
    check('M06_historical_snapshot', logical_sha256(historical) == logical_sha256(baseline))
    mutations = read_json(ev / 'input_contract_tests.json')
    check('M07_invalid_batches', len(mutations) == 3 and all(r['passed'] and r['writes_performed'] == 0 for r in mutations))

    for label, rows in (('baseline', actual_baseline), ('after_batch02', actual)):
        gold = read_json(ev / f'gold_{label}.json')
        check('B03_' + label + '_conservation', sum(r['n_observations'] for r in gold) == len(rows)
              and sum(Decimal(r['sum_temperature_c']) for r in gold) == sum(Decimal(r['temperature_c']) for r in rows))
        check('M08_' + label + '_all_gold_values', gold == aggregate(rows), {'groups': len(gold)})

    gold = aggregate(actual)
    q1 = sorted([{k:r[k] for k in ('site','event_date','n_observations','mean_temperature_c','n_late','late_rate')}
                 for r in gold], key=lambda r:(r['event_date'],r['site']))
    q3 = [dict(site=r['site'], event_date=r['event_date'], n_observations=r['n_observations'],
               n_above_25=r['n_above_25'], above25_rate=rounded(Decimal(r['n_above_25'])/r['n_observations'])) for r in gold]
    q3.sort(key=lambda r:(r['event_date'], -Decimal(r['above25_rate']), r['site']))
    by_day = defaultdict(list)
    for row in gold:
        by_day[row['event_date']].append(row)
    q2 = []
    for day, rows in sorted(by_day.items()):
        n = sum(r['n_observations'] for r in rows)
        q2.append(dict(event_date=day, n_observations=n,
            weighted_mean_c=rounded(sum(Decimal(r['sum_temperature_c']) for r in rows)/n),
            naive_mean_of_means_c=rounded(sum(Decimal(r['mean_temperature_c']) for r in rows)/len(rows))))
    for name, expected_rows in (('Q1_site_day',q1),('Q2_weighted_day',q2),('Q3_above25_rate',q3)):
        output = read_json(ev / (name+'.json'))
        check(name, output['rows'] == expected_rows and output['gold_version'] == state['gold_final_version']
              and output['source_silver_version'] == state['silver_final_version'])

    bench = read_json(ev / 'benchmark_report.json')
    check('Q04_six_actual_timings', len(bench['measurements']) == 6
          and [r['layout'] for r in bench['measurements']] == list('ABBAAB')
          and all(r['elapsed_seconds'] > 0 and r['result_equal'] for r in bench['measurements']))
    check('Q05_benchmark_snapshot_and_size', bench['source_silver_version'] == state['silver_final_version']
          and bench['benchmark_copies'] == 100 and bench['source_rows'] == len(actual)
          and bench['expanded_rows'] == len(actual)*100)
    benchmark_expected = [dict(site=r['site'], n_observations=r['n_observations']*100,
        sum_temperature_c=rounded(Decimal(r['sum_temperature_c'])*100,2), n_late=r['n_late']*100)
        for r in gold if r['event_date']=='2026-02-06']
    check('Q06_equivalent_benchmark_values', benchmark_expected == bench['query_result'])
    check('Q07_recorded_medians', all(bench['median_seconds'][label] ==
          statistics.median(r['elapsed_seconds'] for r in bench['measurements'] if r['layout']==label)
          for label in ('A','B')))
    plan_b = (ev/'plan_B.txt').read_text(encoding='utf-8')
    check('Q08_date_partition_pruning', 'PartitionFilters:' in plan_b and '2026-02-06' in plan_b)

    gate = read_json(ev/'release_gate_tests.json')
    check('R01_R05_five_release_gates', len(gate)==5 and all(r['test_passed'] for r in gate)
          and [r['observed_pass'] for r in gate] == [True,False,False,False,False])
    candidate = ev/'release_candidate'
    binding = read_json(candidate/'snapshot_binding.json')
    manifest = read_json(candidate/'release_manifest.json')
    check('R06_binding', binding['silver_version'] == binding['gold_source_silver_version'] == state['silver_final_version']
          and binding['gold_version'] == state['gold_final_version']
          and binding['code_sha256'] == state['final_implementation_sha256'])
    check('R07_executed_code_archive', sha256_file(ev/'code_versions'/(binding['code_sha256']+'.py')) == binding['code_sha256'])
    check('R08_candidate_object_hashes', all((candidate/r['key']).stat().st_size == r['bytes']
          and sha256_file(candidate/r['key']) == r['sha256'] for r in manifest['objects']))
    check('R09_candidate_gold', read_json(candidate/'gold.json') == gold
          and read_json(ev/'release_check.json')['snapshot_recompute_equal'])

    from jsonschema import Draft202012Validator, FormatChecker
    schema_path = ROOT/'schemas/openlineage/OpenLineage-2-0-2.json'
    official = read_json(schema_path)
    validator = Draft202012Validator(dict(official, **{'$ref':'#/$defs/RunEvent'}), format_checker=FormatChecker())
    events = exported('lineage.jsonl')
    pairs = defaultdict(list)
    for event in events:
        validator.validate(event)
        pairs[event['run']['runId']].append(event)
    check('L01_official_openlineage_schema', True,
          {'source':'https://openlineage.io/spec/2-0-2/OpenLineage.json','sha256':sha256_file(schema_path),'events':len(events)})
    check('L02_paired_run_identity', all(len(pair)==2 and pair[0]['eventType']=='START'
          and pair[1]['eventType'] in ('COMPLETE','FAIL','ABORT') and pair[0]['job']==pair[1]['job']
          and pair[0]['run']==pair[1]['run'] and pair[0]['eventTime'] <= pair[1]['eventTime'] for pair in pairs.values()))
    check('L03_six_completed_tasks', {'preflight','bootstrap','incremental','analytics','benchmark','release'} <=
          {event['job']['name'] for event in events if event['eventType']=='COMPLETE'})

    summary = dict(baseline_rows=len(baseline), incoming_physical_rows=len(incoming), incoming_winners=len(winners),
        superseded_incoming_rows=len(incoming)-len(winners), decisions=dict(counts), silver_rows=len(actual),
        gold_groups=len(gold), temperature_sum_c=rounded(sum(Decimal(r['temperature_c']) for r in actual),2),
        n_late=sum(r['is_late'] for r in actual), n_above_25=sum(Decimal(r['temperature_c'])>25 for r in actual))
    write_json(ev/'revision_decisions.json', {'summary':summary,'rows':traces})
    examples = []
    with (ROOT/'data/changes/source_batch02.csv').open(encoding='utf-8',newline='') as handle:
        source_records=list(csv.DictReader(handle))
    check('TRACE_all_batch_source_ordinals',all(
        source_records[row['source_row']-1]['record_id'].strip().upper()==row['record_id']
        and source_records[row['source_row']-1]['sensor_id']==row['sensor_id']
        and source_records[row['source_row']-1]['event_time']==row['event_time_utc']
        and source_records[row['source_row']-1]['ingest_time']==row['ingest_time_utc']
        and source_records[row['source_row']-1]['unit']=='C'
        and Decimal(source_records[row['source_row']-1]['reading'])==Decimal(row['temperature_c'])
        for row in incoming))
    for decision in sorted(counts):
        example = next(r for r in traces if r['decision']==decision)
        source = example['incoming_winner']
        with (ROOT/'data/changes/source_batch02.csv').open(encoding='utf-8',newline='') as handle:
            source_record = list(csv.DictReader(handle))[source['source_row']-1]
        check('TRACE_'+decision, sha256_file(ROOT/'data/changes/source_batch02.csv') == source['source_sha256']
              and source_record['record_id'].strip().upper() == source['record_id'])
        examples.append(dict(example, original_source_projection=source_record))
    write_json(ev/'row_trace.json', {'ordinal_rule':'One-based physical CSV record ordinal excluding header.', 'examples':examples})
    write_json(ev/'independent_checks.json', {'passed':True,'checks':checks,'summary':summary,
        'method':'Independent standard-library revision model and exact Decimal aggregates compared with actual Spark exports.',
        'scope':'Technical checks only; individual hands-on review and exit responses require the assigned members.'})
    print(f'PASS: {len(checks)} independent evidence checks. Silver={len(actual)}, Gold={len(gold)}.')


if __name__ == '__main__':
    main()
