"""Render role D in the same assessment structure as the retained role A report."""
import ast
import os
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from common import read_json


def table(headers, rows):
    return '| ' + ' | '.join(headers) + ' |\n| ' + ' | '.join('---' for _ in headers) + ' |\n' + ''.join(
        '| ' + ' | '.join(str(v).replace('|', '/') for v in row) + ' |\n' for row in rows)


def build_role_d_record(root, reports, ev):
    result = read_json(ev / 'role_D_check.json')
    prediction = read_json(ev / 'role_D_prediction.json')
    if not result['passed'] or prediction['recorded_at_utc'] > result['completed_at_utc']:
        raise ValueError('VERIFIED_ROLE_D_RECORD_REQUIRED')
    state = read_json(ev / 'state.json')
    runtime = read_json(ev / 'runtime_benchmark.json')
    bench = read_json(ev / 'benchmark_report.json')
    scan = read_json(ev / 'benchmark_scan_metrics.json')
    summary = read_json(ev / 'independent_checks.json')['summary']
    binding = read_json(ev / 'release_candidate/snapshot_binding.json')
    tests = read_json(ev / 'implementation_tests.json')
    queries = {name: read_json(ev / (name + '.json')) for name in ('Q1_site_day', 'Q2_weighted_day', 'Q3_above25_rate')}
    source = (root / 'templates/student_impl.py').read_text(encoding='utf-8')
    node = next(node for node in ast.parse(source).body if isinstance(node, ast.FunctionDef) and node.name == 'benchmark_query')
    timings = []
    for index, measurement in enumerate(bench['measurements'], 1):
        execution = next(e for e in scan['executions'] if e.get('measurement_number') == index)
        files = [m['value'] for node in execution['scan_nodes'] for m in node['metrics'] if m['name'] == 'number of files read']
        if len(files) != 1 or not measurement['result_equal']:
            raise ValueError('UNVERIFIED_BENCHMARK_MEASUREMENT')
        timings.append([index, measurement['round'], measurement['layout'], f"{measurement['elapsed_seconds']:.9f}",
                        execution['sql_execution_id'], files[0], 'PASS'])
    if len(timings) != 6 or [row[2] for row in timings] != list('ABBAAB'):
        raise ValueError('SIX_ORDERED_MEASUREMENTS_REQUIRED')
    denominator_rows = [[r[key] for key in ('event_date', 'sum_temperature_c', 'n_observations',
                          'weighted_mean_c', 'naive_mean_of_means_c')] for r in result['daily_denominators']]
    feb02 = next(c['observed']['site_counts'] for c in result['checks'] if c['check_id'] == 'D_B_weighted_2026-02-02')
    markers = {
        'GROUP_ID': state['group_id'], 'RUN_LABEL': state['run_label'],
        'SPARK': runtime['spark'], 'DELTA': runtime['delta_spark'], 'PYTHON': runtime['python'],
        'JAVA': runtime['java'], 'HADOOP': runtime['hadoop'], 'AQE': runtime['aqe'], 'APP_ID': runtime['application_id'],
        'TRUSTED_SHA': state['trusted_manifest_sha256'], 'HANDOFF_LOGICAL_SHA': state['handoff_logical_sha256'],
        'BENCHMARK_RUN_ID': read_json(ev / 'run_benchmark.json')['run_id'],
        'SILVER_VERSION': state['silver_final_version'], 'GOLD_VERSION': state['gold_final_version'],
        'GOLD_SOURCE_VERSION': state['gold_source_silver_version'], 'REPLAY_VERSION': state['silver_replay_version'],
        'SOURCE_COUNT': f"{bench['source_rows']:,}".replace(',', '.'),
        'EXPANDED_COUNT': f"{bench['expanded_rows']:,}".replace(',', '.'),
        'COPIES': bench['benchmark_copies'], 'GROUP_COUNT': summary['gold_groups'],
        'CODE_SHA': binding['code_sha256'], 'BENCHMARK_FUNCTION': ast.get_source_segment(source, node),
        'Q1_SQL': queries['Q1_site_day']['sql'].strip() + ';', 'Q2_SQL': queries['Q2_weighted_day']['sql'].strip() + ';',
        'Q3_SQL': queries['Q3_above25_rate']['sql'].strip() + ';',
        'Q1_ROWS': len(queries['Q1_site_day']['rows']), 'Q2_ROWS': len(queries['Q2_weighted_day']['rows']),
        'Q3_ROWS': len(queries['Q3_above25_rate']['rows']),
        'LAYOUT_TABLE': table(['Layout', 'Writer partitions', 'Total data files', 'Total bytes', 'Setup/ghi (giây)'],
            [[label, 32 if label == 'A' else 5, data['total_table_data_files'], data['total_table_bytes'],
              f"{bench['setup_seconds_excluded_from_query_times'][label]:.9f}"] for label, data in bench['layout_details'].items()]),
        'TIMING_TABLE': table(['Lần', 'Vòng', 'Layout', 'Driver wall time (giây)', 'SQL execution ID', 'File thực đọc', 'Đáp án'], timings),
        'MEDIAN_A': f"{bench['median_seconds']['A']:.9f}", 'MEDIAN_B': f"{bench['median_seconds']['B']:.9f}",
        'MEDIAN_RATIO': f"{bench['A_median_over_B_median']:.6f}",
        'BENCHMARK_ANSWER_TABLE': table(['Site', 'n_observations', 'sum_temperature_c', 'n_late'],
            [[r[k] for k in ('site', 'n_observations', 'sum_temperature_c', 'n_late')] for r in bench['query_result']]),
        'PLAN_A_FILTER': next(line for line in (ev / 'plan_A.txt').read_text().splitlines() if 'PushedFilters:' in line),
        'PLAN_B_FILTER': next(line for line in (ev / 'plan_B.txt').read_text().splitlines() if 'PartitionFilters:' in line),
        'A_TABLE_FILES': bench['layout_details']['A']['total_table_data_files'],
        'B_TABLE_FILES': bench['layout_details']['B']['total_table_data_files'],
        'A_SCAN_FILES': next(row[5] for row in timings if row[2] == 'A'),
        'B_SCAN_FILES': next(row[5] for row in timings if row[2] == 'B'),
        'N_CHECKS': len(result['checks']), 'N_TESTS': tests['tests_run'],
        'TOTAL_TEMPERATURE': summary['temperature_sum_c'],
        'SITE_COUNTS_FEB02': ', '.join('`' + key + '=' + str(value) + '`' for key, value in feb02.items()),
        'PREDICTED_AT': prediction['recorded_at_utc'], 'COMPLETED_AT': result['completed_at_utc'],
        'DENOMINATOR_TABLE': table(['Ngày', 'Tử số °C', 'Mẫu số quan sát', 'Mean có trọng số °C', 'Mean các mean °C'], denominator_rows),
    }
    filename = '24022463_DamQuangTien_RoleD.md'
    doc = (root / 'templates/individual' / filename).read_text(encoding='utf-8')
    for key, value in markers.items():
        doc = doc.replace('{{' + key + '}}', str(value))
    if re.search(r'\{\{[A-Z0-9_]+\}\}', doc):
        raise ValueError('UNRESOLVED_ROLE_D_TEMPLATE_FIELD')
    target = reports / 'individual'
    target.mkdir(exist_ok=True)
    code_prefix = Path(os.path.relpath(root, target)).as_posix()
    for folder in ('evidence/', 'templates/'):
        doc = doc.replace('](../../' + folder, '](' + code_prefix + '/' + folder)
    (target / filename).write_text(doc, encoding='utf-8')
    return filename


if __name__ == '__main__':
    config = read_json(ROOT / 'config/team.json')
    filename = build_role_d_record(ROOT, (ROOT / config.get('reports_dir', '.')).resolve(), ROOT / config['evidence_dir'])
    print('Prepared', filename)
