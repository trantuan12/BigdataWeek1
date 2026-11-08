"""Render role C with the same three assessment sections as A, D, and E."""
import ast
import csv
import io
import os
from pathlib import Path
import re
import sys
import unicodedata

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from common import read_json


def table(headers, rows):
    return '| ' + ' | '.join(headers) + ' |\n| ' + ' | '.join('---' for _ in headers) + ' |\n' + ''.join(
        '| ' + ' | '.join(str(v).replace('|', '/') for v in row) + ' |\n' for row in rows)


def build_role_c_record(root, reports, ev):
    result = read_json(ev / 'role_C_check.json')
    prediction = read_json(ev / 'role_C_prediction.json')
    member_c = next(member for member in read_json(root / 'team.json') if member['role'] == 'C')
    if not result['passed'] or prediction['recorded_at_utc'] > result['completed_at_utc']:
        raise ValueError('VERIFIED_ROLE_C_RECORD_REQUIRED')
    if result['member_id'] != member_c['student_id'] or result['member_name'] != member_c['name']:
        raise ValueError('RECHECK_ROLE_C_AFTER_MEMBER_CHANGE')

    state = read_json(ev / 'state.json')
    inc = read_json(ev / 'incremental.json')
    runtime = read_json(ev / 'runtime_incremental.json')
    live = read_json(ev / 'live_table_verification.json')
    mutations = read_json(ev / 'input_contract_tests.json')
    bench = read_json(ev / 'benchmark_report.json')
    scan = read_json(ev / 'benchmark_scan_metrics.json')
    gold_v1 = read_json(ev / 'gold_after_batch02.json')
    tests = read_json(ev / 'implementation_tests.json')

    source = (root / 'templates/student_impl.py').read_text(encoding='utf-8')
    parsed = ast.parse(source)
    choose_node = next(n for n in parsed.body if isinstance(n, ast.FunctionDef) and n.name == 'choose_revisions')
    merge_node = next(n for n in parsed.body if isinstance(n, ast.FunctionDef) and n.name == 'merge_silver')

    superseded_rows = [
        [
            f"`{item['record_id']}`",
            f"`{item['ingest_time_utc']}`",
            f"row `{item['winner_row']}` (`{item['winner_temp_c']} °C`)",
            f"row `{item['superseded_row']}` (`{item['superseded_temp_c']} °C`)",
            f"`source_row` nhỏ hơn (`{item['winner_row']} < {item['superseded_row']}`)",
        ]
        for item in result['superseded_rows']
    ]

    rep_rows = []
    for key in ('R010001', 'R003001', 'R002001'):
        tr = result['representative_traces'][key]
        prev = tr['previous']
        win = tr['incoming_winner']
        sel = tr['selected']
        prev_str = (f"`{prev['source_object']}` row `{prev['source_row']}` (`{prev['temperature_c']} °C`, ingest `{prev['ingest_time_utc']}`)"
                    if prev else '*(Chưa tồn tại trong Silver v0)*')
        win_str = f"`{win['source_object']}` row `{win['source_row']}` (`{win['temperature_c']} °C`, ingest `{win['ingest_time_utc']}`)"
        sel_str = f"`{sel['source_object']}` row `{sel['source_row']}` (`{sel['temperature_c']} °C`, ingest `{sel['ingest_time_utc']}`)"
        rep_rows.append([f"`{key}`", f"`{tr['decision']}`", prev_str, win_str, sel_str])

    silver_hist = {entry['version']: entry for entry in live['tables']['silver']['history']}
    delta_hist_rows = []
    for ver in (0, 1, 2):
        entry = silver_hist[ver]
        m = entry['operationMetrics']
        delta_hist_rows.append([
            f"`v{ver}`",
            f"`{entry['operation']}`",
            m.get('numSourceRows', '—'),
            m.get('numTargetRowsInserted', m.get('numOutputRows', '0')),
            m.get('numTargetRowsMatchedUpdated', '0'),
            m.get('numTargetRowsCopied', '0'),
            m.get('numOutputRows', '0'),
        ])

    mutation_descriptions = {
        'VALUE': 'Nhiệt độ vượt ngưỡng hợp đồng (`temperature_c = 100.00`)',
        'REQUIRED_VALUE': 'Thiếu giá trị bắt buộc (`temperature_c = None`)',
        'SCHEMA_ALLOWLIST': 'Xuất hiện cột hạn chế ngoài schema (`operator_email`)',
    }
    mutation_rows = [
        [
            f"`{m['case']}`",
            mutation_descriptions.get(m['case'], m['case']),
            f"`{m['observed']}`",
            str(m['writes_performed']),
            'PASS' if m['passed'] else 'FAIL',
        ]
        for m in mutations
    ]

    gold_feb06 = {r['site']: r for r in gold_v1 if r['event_date'] == '2026-02-06'}
    cross_d_result_rows = []
    for row in bench['query_result']:
        site = row['site']
        g = gold_feb06[site]
        cross_d_result_rows.append([
            f"`{site}`",
            f"{g['n_observations']} × {bench['benchmark_copies']} = **{row['n_observations']}**",
            f"{g['sum_temperature_c']} × {bench['benchmark_copies']} = **{row['sum_temperature_c']}**",
            f"{g['n_late']} × {bench['benchmark_copies']} = **{row['n_late']}**",
            'PASS',
        ])

    cross_d_timing_rows = []
    for index, measurement in enumerate(bench['measurements'], 1):
        execution = next(e for e in scan['executions'] if e.get('measurement_number') == index)
        files = [m['value'] for node in execution['scan_nodes'] for m in node['metrics'] if m['name'] == 'number of files read']
        cross_d_timing_rows.append([
            index,
            measurement['round'],
            measurement['layout'],
            f"{measurement['elapsed_seconds']:.9f}",
            execution['sql_execution_id'],
            files[0],
            'PASS' if measurement['result_equal'] else 'FAIL',
        ])

    markers = {
        'MEMBER_C_NAME': member_c['name'],
        'MEMBER_C_ID': member_c['student_id'],
        'GROUP_ID': state['group_id'],
        'RUN_LABEL': state['run_label'],
        'SPARK': runtime['spark'],
        'DELTA': runtime['delta_spark'],
        'PYTHON': runtime['python'],
        'JAVA': runtime['java'],
        'HADOOP': runtime['hadoop'],
        'TRUSTED_SHA': state['trusted_manifest_sha256'],
        'HANDOFF_LOGICAL_SHA': state['handoff_logical_sha256'],
        'INCREMENTAL_RUN_ID': state['run_ids']['incremental'],
        'BRONZE_INIT': state['bronze_initial_version'],
        'BRONZE_FINAL': state['bronze_final_version'],
        'BRONZE_REPLAY': state['bronze_replay_version'],
        'SILVER_INIT': state['silver_initial_version'],
        'SILVER_FINAL': state['silver_final_version'],
        'SILVER_REPLAY': state['silver_replay_version'],
        'GOLD_INIT': state['gold_initial_version'],
        'GOLD_FINAL': state['gold_final_version'],
        'BASELINE_ROWS': '9,610',
        'INCOMING_ROWS': inc['incoming_rows'],
        'INCOMING_WINNERS': inc['incoming_winners'],
        'SUPERSEDED_COUNT': inc['incoming_rows'] - inc['incoming_winners'],
        'SILVER_ROWS': f"{inc['silver_rows']:,}",
        'BRONZE_RECEIPTS': f"{inc['bronze_after_replay']:,}",
        'BRONZE_SHA': inc['bronze_receipt_sha256_after_replay'],
        'SILVER_AFTER_SHA': inc['logical_sha256_after'],
        'SILVER_REPLAY_SHA': inc['logical_sha256_replay'],
        'GOLD_GROUPS': len(gold_v1),
        'CODE_SHA': state['final_implementation_sha256'],
        'CHOOSE_REVISIONS_FUNCTION': ast.get_source_segment(source, choose_node),
        'MERGE_SILVER_FUNCTION': ast.get_source_segment(source, merge_node),
        'SUPERSEDED_TABLE': table(
            ['Khóa record_id', 'Thời điểm ingest_time_utc', 'Winner được chọn', 'Bản ghi bị thay thế (Superseded)', 'Lý do phá hòa'],
            superseded_rows,
        ),
        'REPRESENTATIVE_TRACE_TABLE': table(
            ['record_id', 'Quyết định', 'Trạng thái trước (Silver v0)', 'Incoming Winner (Batch 02)', 'Trạng thái sau (Silver v1)'],
            rep_rows,
        ),
        'DELTA_HISTORY_TABLE': table(
            ['Phiên bản Silver', 'Thao tác', 'Số dòng nguồn', 'Số dòng Insert', 'Số dòng Update', 'Số dòng Copy (Unchanged)', 'Tổng dòng đầu ra'],
            delta_hist_rows,
        ),
        'MUTATION_TABLE': table(
            ['Mã probe', 'Mô tả vi phạm hợp đồng', 'Mã lỗi bắt được', 'Số lần ghi Delta', 'Kết quả'],
            mutation_rows,
        ),
        'CROSS_D_RESULT_TABLE': table(
            ['Site', 'n_observations (Gold × 100 = A/B)', 'sum_temperature_c (Gold × 100 = A/B)', 'n_late (Gold × 100 = A/B)', 'Đối chiếu'],
            cross_d_result_rows,
        ),
        'CROSS_D_TIMINGS_TABLE': table(
            ['Lần đo', 'Vòng', 'Layout', 'Driver wall time (giây)', 'SQL execution ID', 'File thực đọc', 'Đáp án khớp'],
            cross_d_timing_rows,
        ),
        'MEDIAN_A': f"{bench['median_seconds']['A']:.9f}",
        'MEDIAN_B': f"{bench['median_seconds']['B']:.9f}",
        'MEDIAN_RATIO': f"{bench['A_median_over_B_median']:.6f}",
        'N_CHECKS': len(result['checks']),
        'N_TESTS': tests['tests_run'],
        'PREDICTED_AT': prediction['recorded_at_utc'],
        'COMPLETED_AT': result['completed_at_utc'],
    }

    plain_name = unicodedata.normalize('NFD', member_c['name'].replace('Đ', 'D').replace('đ', 'd')).encode('ascii', 'ignore').decode()
    name_slug = ''.join(part[:1].upper() + part[1:] for part in re.findall(r'[A-Za-z0-9]+', plain_name))
    filename = member_c['student_id'] + '_' + name_slug + '_RoleC.md'
    doc = (root / 'templates/individual' / filename).read_text(encoding='utf-8')
    for key, value in markers.items():
        doc = doc.replace('{{' + key + '}}', str(value))
    if re.search(r'\{\{[A-Z0-9_]+\}\}', doc):
        raise ValueError('UNRESOLVED_ROLE_C_TEMPLATE_FIELD')

    individual = reports / 'individual'
    individual.mkdir(exist_ok=True)
    code_prefix = Path(os.path.relpath(root, individual)).as_posix()
    for folder in ('evidence/', 'templates/'):
        doc = doc.replace('](../../' + folder, '](' + code_prefix + '/' + folder)
    (individual / filename).write_text(doc, encoding='utf-8')

    role_order = {'A': 0, 'B': 1, 'C': 2, 'D': 3, 'E': 4}
    check_order = {'A_CHECK_E': 0, 'B_CHECK_C': 1, 'C_CHECK_D': 2, 'D_CHECK_B': 3, 'E_CHECK_A': 4}

    contrib_path = reports / 'contributions.csv'
    with contrib_path.open(encoding='utf-8-sig', newline='') as handle:
        reader = csv.DictReader(handle)
        fields = reader.fieldnames
        contrib_rows = [row for row in reader if row['role'] != 'C']
    contrib_rows.append({
        'member_id': member_c['student_id'],
        'role': 'C',
        'authored_file': 'templates/student_impl.py:choose_revisions|templates/student_impl.py:merge_silver|tools/check_role_c.py',
        'change_description': 'Deterministic incoming winner selection, conditional Silver merge, Bronze/Silver replay idempotence, time-travel and invalid-batch checks',
        'test_evidence': 'evidence/role_C_check.json|evidence/incremental.json|evidence/row_trace.json|evidence/input_contract_tests.json',
        'independent_cross_check': 'Role D layout A/B result equivalence across all six evaluations and operational Gold isolation',
        'exit_response_file': 'individual/' + filename,
    })
    contrib_rows.sort(key=lambda r: role_order.get(r['role'], 99))

    manual_path = reports / 'manual_checks.csv'
    with manual_path.open(encoding='utf-8-sig', newline='') as handle:
        reader = csv.DictReader(handle)
        manual_fields = reader.fieldnames
        manual_rows = [row for row in reader if row['check_id'] != 'C_CHECK_D']
    manual_rows.append({
        'check_id': 'C_CHECK_D',
        'operator': 'tools/check_role_c.py',
        'predicted_outcome': 'All 6 A/B benchmark evaluations in A-B-B-A-A-B order return identical 4-site aggregates equal to 100x operational Gold on 2026-02-06',
        'observed_outcome': f"PASS: {len(result['checks'])} checks (M01-M08 and 6/6 equivalent A/B evaluations); zero Delta writes",
        'evidence_location': 'evidence/role_C_prediction.json|evidence/role_C_check.json|evidence/benchmark_report.json',
        'explanation': 'Prediction recorded before assertion evaluation; independent recomputation from post-merge Silver/Gold; automated read-only check',
    })
    manual_rows.sort(key=lambda r: check_order.get(r['check_id'], 99))

    for path, headers, rows_to_write in ((contrib_path, fields, contrib_rows), (manual_path, manual_fields, manual_rows)):
        buffer = io.StringIO(newline='')
        writer = csv.DictWriter(buffer, fieldnames=headers, lineterminator='\n')
        writer.writeheader()
        writer.writerows(rows_to_write)
        path.write_text(buffer.getvalue(), encoding='utf-8-sig')

    return filename


if __name__ == '__main__':
    config = read_json(ROOT / 'config/team.json')
    filename = build_role_c_record(ROOT, (ROOT / config.get('reports_dir', '.')).resolve(), ROOT / config['evidence_dir'])
    print('Prepared', filename)
