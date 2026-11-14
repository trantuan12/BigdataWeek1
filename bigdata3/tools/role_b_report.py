"""Render role B with the same three assessment sections as A, C, D, and E."""
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


def build_role_b_record(root, reports, ev):
    result = read_json(ev / 'role_B_check.json')
    prediction = read_json(ev / 'role_B_prediction.json')
    model = read_json(ev / 'role_B_gold_model.json')
    unit_tests = read_json(ev / 'role_B_unit_tests.json')
    team = read_json(root / 'team.json')
    member_b = next(member for member in team if member['role'] == 'B')
    member_c = next(member for member in team if member['role'] == 'C')
    if not result['passed'] or not model['passed'] or prediction['recorded_at_utc'] > result['completed_at_utc']:
        raise ValueError('VERIFIED_ROLE_B_RECORD_REQUIRED')
    if result['member_id'] != member_b['student_id'] or result['member_name'] != member_b['name']:
        raise ValueError('RECHECK_ROLE_B_AFTER_MEMBER_CHANGE')

    state = read_json(ev / 'state.json')
    inc = read_json(ev / 'incremental.json')
    runtime = read_json(ev / 'runtime_bootstrap.json')
    live = read_json(ev / 'live_table_verification.json')
    gold_v1 = read_json(ev / 'gold_after_batch02.json')
    gold_schema = read_json(root / 'schemas/gold_schema.json')
    tests = read_json(ev / 'implementation_tests.json')

    source = (root / 'templates/student_impl.py').read_text(encoding='utf-8')
    parsed = ast.parse(source)
    build_gold_node = next(n for n in parsed.body if isinstance(n, ast.FunctionDef) and n.name == 'build_gold')

    layer_rows = [
        [
            'Bronze',
            f"`{state['bronze_path']}`",
            '`(source_object, source_row, source_sha256)`',
            f"`v{state['bronze_initial_version']}` (9,610 dòng) → `v{state['bronze_final_version']}` (9,750 dòng; replay `v{state['bronze_replay_version']}`)",
            'Lưu biên nhận vật lý hợp lệ; không chèn trùng biên nhận khi replay',
        ],
        [
            'Silver',
            f"`{state['silver_path']}`",
            '`record_id`',
            f"`v{state['silver_initial_version']}` (9,610 dòng) → `v{state['silver_final_version']}` (9,710 dòng; replay `v{state['silver_replay_version']}`)",
            '1 dòng hiện hành / `record_id`; giữ 10 cột Lab 2 + `event_date` (UTC); giữ cả dòng `is_late`',
        ],
        [
            'Gold',
            f"`{state['gold_path']}`",
            '`(site, event_date)`',
            f"`v{state['gold_initial_version']}` (16 nhóm) → `v{state['gold_final_version']}` (20 nhóm, đọc từ `Silver v{state['gold_source_silver_version']}`)",
            'Tổng hợp 11 cột theo site/ngày UTC; bảo toàn số đếm và tổng nhiệt độ cho rollup',
        ],
    ]

    spark_gold_types = {f['name']: f['type'] for f in live['tables']['gold']['schema']['fields']}
    gold_schema_rows = []
    formulas = {
        'site': 'Khóa nhóm (`groupBy`)',
        'event_date': 'Khóa nhóm (`groupBy`, ngày UTC từ `event_time_utc`)',
        'n_observations': '`COUNT(1)` ép kiểu `BIGINT`',
        'sum_temperature_c': '`SUM(temperature_c)` ép kiểu `DECIMAL(18,2)`',
        'mean_temperature_c': '`sum_temperature_c / n_observations` ép kiểu `DECIMAL(18,6)`',
        'min_temperature_c': '`MIN(temperature_c)` ép kiểu `DECIMAL(18,2)`',
        'max_temperature_c': '`MAX(temperature_c)` ép kiểu `DECIMAL(18,2)`',
        'n_late': '`SUM(CASE WHEN is_late THEN 1 ELSE 0 END)` ép kiểu `BIGINT`',
        'late_rate': '`n_late / n_observations` ép kiểu `DECIMAL(18,6)`',
        'n_above_25': '`SUM(CASE WHEN temperature_c > 25 THEN 1 ELSE 0 END)` ép kiểu `BIGINT`',
        'n_sensors': '`COUNT(DISTINCT sensor_id)` ép kiểu `BIGINT`',
    }
    for index, col in enumerate(gold_schema['columns'], 1):
        name = col['name']
        gold_schema_rows.append([
            index,
            f"`{name}`",
            f"`{col['type']}`",
            f"`{spark_gold_types[name]}`",
            formulas[name],
            col['meaning'],
        ])

    b03_obs = next(c['observed'] for c in model['checks'] if c['check_id'] == 'B03_conservation_counts_and_sums')
    conservation_rows = [
        [
            'Baseline (`Silver v0` → `Gold v0`)',
            f"{b03_obs['v0']['silver_rows']:,}",
            f"{b03_obs['v0']['gold_n_observations']:,}",
            b03_obs['v0'][' groups'] if ' groups' in b03_obs['v0'] else b03_obs['v0']['groups'],
            f"{b03_obs['v0']['silver_temp_sum']} °C",
            f"{b03_obs['v0']['gold_temp_sum']} °C",
            f"{b03_obs['v0']['silver_late']} = {b03_obs['v0']['gold_n_late']}",
            f"{b03_obs['v0']['silver_above_25']} = {b03_obs['v0']['gold_n_above_25']}",
            'PASS',
        ],
        [
            'Post-merge (`Silver v1` → `Gold v1`)',
            f"{b03_obs['v1']['silver_rows']:,}",
            f"{b03_obs['v1']['gold_n_observations']:,}",
            b03_obs['v1']['groups'],
            f"{b03_obs['v1']['silver_temp_sum']} °C",
            f"{b03_obs['v1']['gold_temp_sum']} °C",
            f"{b03_obs['v1']['silver_late']} = {b03_obs['v1']['gold_n_late']}",
            f"{b03_obs['v1']['silver_above_25']} = {b03_obs['v1']['gold_n_above_25']}",
            'PASS',
        ],
    ]

    gold_v1_rows = [
        [
            f"`{g['site']}`",
            f"`{g['event_date']}`",
            g['n_observations'],
            g['sum_temperature_c'],
            g['mean_temperature_c'],
            g['min_temperature_c'],
            g['max_temperature_c'],
            g['n_late'],
            g['late_rate'],
            g['n_above_25'],
            g['n_sensors'],
        ]
        for g in gold_v1
    ]

    daily_rows = []
    for d in result['daily_weighted_vs_naive']:
        sc = d['site_counts']
        counts_str = f"`{sc['zone-1']}/{sc['zone-2']}/{sc['zone-3']}/{sc['zone-4']}`"
        daily_rows.append([
            f"`{d['event_date']}`",
            counts_str,
            d['n_observations'],
            d['sum_temperature_c'],
            f"**{d['weighted_mean_c']}**",
            d['naive_mean_of_means_c'],
            f"`{d['difference_c']}`",
            'Bằng nhau (count đều)' if d['equal_site_counts'] else '**Khác nhau (count lệch)**',
        ])

    stale_rows = []
    for key, info in sorted(result['stale_revisions'].items()):
        cur = info['current_silver_v0']
        inc_row = info['incoming_candidates'][0]
        sel = info['silver_v1']
        stale_rows.append([
            f"`{key}` (`{cur['site']}`)",
            f"`{cur['source_object']}` row `{cur['source_row']}` (`{cur['temperature_c']} °C`, ingest `{cur['ingest_time_utc']}`)",
            f"`{inc_row['source_object']}` row `{inc_row['source_row']}` (`{inc_row['temperature_c']} °C`, ingest `{inc_row['ingest_time_utc']}`)",
            f"Giữ row `{sel['source_row']}` (`{sel['temperature_c']} °C`)",
            'PASS (`unchanged_stale`)',
        ])

    tied_rows = []
    for key, info in sorted(result['tied_revisions'].items()):
        c1, c2 = info['candidates']
        prev = info['previous_silver']
        prev_str = f"`{prev[0]}` row `{prev[1]}` (`{prev[2]} °C`)" if prev else '*(Khóa mới)*'
        sel = info['silver_v1']
        tied_rows.append([
            f"`{key}`",
            f"row `{c1[0]}` (`{c1[1]} °C`) vs row `{c2[0]}` (`{c2[1]} °C`)",
            f"row `{info['expected_winner_row']}` (`{info['expected_winner_temp']} °C`)",
            f"row `{info['superseded_row']}`",
            prev_str,
            f"`{info['decision']}` → row `{sel[1]}` (`{sel[2]} °C`)",
        ])

    truth_rows = [
        [
            f"`{c['case']}`",
            str(c['expected_update']),
            str(c['merge_silver_sql_update']),
            str(c['independent_model_update']),
            'PASS' if c['expected_update'] == c['merge_silver_sql_update'] == c['independent_model_update'] else 'FAIL',
        ]
        for c in result['predicate_truth_table']
    ]

    sens_rows = []
    for group_key, fields in sorted(result['gold_sensitivity']['if_stale_rows_overwrote_silver'].items()):
        sens_rows.append([
            'Nếu Stale ghi đè (`R003001`–`R003010`)',
            f"`{group_key}`",
            f"`{fields['sum_temperature_c'][0]}` → `{fields['sum_temperature_c'][1]}` (`-5.00 °C`)",
            f"`{fields['mean_temperature_c'][0]}` → `{fields['mean_temperature_c'][1]}`",
            f"`{fields['min_temperature_c'][0]}` → `{fields['min_temperature_c'][1]}`",
        ])
    for group_key, fields in sorted(result['gold_sensitivity']['if_last_tied_row_won'].items()):
        min_change = (f"`{fields['min_temperature_c'][0]}` → `{fields['min_temperature_c'][1]}`"
                      if 'min_temperature_c' in fields else '*(Không đổi)*')
        sens_rows.append([
            'Nếu Tied lấy dòng cuối (`R002001`–`R002005`, `R010001`–`R010005`)',
            f"`{group_key}`",
            f"`{fields['sum_temperature_c'][0]}` → `{fields['sum_temperature_c'][1]}` (`+5.00 °C`)",
            f"`{fields['mean_temperature_c'][0]}` → `{fields['mean_temperature_c'][1]}`",
            min_change,
        ])

    markers = {
        'MEMBER_B_NAME': member_b['name'],
        'MEMBER_B_ID': member_b['student_id'],
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
        'BOOTSTRAP_RUN_ID': state['run_ids']['bootstrap'],
        'INCREMENTAL_RUN_ID': state['run_ids']['incremental'],
        'BRONZE_INIT': state['bronze_initial_version'],
        'BRONZE_FINAL': state['bronze_final_version'],
        'BRONZE_REPLAY': state['bronze_replay_version'],
        'SILVER_INIT': state['silver_initial_version'],
        'SILVER_FINAL': state['silver_final_version'],
        'SILVER_REPLAY': state['silver_replay_version'],
        'GOLD_INIT': state['gold_initial_version'],
        'GOLD_FINAL': state['gold_final_version'],
        'GOLD_SOURCE_SILVER': state['gold_source_silver_version'],
        'BASELINE_ROWS': '9,610',
        'CODE_SHA': state['final_implementation_sha256'],
        'SILVER_AFTER_SHA': inc['logical_sha256_after'],
        'BUILD_GOLD_FUNCTION': ast.get_source_segment(source, build_gold_node),
        'TABLE_LAYERS_SUMMARY': table(
            ['Tầng bảng', 'Đường dẫn URI cụ thể', 'Khóa định danh dòng', 'Phiên bản & Số dòng', 'Quy tắc ngữ nghĩa cốt lõi'],
            layer_rows,
        ),
        'GOLD_SCHEMA_TABLE': table(
            ['#', 'Tên cột', 'Kiểu hợp đồng (`gold_schema.json`)', 'Kiểu Spark/Delta thực tế', 'Công thức trong `build_gold`', 'Ý nghĩa nghiệp vụ'],
            gold_schema_rows,
        ),
        'CONSERVATION_TABLE': table(
            ['Snapshot đối chiếu', 'Số dòng Silver', 'SUM(Gold.n_observations)', 'Số nhóm Gold', 'SUM(Silver.temperature_c)', 'SUM(Gold.sum_temperature_c)', 'Bảo toàn n_late', 'Bảo toàn n_above_25', 'Kết quả'],
            conservation_rows,
        ),
        'GOLD_V1_TABLE': table(
            ['site', 'event_date', 'n_obs', 'sum_temp_c', 'mean_temp_c', 'min_temp_c', 'max_temp_c', 'n_late', 'late_rate', 'n_above_25', 'n_sensors'],
            gold_v1_rows,
        ),
        'DAILY_WEIGHTED_TABLE': table(
            ['Ngày UTC', 'Số quan sát từng site (z1/z2/z3/z4)', 'Tổng quan sát N', 'Tổng nhiệt độ (°C)', 'Weighted Mean (°C)', 'Naive Mean-of-Means (°C)', 'Độ lệch (Naive − Weighted)', 'Nhận xét'],
            daily_rows,
        ),
        'STALE_REVISIONS_TABLE': table(
            ['Khóa (Site)', 'Trạng thái hiện hành (Silver v0)', 'Bản ghi đến trong Batch 02 (Stale)', 'Kết quả trong Silver v1', 'Đánh giá'],
            stale_rows,
        ),
        'TIED_REVISIONS_TABLE': table(
            ['Khóa record_id', 'Hai ứng viên đồng hạng trong Batch 02', 'Incoming Winner', 'Bản ghi bị loại (Superseded)', 'Trạng thái trước (Silver v0)', 'Kết quả sau Merge (Silver v1)'],
            tied_rows,
        ),
        'PREDICATE_TRUTH_TABLE': table(
            ['Trường hợp kiểm thử nhánh vị từ', 'Kỳ vọng cập nhật', 'Kết quả SQL `precedes` (merge_silver)', 'Mô hình độc lập Role B', 'Đối chiếu'],
            truth_rows,
        ),
        'GOLD_SENSITIVITY_TABLE': table(
            ['Giả định lỗi chính sách hợp nhất', 'Nhóm Gold bị ảnh hưởng', 'Thay đổi sum_temperature_c (Thực tế → Lỗi)', 'Thay đổi mean_temperature_c', 'Thay đổi min_temperature_c'],
            sens_rows,
        ),
        'N_CHECKS': len(result['checks']),
        'N_UNIT_TESTS': unit_tests['tests_run'],
        'N_TESTS': tests['tests_run'],
        'PREDICTED_AT': prediction['recorded_at_utc'],
        'COMPLETED_AT': result['completed_at_utc'],
    }

    plain_name = unicodedata.normalize('NFD', member_b['name'].replace('Đ', 'D').replace('đ', 'd')).encode('ascii', 'ignore').decode()
    name_slug = ''.join(part[:1].upper() + part[1:] for part in re.findall(r'[A-Za-z0-9]+', plain_name))
    filename = member_b['student_id'] + '_' + name_slug + '_RoleB.md'
    doc = (root / 'templates/individual' / filename).read_text(encoding='utf-8')
    for key, value in markers.items():
        doc = doc.replace('{{' + key + '}}', str(value))
    if re.search(r'\{\{[A-Z0-9_]+\}\}', doc):
        raise ValueError('UNRESOLVED_ROLE_B_TEMPLATE_FIELD')

    individual = reports / 'individual'
    individual.mkdir(exist_ok=True)
    code_prefix = Path(os.path.relpath(root, individual)).as_posix()
    for folder in ('evidence/', 'templates/', 'schemas/'):
        doc = doc.replace('](../../' + folder, '](' + code_prefix + '/' + folder)
    (individual / filename).write_text(doc, encoding='utf-8')

    role_order = {'A': 0, 'B': 1, 'C': 2, 'D': 3, 'E': 4}
    check_order = {'A_CHECK_E': 0, 'B_CHECK_C': 1, 'C_CHECK_D': 2, 'D_CHECK_B': 3, 'E_CHECK_A': 4}

    contrib_path = reports / 'contributions.csv'
    with contrib_path.open(encoding='utf-8-sig', newline='') as handle:
        reader = csv.DictReader(handle)
        fields = reader.fieldnames
        contrib_rows = [row for row in reader if row['role'] != 'B']
    contrib_rows.append({
        'member_id': member_b['student_id'],
        'role': 'B',
        'authored_file': 'templates/student_impl.py:build_gold|schemas/gold_schema.json|tools/check_role_b.py|tests/test_role_b_gold.py',
        'change_description': 'Silver/Gold modeling and build_gold aggregation; B01-B04 conservation and weighted-rollup checks; independent review of Role C stale and tied revisions',
        'test_evidence': 'evidence/role_B_check.json|evidence/role_B_gold_model.json|evidence/role_B_unit_tests.json|evidence/gold_after_batch02.json',
        'independent_cross_check': 'Role C stale and same-time tied revision decisions, SQL merge predicate truth table and Gold sensitivity',
        'exit_response_file': 'individual/' + filename,
    })
    contrib_rows.sort(key=lambda r: role_order.get(r['role'], 99))

    manual_path = reports / 'manual_checks.csv'
    with manual_path.open(encoding='utf-8-sig', newline='') as handle:
        reader = csv.DictReader(handle)
        manual_fields = reader.fieldnames
        manual_rows = [row for row in reader if row['check_id'] != 'B_CHECK_C']
    manual_rows.append({
        'check_id': 'B_CHECK_C',
        'operator': 'tools/check_role_b.py',
        'predicted_outcome': '10 stale keys (R003001-R003010) keep Silver v0 rows; 10 tied keys pick smaller source_row (e.g. R002005 row 105, R010005 row 5); Gold v1 reflects exact winners',
        'observed_outcome': f"PASS: {len(result['checks'])} checks (7 B01-B04 model checks and 7 B_C01-B_C07 stale/tied checks); zero Delta writes",
        'evidence_location': 'evidence/role_B_prediction.json|evidence/role_B_check.json|evidence/role_B_gold_model.json',
        'explanation': 'Prediction recorded before assertion evaluation; independent comparator, SQLite merge-predicate truth table and counterfactual Gold sensitivity; automated read-only check',
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
    filename = build_role_b_record(ROOT, (ROOT / config.get('reports_dir', '.')).resolve(), ROOT / config['evidence_dir'])
    print('Prepared', filename)
