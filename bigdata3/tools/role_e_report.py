"""Render role E with the same three assessment sections as A and D."""
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


def build_role_e_record(root, reports, ev):
    result = read_json(ev / 'role_E_check.json')
    prediction = read_json(ev / 'role_E_prediction.json')
    member_e = next(member for member in read_json(root / 'team.json') if member['role'] == 'E')
    if not result['passed'] or prediction['recorded_at_utc'] > result['completed_at_utc']:
        raise ValueError('VERIFIED_ROLE_E_RECORD_REQUIRED')
    if result['member_id'] != member_e['student_id'] or result['member_name'] != member_e['name']:
        raise ValueError('RECHECK_ROLE_E_AFTER_MEMBER_CHANGE')
    state = read_json(ev / 'state.json')
    binding = read_json(ev / 'release_candidate/snapshot_binding.json')
    catalog = read_json(ev / 'release_candidate/catalog.json')
    manifest = read_json(ev / 'release_candidate/release_manifest.json')
    runtime = read_json(ev / 'runtime_role_E_check.json')
    gates = read_json(ev / 'role_E_gate_tests.json')
    edges = read_json(ev / 'role_E_edge_cases.json')
    lineage = read_json(ev / 'role_E_lineage_check.json')
    measured = read_json(ev / 'role_E_catalog_check.json')
    tests = read_json(ev / 'implementation_tests.json')
    source = (root / 'templates/student_impl.py').read_text(encoding='utf-8')
    node = next(n for n in ast.parse(source).body if isinstance(n, ast.FunctionDef) and n.name == 'validate_release_binding')
    binding_keys = ('silver_path', 'silver_version', 'gold_path', 'gold_version', 'gold_source_silver_version')
    cross = [c for c in result['checks'] if c['check_id'].startswith('E_cross_A_')]
    markers = {
        'GROUP_ID': state['group_id'], 'RUN_LABEL': state['run_label'],
        'MEMBER_E_NAME': member_e['name'], 'MEMBER_E_ID': member_e['student_id'],
        'SPARK': runtime['spark'], 'DELTA': runtime['delta_spark'], 'PYTHON': runtime['python'],
        'JAVA': runtime['java'], 'HADOOP': runtime['hadoop'],
        'TRUSTED_SHA': state['trusted_manifest_sha256'], 'HANDOFF_LOGICAL_SHA': state['handoff_logical_sha256'],
        'RELEASE_ID': binding['release_id'], 'REVIEW_RUN_ID': result['run_id'],
        'SILVER_VERSION': binding['silver_version'], 'GOLD_VERSION': binding['gold_version'],
        'GOLD_SOURCE_VERSION': binding['gold_source_silver_version'], 'REPLAY_VERSION': state['silver_replay_version'],
        'CODE_SHA': binding['code_sha256'], 'VALIDATOR_SHA': binding['release_validator_sha256'],
        'VALIDATOR_FUNCTION': ast.get_source_segment(source, node),
        'GATE_TABLE': table(['Ca thử', 'Kỳ vọng candidate', 'Quan sát', 'Lý do', 'Kết quả test'],
            [[g['case'], 'Chấp nhận' if g['expected_pass'] else 'Từ chối', 'Chấp nhận' if g['observed_pass'] else 'Từ chối',
              ', '.join(g['reasons']) or 'Không có lỗi', 'PASS' if g['test_passed'] else 'FAIL'] for g in gates]),
        'N_EDGE_CASES': len(edges), 'N_CHECKS': len(result['checks']), 'N_TESTS': tests['tests_run'],
        'BINDING_TABLE': table(['Trường binding', 'Giá trị đã kiểm tra'], [[key, '`' + str(binding[key]) + '`'] for key in binding_keys]),
        'SILVER_ROWS': f"{measured['silver_rows']:,}".replace(',', '.'),
        'GOLD_ROWS': measured['gold_groups'],
        'EXPANDED_ROWS': f"{read_json(ev / 'benchmark_report.json')['expanded_rows']:,}".replace(',', '.'),
        'BASELINE_ROWS': f"{next(c['observed']['rows'] for c in cross if c['check_id'] == 'E_cross_A_logical_handoff'):,}".replace(',', '.'),
        'CATALOG_TABLE': table(['Dataset', 'Version', 'Rows đo thực', 'Phân loại'],
            [[d['dataset_id'], d['version'], d['row_count'], d['classification']] for d in catalog['datasets']]),
        'ALLOWED_USE': catalog['allowed_use'],
        'OBJECT_HASH_TABLE': table(['Object', 'Bytes', 'SHA-256'], [[o['key'], o['bytes'], o['sha256']] for o in manifest['objects']]),
        'LINEAGE_EVENTS': lineage['events'], 'LINEAGE_RUNS': lineage['runs'],
        'PREDICTED_AT': prediction['recorded_at_utc'], 'COMPLETED_AT': result['completed_at_utc'],
        'CROSS_A_TABLE': table(['Kiểm tra A', 'Giá trị quan sát', 'Kết quả'],
            [[c['check_id'], '; '.join(key + '=' + str(value) for key, value in c['observed'].items()), 'PASS'] for c in cross]),
    }
    plain_name = unicodedata.normalize('NFD', member_e['name'].replace('Đ', 'D').replace('đ', 'd')).encode('ascii', 'ignore').decode()
    name_slug = ''.join(part[:1].upper() + part[1:] for part in re.findall(r'[A-Za-z0-9]+', plain_name))
    filename = member_e['student_id'] + '_' + name_slug + '_RoleE.md'
    doc = (root / 'templates/individual' / filename).read_text(encoding='utf-8')
    for key, value in markers.items():
        doc = doc.replace('{{' + key + '}}', str(value))
    if re.search(r'\{\{[A-Z0-9_]+\}\}', doc):
        raise ValueError('UNRESOLVED_ROLE_E_TEMPLATE_FIELD')
    individual = reports / 'individual'
    individual.mkdir(exist_ok=True)
    code_prefix = Path(os.path.relpath(root, individual)).as_posix()
    for folder in ('evidence/', 'templates/'):
        doc = doc.replace('](../../' + folder, '](' + code_prefix + '/' + folder)
    (individual / filename).write_text(doc, encoding='utf-8')
    additions = [
        (reports / 'contributions.csv', 'role', 'E', {
            'member_id': member_e['student_id'], 'role': 'E',
            'authored_file': 'templates/student_impl.py:validate_release_binding|tools/check_role_e.py',
            'change_description': 'Release binding, five required gates, refusal edge cases, independent pinned-data review, catalog and lineage checks',
            'test_evidence': 'evidence/role_E_check.json|evidence/release_review.json|evidence/role_E_gate_tests.json',
            'independent_cross_check': 'Role A trusted input bytes, handoff identity and actual baseline snapshot',
            'exit_response_file': 'individual/' + filename}),
        (reports / 'manual_checks.csv', 'check_id', 'E_CHECK_A', {
            'check_id': 'E_CHECK_A', 'operator': 'tools/check_role_e.py',
            'predicted_outcome': '16 trusted inputs match; CSV fallback and actual Silver v0 have the same logical identity',
            'observed_outcome': f"PASS: {len(result['checks'])} review checks; 5 required gates and {len(edges)} refusal cases; zero Delta writes",
            'evidence_location': 'evidence/role_E_prediction.json|evidence/role_E_check.json|evidence/release_review.json',
            'explanation': 'Independent expected values and actual pinned Delta reads; program execution, no personal signature or publication claim'})]
    for path, key, value, addition in additions:
        with path.open(encoding='utf-8-sig', newline='') as handle:
            reader = csv.DictReader(handle)
            fields = reader.fieldnames
            rows = [row for row in reader if row[key] != value]
        rows.append(addition)
        buffer = io.StringIO(newline='')
        writer = csv.DictWriter(buffer, fieldnames=fields, lineterminator='\n')
        writer.writeheader()
        writer.writerows(rows)
        path.write_text(buffer.getvalue(), encoding='utf-8-sig')
    return filename


if __name__ == '__main__':
    config = read_json(ROOT / 'config/team.json')
    filename = build_role_e_record(ROOT, (ROOT / config.get('reports_dir', '.')).resolve(), ROOT / config['evidence_dir'])
    print('Prepared', filename)
