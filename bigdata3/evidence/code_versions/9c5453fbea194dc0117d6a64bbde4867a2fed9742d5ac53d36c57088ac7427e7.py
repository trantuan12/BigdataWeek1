"""Render the minimally corrected original role A record and update its index rows."""
import csv
import io
import os
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from common import read_json


def build_role_a_record(root, reports, ev):
    result = read_json(ev / 'role_A_check.json')
    prediction = read_json(ev / 'role_A_prediction.json')
    if not result['passed'] or prediction['recorded_at_utc'] > result['completed_at_utc']:
        raise ValueError('VERIFIED_ROLE_A_RECORD_REQUIRED')
    runtime = read_json(ev / 'runtime_preflight.json')
    state = read_json(ev / 'state.json')
    traces = read_json(ev / 'role_A_source_trace.json')['rows']
    markers = {
        'SPARK': runtime['spark'], 'DELTA': runtime['delta_spark'], 'PYTHON': runtime['python'],
        'JAVA': runtime['java'], 'HADOOP': runtime['hadoop'], 'APP_ID': runtime['application_id'],
        'RUN_ID': read_json(ev / 'run_preflight.json')['run_id'],
        'SILVER_VERSION': str(state['silver_final_version']), 'GOLD_VERSION': str(state['gold_final_version']),
        'N_CHECKS': str(len(result['checks'])), 'COMPLETED_AT': result['completed_at_utc'],
    }
    for row in traces:
        markers['SOURCE_' + row['record_id']] = repr(row['source_projection'])
        markers['SILVER_' + row['record_id']] = repr(row['selected_release_row'])
    filename = '24022484_TranAnhTuan_RoleA.md'
    doc = (root / 'templates' / 'individual' / filename).read_text(encoding='utf-8')
    for key, value in markers.items():
        doc = doc.replace('{{' + key + '}}', value)
    if re.search(r'\{\{[A-Z0-9_]+\}\}', doc):
        raise ValueError('UNRESOLVED_ROLE_A_TEMPLATE_FIELD')
    individual = reports / 'individual'
    individual.mkdir(exist_ok=True)
    code_prefix = Path(os.path.relpath(root, individual)).as_posix()
    for folder in ('evidence/', 'src/'):
        doc = doc.replace('](../../' + folder, '](' + code_prefix + '/' + folder)
    (individual / filename).write_text(doc, encoding='utf-8')
    contribution_path = reports / 'contributions.csv'
    with contribution_path.open(encoding='utf-8-sig', newline='') as handle:
        reader = csv.DictReader(handle)
        fields = reader.fieldnames
        contributions = [row for row in reader if row['role'] != 'A']
    contributions.insert(0, dict(zip(fields, [
        '24022484', 'A', 'tools/check_role_a.py|config/team.json|src/session.py',
        'Input integrity, handoff/runtime verification; exact source-to-pinned-snapshot trace for role E',
        'evidence/role_A_check.json|evidence/role_A_source_trace.json|evidence/role_A_worker_probe.json',
        'Role E source-to-snapshot trace, candidate versions and object hashes', 'individual/' + filename])))
    checks_path = reports / 'manual_checks.csv'
    with checks_path.open(encoding='utf-8-sig', newline='') as handle:
        reader = csv.DictReader(handle)
        check_fields = reader.fieldnames
        manual = [row for row in reader if row['check_id'] != 'A_CHECK_E']
    manual.insert(0, dict(zip(check_fields, [
        'A_CHECK_E', 'tools/check_role_a.py',
        'Pinned Silver v1 traces to exact source ordinals/hashes; R010001 selects row 1 at 19.10 C',
        f"PASS: {len(result['checks'])} live checks and three matching traces; zero Delta writes",
        'evidence/role_A_prediction.json|evidence/role_A_check.json|evidence/role_A_source_trace.json',
        'Prediction recorded before evaluation; actual pinned Delta reads; script execution only'])))
    for path, headers, rows_to_write in ((contribution_path, fields, contributions), (checks_path, check_fields, manual)):
        buffer = io.StringIO(newline='')
        writer = csv.DictWriter(buffer, fieldnames=headers, lineterminator='\n')
        writer.writeheader()
        writer.writerows(rows_to_write)
        path.write_text(buffer.getvalue(), encoding='utf-8-sig')
    return filename


if __name__ == '__main__':
    config = read_json(ROOT / 'config/team.json')
    target = build_role_a_record(ROOT, (ROOT / config.get('reports_dir', '.')).resolve(), ROOT / config['evidence_dir'])
    print('Prepared', target)
