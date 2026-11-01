"""Build the role A record from the executed, pinned trace checks."""
import csv
import io
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from common import read_json


def build_role_a_record(root, reports, ev):
    result = read_json(ev / 'role_A_check.json')
    if not result['passed']:
        raise ValueError('ROLE_A_VERIFICATION_REQUIRED')
    state = read_json(ev / 'state.json')
    runtime = read_json(ev / 'runtime_role_A_check.json')
    binding = read_json(ev / 'release_candidate/snapshot_binding.json')
    trace = read_json(ev / 'role_A_source_trace.json')['rows']
    prediction = read_json(ev / 'role_A_prediction.json')
    if prediction['recorded_at_utc'] > result['completed_at_utc']:
        raise ValueError('ROLE_A_PREDICTION_ORDER')
    individual = reports / 'individual'
    individual.mkdir(exist_ok=True)
    code_prefix = Path(os.path.relpath(root, individual)).as_posix()

    def evidence(name):
        return code_prefix + '/evidence/' + name

    rows = '\n'.join('| ' + ' | '.join(str(v) for v in (
        entry['record_id'], entry['selected_release_row']['source_object'],
        entry['selected_release_row']['source_row'], entry['selected_release_row']['temperature_c'],
        entry['arrival_lag_seconds'], entry['selected_release_row']['is_late'])) + ' |' for entry in trace)
    hashes = '\n'.join('- `' + entry['selected_release_row']['source_object'] + '`: `' + entry['source_file_sha256'] + '`.' for entry in trace)
    doc = f'''# BÁO CÁO CÁ NHÂN — ROLE A

**Trần Anh Tuấn — 24022484**  
Lab 3 — Lakehouse Processing & Analytics. Nhóm `{state['group_id']}`, run `{state['run_label']}`.

Nhiệm vụ chính: tiếp nhận handoff, kiểm tra tính toàn vẹn đầu vào, runtime và schema. Kiểm tra chéo: tái lập truy vết source-to-snapshot của role E. Hồ sơ ghi kết quả kiểm tra bằng chương trình và nội dung chuẩn bị bảo vệ; không dùng log thay cho phần trình bày của thành viên.

## Phần 1. Đóng góp kỹ thuật

### H01 — Byte, schema và nguồn tin cậy

Manifest cấp riêng có SHA-256 `{state['trusted_manifest_sha256']}`. Cả **16/16** tệp khai báo khớp số byte và SHA-256. Mười cột handoff được giữ: `record_id`, `sensor_id`, `site`, `event_time_utc`, `ingest_time_utc`, `temperature_c`, `is_late`, `source_object`, `source_row`, `source_sha256`. Silver thêm `event_date` theo UTC. Nhiệt độ dùng DECIMAL(8,2), ordinal dùng BIGINT và cờ late dùng BOOLEAN.

Kiểm tra byte chứng minh đồng nhất với manifest được cấp; tính xác thực của kênh phân phối do giảng viên bảo đảm. Không thay hash kỳ vọng. Dữ liệu nguồn không bị sửa để làm khớp kiểm tra. Các giá trị và trường hạn chế trong raw không được đưa vào hồ sơ này.

### H02 — Handoff logic

Sử dụng `course_fallback_not_student_approval`, gồm **9.610** quan sát từ CSV transport của học phần. Đây không phải Parquet đã được nhóm phê duyệt ở Lab 2. Digest logic của handoff và Silver v0 cùng là `{state['handoff_logical_sha256']}`. Hợp đồng giữ UTC, AS_OF cố định `2026-02-09T00:00:00Z`, điều kiện late là lag **> 900 giây** và giữ các quan sát đến muộn hợp lệ.

### H03 — Delta từ chối schema ngoài hợp đồng

Probe preflight đã thử append cột ngoài schema và bị Delta từ chối dưới dạng lỗi analysis. `delta_schema_probe.json` ghi `passed=true`, tám dòng ban đầu được giữ. Lần kiểm tra A đọc lại chính bảng probe và xác nhận vẫn có **8 dòng**; lần kiểm tra này không append dữ liệu mới.

### H04 — Runtime và placement

Runtime kiểm tra A: Python **{runtime['python']}**, Spark **{runtime['spark']}**, Delta **{runtime['delta_spark']}**, Java **{runtime['java']}**, Hadoop **{runtime['hadoop']}**, timezone UTC. Application ID: `{runtime['application_id']}`. Master: `spark://spark-master:7077`; các bảng dùng S3A chung cho driver/executor. Probe ghi nhận **24 partition task trên hai worker container**. Hai container không chứng minh hai máy chủ vật lý.

Mã kiểm tra: `tools/check_role_a.py`. Các phần cần giải thích là kiểm tra manifest, allowlist/type của handoff, nguồn gốc physical ordinal và cách chọn đúng snapshot thay vì dùng latest.

## Phần 2. Kiểm tra chéo role E

### 2.1. Dự đoán trước khi thực thi

Dự đoán được ghi lúc `{prediction['recorded_at_utc']}` trước khi đánh giá các assertion. Candidate phải ghim **Silver v{binding['silver_version']} / Gold v{binding['gold_version']}**, với `gold_source_silver_version={binding['gold_source_silver_version']}`. Ba mẫu phải giải được cả object, ordinal, hash và giá trị trong đúng snapshot đó.

| Record ID | Source object | Physical ordinal | Nhiệt độ °C | Lag giây | is_late |
| --- | --- | --- | --- | --- | --- |
{rows}

Ordinal tính từ 1, không tính header CSV; JSONL dùng số dòng vật lý. SHA-256 được tính trên toàn bộ byte file:

{hashes}

### 2.2. Cách thực thi và kết quả

Chạy tại thư mục có `compose.yaml` (`03_DU_AN` trong bộ đã quy hoạch):

```powershell
docker compose exec -T driver python tools/check_role_a.py --config config/team.json --trusted-manifest /instructor/trusted_lab3_inputs.json
```

Chương trình:

1. Kiểm lại 16 input và digest handoff, đối chiếu binding với `state.json` và implementation đã lưu theo hash.
2. Đọc Silver v0 và **Silver v{binding['silver_version']} bằng versionAsOf**; không thay bằng snapshot mới nhất.
3. Lấy từng mẫu từ snapshot đã ghim, đọc đúng ordinal của nguồn tương ứng, chuẩn hóa các trường đo được phép rồi so khớp giá trị, timestamp, sensor, cờ late và SHA-256. Không xuất payload raw.
4. Đọc Gold v{binding['gold_version']} và so toàn bộ 20 nhóm với `release_candidate/gold.json`; kiểm hash các object của candidate.
5. Kiểm tra version mới nhất của Silver/Gold trước và sau để xác nhận lần kiểm tra không ghi bảng.

Kết quả lúc `{result['completed_at_utc']}`: **{len(result['checks'])}/{len(result['checks'])} kiểm tra đạt**, ba trace khớp, **0 thao tác ghi Delta**. Manifest giữ trạng thái `CANDIDATE_NOT_OWNER_PUBLISHED`. Kết quả xác nhận binding và dữ liệu kỹ thuật; metadata reviewer không phải chữ ký hoặc chứng cứ trình bày cá nhân.

### 2.3. Sửa ví dụ R010001

Batch có hai revision cùng record, timestamp và object: **row 1 = 19,10°C**, **row 131 = 20,10°C**. Quy tắc `ingest_time_utc DESC, source_object ASC, source_row ASC` chọn row 1. Đổi thứ tự đọc vẫn cho cùng winner và winner khớp Silver v{binding['silver_version']}.

Không dùng `{{record_id: row}}` để chọn winner từ batch chứa nhiều revision: biểu thức đó giữ dòng gặp sau cùng, nên ở fixture này sẽ lấy row 131, trái hợp đồng. Việc đọc source row 1 chỉ có ý nghĩa khi đã chứng minh snapshot thực chọn provenance đó.

### 2.4. Kịch bản bảo vệ hai phút

| Thời gian | Nội dung |
| --- | --- |
| 0–20 giây | Chỉ manifest cấp riêng và nói rõ CSV fallback không phải release Parquet do nhóm phê duyệt. |
| 20–55 giây | Mở binding, xác định Silver v1/Gold v1 và code hash. |
| 55–95 giây | Chạy lệnh kiểm tra; mở trace R010001, chỉ source row 1, SHA-256 và giá trị 19,10°C trong snapshot. |
| 95–120 giây | Giải thích row 131 không thắng dù đứng sau trong file; nêu hạn chế container placement và candidate chưa owner-published. |

## Phần 3. Nội dung chuẩn bị trả lời exit questions

### 1. Vì sao no-op MERGE có thể tăng version?

Delta có thể ghi thêm một commit dù nội dung nghiệp vụ không đổi. Trong lượt này Silver v1 và replay v2 có cùng digest của các giá trị và provenance, nên replay có tính lũy đẳng. Không yêu cầu version hoặc byte Parquet phải đứng yên; cũng không khẳng định mọi no-op MERGE luôn tạo commit mới.

### 2. Vì sao range check chưa chứng minh độ chính xác đo lường?

Miền giá trị chỉ loại các giá trị vi phạm hợp đồng. Một nhiệt độ hợp lệ vẫn có thể lệch mẫu QA hoặc thiết bị tham chiếu. Bài giữ các bất đồng đo lường kế thừa; QA dùng kiểm chứng, không dùng để thay giá trị quan sát khi dựng Silver/Gold.

### 3. Vì sao replica phải tách khỏi Gold vận hành?

Silver sau cập nhật có **9.710** khóa; nhân 100 thành **971.000** dòng workload. Đây là các bản sao tương quan, không phải quan sát độc lập mới. Đưa chúng vào Gold sẽ phóng đại count, sum và ý nghĩa mẫu. Các bảng benchmark A/B được lưu riêng.

### 4. Vì sao manifest không phải giao dịch xuyên bảng?

Bronze, Silver và Gold commit riêng. Binding ghim một cặp phiên bản đã kiểm tra, còn manifest ghi đối tượng và hash để consumer chọn đúng kết quả. Chúng không đem lại atomic commit hoặc rollback đồng thời cho các bảng/đối tượng. Candidate hiện ghim Silver v1 và Gold v1 dù Silver latest đã có commit replay v2.

### 5. Vì sao S3 API chưa đủ chứng minh Delta tương thích?

Đọc/ghi object thành công chưa chứng minh backend đáp ứng giao thức LogStore và chế độ writer mà Delta đang sử dụng. Cần kiểm tra backend thực bằng các thao tác Delta và ghi rõ giới hạn. Bằng chứng của lượt này gồm schema rejection, merge, replay và time travel dưới chế độ single writer; không suy thành kết quả thử concurrent multi-driver.

## Bằng chứng

- [Dự đoán trước kiểm tra]({evidence('role_A_prediction.json')}).
- [16 kiểm tra A]({evidence('role_A_check.json')}).
- [Ba trace từ source tới snapshot]({evidence('role_A_source_trace.json')}).
- [Runtime của kiểm tra A]({evidence('runtime_role_A_check.json')}), [worker probe]({evidence('role_A_worker_probe.json')}).
- [Schema probe]({evidence('delta_schema_probe.json')}), [input integrity]({evidence('input_integrity.json')}).
- [Binding của candidate]({evidence('release_candidate/snapshot_binding.json')}), [state]({evidence('state.json')}).

Trước khi bảo vệ, thành viên cần đọc mã, thực hiện lệnh và giải thích dự đoán/kết quả bằng lời của mình.
'''
    filename = '24022484_TranAnhTuan_RoleA.md'
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
