"""Freeze the verified pipeline with the individual records for roles A and D."""
import csv
from datetime import datetime, timezone
from decimal import Decimal
import io
import json
import os
from pathlib import Path
import shutil
import sys
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from common import read_json, write_json, sha256_file


def table(headers, rows):
    return '| ' + ' | '.join(headers) + ' |\n| ' + ' | '.join('---' for _ in headers) + ' |\n' + ''.join(
        '| ' + ' | '.join(str(value).replace('|', '/') for value in row) + ' |\n' for row in rows)


def save_csv(path, headers, rows):
    text = io.StringIO(newline='')
    writer = csv.writer(text, lineterminator='\n')
    writer.writerow(headers)
    writer.writerows(rows)
    path.write_text(text.getvalue(), encoding='utf-8-sig')


def main():
    config = read_json(ROOT / 'config/team.json')
    reports = (ROOT / config.get('reports_dir', '.')).resolve()
    output_root = (ROOT / config.get('submission_dir', 'submission')).resolve()
    destination = output_root / 'LAB3_24022463_DamQuangTien_RoleD'
    if destination.exists() or destination.with_suffix('.zip').exists():
        raise ValueError('FROZEN_SUBMISSION_ALREADY_EXISTS')
    reports.mkdir(parents=True, exist_ok=True)
    (reports / 'individual').mkdir(exist_ok=True)
    code_prefix = Path(os.path.relpath(ROOT, reports)).as_posix()

    def report_text(value):
        if code_prefix != '.':
            for prefix in ('evidence/', 'sql/'):
                value = value.replace('](' + prefix, '](' + code_prefix + '/' + prefix)
        return value

    def packaged_text(value):
        if code_prefix != '.':
            value = value.replace('](' + code_prefix + '/', '](')
        return value
    ev = ROOT / config['evidence_dir']
    state = read_json(ev / 'state.json')
    independent = read_json(ev / 'independent_checks.json')
    live = read_json(ev / 'live_table_verification.json')
    tests = read_json(ev / 'implementation_tests.json')
    role = read_json(ev / 'role_D_check.json')
    role_a = read_json(ev / 'role_A_check.json')
    review = read_json(ev / 'role_A_E_review.json')
    if not all(item['passed'] for item in (independent, live, tests, role, role_a, review['A'], review['E'])):
        raise ValueError('PASSING_VERIFICATION_REQUIRED')
    if not (ROOT / 'portable_delta/snapshot_manifest.json').is_file():
        raise ValueError('PORTABLE_DELTA_EXPORT_REQUIRED')
    bench = read_json(ev / 'benchmark_report.json')
    scan = read_json(ev / 'benchmark_scan_metrics.json')
    runtime = read_json(ev / 'runtime_benchmark.json')
    summary = independent['summary']
    binding = read_json(ev / 'release_candidate/snapshot_binding.json')
    q2 = read_json(ev / 'Q2_weighted_day.json')['rows']
    q3 = read_json(ev / 'Q3_above25_rate.json')['rows']
    queries_dir = ROOT / 'sql'
    queries_dir.mkdir(exist_ok=True)
    for name in ('Q1_site_day', 'Q2_weighted_day', 'Q3_above25_rate'):
        query = read_json(ev / (name + '.json'))
        (queries_dir / (name + '.sql')).write_text(query['sql'] + ';\n', encoding='utf-8')
        fields = list(query['rows'][0])
        save_csv(ev / (name + '.csv'), fields, [[row[key] for key in fields] for row in query['rows']])
    (queries_dir / 'benchmark.sql').write_text("""-- Apply unchanged to either pinned A or B table.
SELECT site, COUNT(*) AS n_observations,
       CAST(SUM(temperature_c) AS DECIMAL(18,2)) AS sum_temperature_c,
       CAST(SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS BIGINT) AS n_late
FROM benchmark_layout
WHERE event_date = DATE '2026-02-06'
GROUP BY site;
""", encoding='utf-8')
    timing_rows = []
    for index, measured in enumerate(bench['measurements'], 1):
        execution = next(e for e in scan['executions'] if e.get('measurement_number') == index)
        files = [m['value'] for node in execution['scan_nodes'] for m in node['metrics'] if m['name'] == 'number of files read']
        timing_rows.append([index, measured['round'], measured['layout'], f"{measured['elapsed_seconds']:.9f}",
                            execution['sql_execution_id'], ', '.join(files), measured['result_equal']])
    save_csv(ev / 'benchmark_timings.csv', ['measurement', 'round', 'layout', 'elapsed_seconds',
             'sql_execution_id', 'actual_files_read', 'result_equal'], timing_rows)
    denominator_rows = [[r[k] for k in ('event_date', 'sum_temperature_c', 'n_observations',
                          'weighted_mean_c', 'naive_mean_of_means_c')] for r in role['daily_denominators']]
    version_rows = [[layer, state[layer + '_path'], state[layer + '_initial_version'],
                     state[layer + '_final_version']] for layer in ('bronze', 'silver', 'gold')]
    q3_ranges = []
    for day in sorted({row['event_date'] for row in q3}):
        rows = [row for row in q3 if row['event_date'] == day]
        q3_ranges.append([day, sum(r['n_above_25'] for r in rows), sum(r['n_observations'] for r in rows),
                          min(Decimal(r['above25_rate']) for r in rows), max(Decimal(r['above25_rate']) for r in rows)])
    report = f'''# Lab 3 — Lakehouse Processing & Analytics

Nhóm: `{state['group_id']}`. Lượt chạy: `{state['run_label']}`. Phần phụ trách: **Role D — 24022463 — Đàm Quang Tiến**.

## 1. Phạm vi và kết quả kiểm tra

Phần D gồm hàm `benchmark_query`, Q1/Q2/Q3, hai biểu đồ có phiên bản nguồn, thí nghiệm A/B và đối chiếu mẫu số của role B. Hồ sơ role A được bổ sung cho Trần Anh Tuấn (24022484), gồm kiểm tra đầu vào/runtime và truy vết trực tiếp snapshot đã ghim của E. Các hàm B/C/E được tích hợp để chạy chuỗi tạo dữ liệu nguồn và kiểm tra tính tái lập của kết quả D.

Đã chạy {len(independent['checks'])} kiểm tra giá trị xuất, {len(live['checks'])} kiểm tra trực tiếp bảng Delta, {tests['tests_run']} kiểm thử hàm không bỏ qua ca nào, {len(role['checks'])} kiểm tra role D và {len(role_a['checks'])} kiểm tra role A. Chi tiết nằm trong `evidence/independent_checks.json`, `live_table_verification.json`, `implementation_tests.json`, `role_D_check.json`, `role_A_check.json`. Các kết quả này xác nhận kiểm tra kỹ thuật bằng chương trình; phần trình bày cá nhân do thành viên thực hiện khi bảo vệ.

## 2. Đầu vào, runtime và snapshot

Sử dụng `course_fallback_not_student_approval`: CSV bàn giao gồm {summary['baseline_rows']:,} quan sát, đúng mười cột phát hành Lab 2; đây là fallback học phần. SHA-256 manifest cung cấp riêng: `{state['trusted_manifest_sha256']}`. Digest logic ban đầu: `{state['handoff_logical_sha256']}`. Cả 16 tệp đã qua kiểm tra byte/hash; các bản bị đổi xuống dòng được khôi phục từ ZIP gốc theo hash cố định, ghi tại `input_restoration.json`. Không sửa hash kỳ vọng.

Runtime: Python {runtime['python']}, Spark {runtime['spark']}, Delta {runtime['delta_spark']}, Java {runtime['java']}, Hadoop {runtime['hadoop']}; UTC; AQE={runtime['aqe']}; shuffle partitions={runtime['shuffle_partitions']}. Spark standalone dùng hai worker container và S3A dùng chung. Các container không chứng minh có hai máy chủ vật lý. Image ID, thư viện và JAR được lưu trong `container_images.txt`, `requirements.lock.txt`, `jars.lock.json`.

{table(['Lớp', 'URI cụ thể', 'Phiên bản đầu', 'Phiên bản sau cập nhật'], version_rows)}
Bronze là biên nhận vật lý theo `(source_object, source_row, source_sha256)`. Silver chọn một bản ghi theo `record_id`, giữ mười cột và thêm ngày UTC. Gold chứa {summary['gold_groups']} nhóm site/ngày từ {summary['silver_rows']:,} quan sát; tổng nhiệt độ {summary['temperature_sum_c']} °C. Trường nguồn gốc được giữ để truy vết; dữ liệu thô và trường hạn chế nằm ngoài gói nộp.

Batch 02 có {summary['incoming_physical_rows']} dòng vật lý, {summary['incoming_winners']} winner và {summary['superseded_incoming_rows']} phiên bản bị thay thế trong batch. Quy tắc chọn là ingest giảm dần, object tăng dần, ordinal tăng dần. Quyết định: `{summary['decisions']}`. Bronze replay không nhân biên nhận; Silver replay v{state['silver_replay_version']} bằng nội dung Silver v{state['silver_final_version']}; đọc lịch sử v0 vẫn bằng handoff. Ba batch sai bị từ chối trước ghi. Minh chứng đầy đủ ở `incremental.json`, bốn bản xuất Silver, `row_trace.json` và `input_contract_tests.json`.

## 3. Q1 — KPI theo site/ngày

Q1 trả 20 dòng, bốn site trên năm ngày UTC: `n_observations`, `mean_temperature_c`, `n_late`, `late_rate`. Mẫu số là mọi quan sát được giữ của đúng site/ngày; không dùng số sensor và không loại quan sát đến muộn. `is_late` chỉ đúng khi độ trễ **lớn hơn** 900 giây. Đọc bảng Gold v{state['gold_final_version']} tạo từ Silver v{state['gold_source_silver_version']} bằng `versionAsOf`.

Truy vấn: [Q1](sql/Q1_site_day.sql). Kết quả: [JSON](evidence/Q1_site_day.json), [CSV](evidence/Q1_site_day.csv).

## 4. Q2 — Trung bình có trọng số và kiểm tra role B

Trung bình ngày = `SUM(sum_temperature_c) / SUM(n_observations)`. `AVG(mean_temperature_c)` cho mỗi site cùng trọng số dù số quan sát khác nhau. Kiểm tra D tính lại tử số/mẫu số trực tiếp từ toàn bộ bản ghi Silver đã xuất bằng Decimal rồi so với Gold và Q2.

{table(['Ngày UTC', 'Tổng nhiệt độ (°C)', 'Số quan sát', 'Trung bình có trọng số (°C)', 'Trung bình các trung bình (°C)'], denominator_rows)}
Ngày 2026-02-02 minh họa rõ: 57283.50 / 2489 = 23.014665 °C; trung bình các site là 23.016459 °C. Sự khác nhau do số quan sát theo site không bằng nhau. Mẫu số gồm cả quan sát đến muộn. Số sensor phân biệt trong một site/ngày không cộng dồn thành số sensor phân biệt của toàn kỳ.

Truy vấn: [Q2](sql/Q2_weighted_day.sql). Kết quả: [JSON](evidence/Q2_weighted_day.json), [CSV](evidence/Q2_weighted_day.csv). Đối chiếu: [role D](evidence/role_D_check.json).

![Trung bình nhiệt độ có trọng số](evidence/daily_mean.png)

Biểu đồ ghi Silver/Gold, ngày UTC, đơn vị °C, tổng mẫu và số quan sát từng ngày. Ngày cuối chỉ có 100 quan sát, vì vậy không diễn giải biến động đó như xu thế tổng thể. Dữ liệu là fixture tổng hợp và vẫn có các bất đồng đo lường kế thừa với mẫu QA.

## 5. Q3 — Tỷ lệ vượt ngưỡng giảng dạy

Tử số là số nhiệt độ **> 25**, không gồm giá trị bằng 25. Mẫu số là toàn bộ quan sát site/ngày, kể cả đến muộn. Ngưỡng 25 °C phục vụ bài học; kết quả không phải kết luận an toàn hoặc hiệu chuẩn thiết bị. Bảng sau tóm tắt kết quả site/ngày, còn 20 dòng đầy đủ nằm trong tệp kết quả.

{table(['Ngày UTC', 'Số > 25 °C', 'Tổng quan sát', 'Tỷ lệ site thấp nhất', 'Tỷ lệ site cao nhất'], q3_ranges)}
Truy vấn: [Q3](sql/Q3_above25_rate.sql). Kết quả: [JSON](evidence/Q3_above25_rate.json), [CSV](evidence/Q3_above25_rate.csv).

![Tỷ lệ quan sát đến muộn](evidence/late_fraction.png)

Biểu đồ thể hiện `n_late / n_observations`, có chú giải site, số mẫu theo site và phiên bản nguồn. Qua kiểm tra miền giá trị không chứng minh đo lường chính xác; QA dùng để đánh giá, không thay giá trị quan sát.

## 6. Thí nghiệm bố trí A/B

Đọc cùng Silver v{bench['source_silver_version']} gồm {bench['source_rows']:,} dòng, nhân với 100 `replica_id` thành {bench['expanded_rows']:,} dòng tổng hợp. Chỉ thu về driver bốn dòng tổng hợp. Replica không phải mẫu độc lập và không đi vào Gold vận hành.

A không partition, ghi bằng `repartition(32)`. B ghi bằng `repartition(5, event_date)` và `partitionBy(event_date)`. Dùng cùng runtime, snapshot, tài nguyên, AQE và `benchmark_query`: lọc 2026-02-06, group theo site; xuất count, tổng DECIMAL(18,2), số late. `replica_id` không nằm trong GROUP BY.

{table(['Layout', 'Tổng file dữ liệu của bảng', 'Tổng byte', 'Thời gian chuẩn bị/ghi (s)'], [[label, detail['total_table_data_files'], detail['total_table_bytes'], f"{bench['setup_seconds_excluded_from_query_times'][label]:.6f}"] for label, detail in bench['layout_details'].items()])}
Mỗi layout warm-up một lần, sau đó đo A–B, B–A, A–B. Runner tạo DataFrame đọc snapshot trước mốc đồng hồ; khoảng đo bắt đầu khi dựng aggregate và kết thúc sau `collect()` của kết quả nhỏ, không chỉ đo dựng DataFrame. Thời gian ghi/chuẩn bị bảng nằm ngoài sáu phép đo.

{table(['Lần', 'Vòng', 'Layout', 'Thời gian driver (s)', 'SQL execution ID', 'File thực đọc', 'Kết quả bằng nhau'], timing_rows)}
Median A = **{bench['median_seconds']['A']:.9f} s**, median B = **{bench['median_seconds']['B']:.9f} s**, tỷ số median(A)/median(B) = **{bench['A_median_over_B_median']:.6f}**. Sáu kết quả bằng nhau và bằng kết quả dựng từ Gold ngày tương ứng nhân hệ số 100. Tổng đáp án là 10.000 dòng workload, 2.500 cho mỗi site; không diễn giải thành 10.000 phép đo độc lập.

Plan A không có mục `PartitionFilters`; điều kiện ngày xuất hiện ở `PushedFilters` và node `Filter`. Plan B có `PartitionFilters` chứa `isnotnull(event_date)` và `event_date = 2026-02-06`, cho phép bỏ qua partition ngày khác; `event_date` không cần nằm trong ReadSchema của dữ liệu Parquet ở B. **Tổng file của bảng** từ Delta metadata khác **file thực đọc** từ scan-node metrics của Spark History Server. `benchmark_scan_metrics.json` gắn metric với tám execution: hai warm-up và sáu lần đo. Không suy số file đọc từ tổng file.

Các plan lưu bằng formatted explain có `isFinalPlan=false`, mô tả kế hoạch trước kết quả AQE cuối; scan metrics được lấy riêng từ SQL execution đã hoàn tất. `clearCache()` không xóa cache hệ điều hành/object store/JVM: đây là đọc lặp sau warm-up. Độ nén, kích thước file và chi phí scheduling cũng khác giữa A/B; kết quả không tách riêng tác động của partition pruning. Tỷ số chỉ mô tả fixture và lượt chạy này; không suy rộng hiệu năng production. Không có yêu cầu speedup tối thiểu.

Minh chứng: [report](evidence/benchmark_report.json), [sáu thời gian](evidence/benchmark_timings.csv), [plan A](evidence/plan_A.txt), [plan B](evidence/plan_B.txt), [metric thực đọc](evidence/benchmark_scan_metrics.json).

## 7. Kiểm tra A/E và release candidate

A: 16/16 tệp, handoff logic, probe schema tám dòng và placement worker đều đạt trong lượt chạy hiện tại. Hồ sơ A được bổ sung bằng {len(role_a['checks'])} kiểm tra, trong đó ba mẫu được lấy từ Silver v1 thật, đối chiếu đúng source object/ordinal/hash và liên kết với binding Silver v1/Gold v1. R010001 chọn source row 1 ở 19,10°C; dictionary giữ dòng gặp cuối sẽ lấy row 131 ở 20,10°C nên không được dùng làm quy tắc winner. Các lỗi byte và Java ngoài baseline của bản báo cáo cũ đã được xử lý trong cấu hình/bằng chứng mới. E: một candidate hợp lệ được nhận; mixed snapshot, tự review, thiếu steward và sai code hash bị từ chối. Kiểm tra bổ sung xác nhận từng khóa bắt buộc bị thiếu đều từ chối.

Candidate ghim Silver v{binding['silver_version']}, Gold v{binding['gold_version']}, nguồn Gold là Silver v{binding['gold_source_silver_version']}. Gold được tính lại từ bảng Silver đã ghim rồi so toàn bộ trường. SHA-256 implementation: `{binding['code_sha256']}`; bản đã thực thi nằm trong `code_versions/`. Catalog, manifest và lineage đi kèm candidate. Trạng thái là **CANDIDATE_NOT_OWNER_PUBLISHED**; nhãn reviewer trong metadata không thay chữ ký hoặc phần bảo vệ của thành viên.

Mỗi lần ghi Delta là một giao dịch của một bảng. Cặp version và manifest giúp chọn đầu ra đã kiểm tra; chúng không tạo giao dịch xuyên bảng. Giữ các file/log còn được tham chiếu, không VACUUM. Bản Delta mang theo trong `portable_delta/` giữ cả log và dữ liệu để kiểm tra lịch sử khi tái lập.

## 8. Danh mục nộp và tái lập

- [Hồ sơ role A](individual/24022484_TranAnhTuan_RoleA.md), [hồ sơ role D](individual/24022463_DamQuangTien_RoleD.md), [kiểm tra A/E](KIEM_TRA_ROLE_A_E.md).
- `templates/student_impl.py`, `sql/`, `contributions.csv`, `manual_checks.csv`.
- `evidence/`: phiên bản, snapshot, SQL, biểu đồ, timing, plan, metric, lineage, catalog, candidate và kiểm tra.
- `portable_delta/`: bản sao đúng byte của các object Delta được tham chiếu.
- `Setup-Lab3.ps1`, `Run-Lab3.ps1`: chuẩn bị môi trường và chạy tuần tự. README mô tả khôi phục dữ liệu gốc.

Tài liệu kỹ thuật: [Spark 4.0.1 performance tuning](https://spark.apache.org/docs/4.0.1/sql-performance-tuning.html), [Delta MERGE](https://docs.delta.io/delta-update/).
'''
    (reports / 'experiment_report.md').write_text(report_text(report), encoding='utf-8')
    individual = f'''# Role D — 24022463 — Đàm Quang Tiến

Nhóm `{state['group_id']}`, run `{state['run_label']}`. Trọng tâm: `benchmark_query`, SQL, biểu đồ, đo bố trí A/B và kiểm tra mẫu số của role B.

## Đóng góp kỹ thuật

`templates/student_impl.py:benchmark_query` lọc literal DATE 2026-02-06 rồi group theo site, COUNT mọi dòng, SUM nhiệt độ DECIMAL(18,2) và SUM cờ late BIGINT. Hàm trả DataFrame và giữ phép tính trong Spark. Runner dùng cùng hàm cho A/B và buộc thực thi bằng collect kết quả nhỏ. `tools/analytics_and_dashboard.py` ghim snapshot Gold, ghi SQL/phiên bản và tạo hai biểu đồ có UTC, số mẫu, đơn vị và hạn chế QA. `tools/check_role_d.py` tính lại bằng Decimal để kiểm tra role B.

## Kiểm tra chéo B: dự đoán, chạy, giải thích

Dự đoán được ghi trong `evidence/role_D_prediction.json` trước khi kiểm tra. Tổng count của Gold phải bằng {summary['silver_rows']:,} dòng Silver và tổng nhiệt độ phải bằng {summary['temperature_sum_c']} °C. Trung bình ngày phải dùng tổng số quan sát làm mẫu số. Bảng so sánh thực tế:

{table(['Ngày', 'Tử số °C', 'Mẫu số', 'Mean có trọng số °C', 'Mean các mean °C'], denominator_rows)}
Chạy từ thư mục dự án với dịch vụ đang hoạt động:

```powershell
docker compose exec -T driver python tools/check_role_d.py --config config/team.json
```

Kết quả: {len(role['checks'])}/{len(role['checks'])} kiểm tra đạt; chi tiết trong `evidence/role_D_check.json`. Đây là kiểm tra chương trình đọc bằng chứng, không ghi bảng hoặc sửa sáu thời gian đo. Ngày 02-02 có count theo site khác nhau nên mean các mean khác mean trọng số; quan sát đến muộn vẫn nằm trong mẫu số.

## Kịch bản trình bày hai phút

| Thời gian | Nội dung |
| --- | --- |
| 0–20 giây | Nêu dự đoán B có predicate partition ngày, mọi đáp án A/B phải bằng nhau. |
| 20–55 giây | Mở plan_A.txt và plan_B.txt; chỉ `PartitionFilters`, so total_table_data_files với scan metrics theo SQL execution ID. |
| 55–85 giây | Mở benchmark_timings.csv: đủ A–B/B–A/A–B; đọc một thời gian thật và median A={bench['median_seconds']['A']:.6f}, B={bench['median_seconds']['B']:.6f}. |
| 85–120 giây | Giải thích collect buộc đánh giá, warm cache và chi phí ghi tách riêng; nêu phép chia 57283.50/2489 của B và lý do không AVG các mean. |

Sáu phép đo thực tế:

{table(['Lần', 'Vòng', 'Layout', 'Giây driver', 'SQL ID', 'File đọc', 'Bằng nhau'], timing_rows)}

## Nội dung chuẩn bị trả lời exit questions

1. **No-op merge vẫn có version mới:** Silver v{state['silver_final_version']} và replay v{state['silver_replay_version']} có cùng digest của giá trị và nguồn gốc. Idempotence là nội dung logic không đổi; history có thể ghi thêm giao dịch.
2. **Qua range check chưa chắc đo đúng:** giá trị hợp lệ vẫn có thể lệch mẫu QA. Hai biểu đồ giữ cảnh báo chất lượng; ngày cuối chỉ n=100, không kết luận xu thế hoặc ngưỡng an toàn.
3. **Replica không vào Gold:** {bench['expanded_rows']:,} dòng benchmark là 100 bản sao của {summary['silver_rows']:,} khóa. Đưa vào Gold sẽ phóng đại count và bằng chứng mẫu; các bản sao không độc lập.
4. **Manifest không phải giao dịch xuyên bảng:** Bronze/Silver/Gold commit riêng; manifest ghi cặp phiên bản đã xác minh sau khi ghi, không đem lại rollback hoặc atomic commit cho cả chuỗi.
5. **S3 API chưa đủ:** thành công PUT/GET không chứng minh hành vi Delta log-store. Cần kiểm tra backend thật bằng schema/merge/replay/time travel và giữ giới hạn single writer.

## Mốc bằng chứng

- `evidence/state.json`: Silver v0/v{state['silver_final_version']}/v{state['silver_replay_version']}, Gold v{state['gold_final_version']}.
- `evidence/code_versions/{binding['code_sha256']}.py`: implementation thực thi.
- `evidence/benchmark_report.json`, `plan_A.txt`, `plan_B.txt`, `benchmark_scan_metrics.json`.
- `evidence/role_D_prediction.json`, `role_D_check.json`: dự đoán và kết quả kiểm tra B.
- `evidence/release_gate_tests.json`: ca release bị từ chối.

Tài liệu này dùng để học và trình bày phần kỹ thuật; các log chỉ ghi nhận lần kiểm tra bằng chương trình.
'''
    (reports / 'individual/24022463_DamQuangTien_RoleD.md').write_text(individual, encoding='utf-8')
    ae = '''# Kiểm tra kỹ thuật role A và E

## Role A

Bản hiện có đã có bằng chứng preflight, nhưng báo cáo cũ dùng Java 23 ngoài baseline Java 17/21 và chứa cấu hình đường dẫn không dùng chung với executor. Các tệp đầu vào hiện tại cũng bị đổi xuống dòng làm sai hash. Đã giữ bản trước, khôi phục 11 tệp đúng byte từ ZIP gốc, giữ nguyên manifest tin cậy và chạy lại toàn bộ kiểm tra.

Lượt chạy mới: 16/16 hash đạt; fallback được khai báo đúng, digest logic khớp; Delta từ chối cột lạ và giữ tám dòng; runtime đúng Spark/Delta 4.0.1, Java 17; task chạy trên hai worker container, bảng dùng S3A chung. Container không xác nhận nhiều máy vật lý. Hồ sơ `individual/24022484_TranAnhTuan_RoleA.md` đã được cập nhật bằng kiểm tra trực tiếp Silver v1 và Gold v1. Ba mẫu truy được về đúng object/ordinal/hash; R010001 chọn row 1 ở 19,10°C, không dùng dòng cuối của dictionary. Bằng chứng bổ sung nằm trong `role_A_check.json`, `role_A_source_trace.json`, `runtime_role_A_check.json`, `role_A_worker_probe.json`.

## Role E

Trong thư mục ban đầu, `validate_release_binding` vẫn là TODO và chưa có state/candidate để kiểm tra. Đã tích hợp hàm có sẵn từ bộ bài tập khác và kiểm tra lại bằng kết quả của lượt chạy mới. Năm gate bắt buộc đạt đúng kỳ vọng: valid nhận; mixed_version, self_review, missing_steward, wrong_code từ chối. Bổ sung thử thiếu từng khóa bắt buộc; tất cả từ chối, không lỗi thực thi.

Gold được tái tính từ snapshot Silver ghim và so toàn bộ trường, hash candidate khớp manifest, code đã thực thi được lưu theo hash; catalog và lineage có trong `evidence/release_candidate/` và `lineage.jsonl`. `evidence/role_A_E_review.json` chứa kết quả. Trạng thái là candidate, chưa owner-published; phần review/bảo vệ cá nhân vẫn do thành viên trình bày.
'''
    (reports / 'KIEM_TRA_ROLE_A_E.md').write_text(ae, encoding='utf-8')
    save_csv(reports / 'contributions.csv', ['member_id','role','authored_file','change_description','test_evidence',
             'independent_cross_check','exit_response_file'], [['24022463','D',
             'templates/student_impl.py:benchmark_query|tools/analytics_and_dashboard.py|sql/|tools/check_role_d.py',
             'SQL analytics; two version-labelled plots; six measured layout evaluations; weighted-denominator check',
             'evidence/role_D_check.json|evidence/benchmark_report.json|evidence/implementation_tests.json',
             'Role B weighted daily mean and observation denominator','individual/24022463_DamQuangTien_RoleD.md']])
    save_csv(reports / 'manual_checks.csv', ['check_id','operator','predicted_outcome','observed_outcome','evidence_location','explanation'],
             [['D_CHECK_B','tools/check_role_d.py','Gold count/sum agree with Silver; use observation-weighted mean',
               f"PASS: {len(role['checks'])} checks",'evidence/role_D_prediction.json|evidence/role_D_check.json',
               'Prediction recorded before assertion evaluation; automated read-only execution; no personal attendance claim']])
    from role_a_report import build_role_a_record
    role_a_filename = build_role_a_record(ROOT, reports, ev)
    readme = '''# Lab 3 — Hồ sơ role A và role D

Gói gồm hai hồ sơ cá nhân: role A — Trần Anh Tuấn (24022484) và role D — Đàm Quang Tiến (24022463). Mở `experiment_report.md` và `individual/` để xem nội dung, kiểm tra chéo và hướng dẫn bảo vệ. Role E có mã và bằng chứng kỹ thuật trong pipeline; chưa có hồ sơ cá nhân role E trong gói này. `KIEM_TRA_ROLE_A_E.md` ghi kết quả kiểm tra A/E. `evidence/` chứa các đầu ra thật của lượt chạy; `portable_delta/` giữ log và dữ liệu của các bảng Delta vận hành, A/B benchmark và schema probe. Bảng benchmark tách khỏi Gold.

## Tái lập từ đầu

Giữ ZIP nộp làm bản đóng băng. Giải nén một bản làm việc mới, đặt nguyên thư mục `data/` từ ZIP starter gốc vào dự án, đặt manifest tin cậy do giảng viên cấp ở thư mục cha. Các dữ liệu gốc được phép không đính kèm gói nộp. Mở Docker Desktop rồi chạy:

```powershell
powershell -ExecutionPolicy Bypass -File .\Setup-Lab3.ps1 -Build
```

Đổi cả `run_label` và `evidence_dir` trong `config/team.json` sang tên mới, sau đó chạy:

```powershell
powershell -ExecutionPolicy Bypass -File .\Run-Lab3.ps1 -Step all
```

Chỉ dùng một driver ghi bảng tại một thời điểm. Không sửa hash, không dùng lại prefix bootstrap đã có, không VACUUM.

## Đọc lại đúng các snapshot đã nộp

Trên object store mới, giữ config và evidence bản nộp, khởi động dịch vụ bằng `Run-Lab3.ps1 -Step start`. Khôi phục dữ liệu nguồn `data/` đúng byte và cung cấp trusted manifest như trên. Sau đó:

```powershell
docker compose exec -T driver python tools/portable_delta.py verify
docker compose exec -T driver python tools/portable_delta.py restore
docker compose exec -T driver python tools/verify_live_tables.py --config config/team.json
```

Restore chỉ nhận các prefix bảng trống. File timings giữ nguyên kết quả đã đo; chạy lại benchmark sẽ tạo các thời gian mới. File scan metrics đã xuất vẫn gắn với SQL execution ID của lần nộp; Spark History Server của môi trường mới chỉ có event log từ các lần chạy mới.

`.env` được setup tạo riêng và bị loại khỏi gói nộp. Không cần cài Python/Java/Spark trực tiếp trên Windows; image dựng môi trường Linux và ghim thư viện/JAR. Bản nộp là technical release candidate; công bố owner và trình bày cá nhân là các hoạt động riêng.
'''
    (reports / 'README_SUBMISSION.md').write_text(readme, encoding='utf-8')
    for name in ('experiment_report.md', 'contributions.csv', 'manual_checks.csv'):
        if name.endswith('.md'):
            template = packaged_text((reports / name).read_text(encoding='utf-8'))
            for prefix in ('evidence/', 'sql/', 'individual/'):
                template = template.replace('](' + prefix, '](../' + prefix)
            template = template.replace('](KIEM_TRA_ROLE_A_E.md)', '](../KIEM_TRA_ROLE_A_E.md)')
            (ROOT / 'templates' / name).write_text(template, encoding='utf-8')
        else:
            shutil.copyfile(reports / name, ROOT / 'templates' / name)
    # Preserve exact generating sources for each delivered stage and verification tool.
    source_files = sorted((ROOT / 'src').glob('*.py')) + sorted((ROOT / 'tools').glob('*.py'))
    code_inventory = []
    for path in source_files:
        digest = sha256_file(path)
        (ev / 'code_versions' / (digest + '.py')).write_bytes(path.read_bytes())
        code_inventory.append({'file': path.relative_to(ROOT).as_posix(), 'sha256': digest})
    write_json(ev / 'generating_code_manifest.json', {'files': code_inventory,
        'student_impl_sha256': binding['code_sha256']})
    destination.mkdir(parents=True)
    for folder in ('src','templates','tools','tests','config','schemas','manifests','infrastructure','sql','portable_delta'):
        shutil.copytree(ROOT / folder, destination / folder, ignore=shutil.ignore_patterns('__pycache__','*.pyc','.env'))
    shutil.copytree(ev, destination / 'evidence', ignore=shutil.ignore_patterns('__pycache__','*.pyc'))
    (destination / 'individual').mkdir()
    shutil.copyfile(reports / 'individual/24022463_DamQuangTien_RoleD.md', destination / 'individual/24022463_DamQuangTien_RoleD.md')
    role_a_text = (reports / 'individual' / role_a_filename).read_text(encoding='utf-8')
    individual_prefix = Path(os.path.relpath(ROOT, reports / 'individual')).as_posix()
    role_a_text = role_a_text.replace('](' + individual_prefix + '/', '](../')
    (destination / 'individual' / role_a_filename).write_text(role_a_text, encoding='utf-8')
    for name in ('experiment_report.md','KIEM_TRA_ROLE_A_E.md'):
        (destination / name).write_text(packaged_text((reports / name).read_text(encoding='utf-8')), encoding='utf-8')
    for name in ('contributions.csv','manual_checks.csv'):
        shutil.copyfile(reports / name, destination / name)
    for name in ('team.json','requirements.txt','compose.yaml','.dockerignore','.gitattributes','.gitignore','Setup-Lab3.ps1','Run-Lab3.ps1'):
        shutil.copyfile(ROOT / name, destination / name)
    packaged_config = {key: value for key, value in config.items()
                       if key not in ('reports_dir', 'submission_dir', 'archive_dir')}
    packaged_config['evidence_dir'] = 'evidence'
    write_json(destination / 'config/team.json', packaged_config)
    compose = (destination / 'compose.yaml').read_text(encoding='utf-8')
    compose = compose.replace('working_dir: /workspace/03_DU_AN', 'working_dir: /workspace')
    compose = compose.replace('- ..:/workspace', '- .:/workspace')
    compose = compose.replace('- ../../trusted_lab3_inputs.json:', '- ../trusted_lab3_inputs.json:')
    (destination / 'compose.yaml').write_text(compose, encoding='utf-8')
    shutil.copyfile(reports / 'README_SUBMISSION.md', destination / 'README.md')
    shutil.copyfile(ROOT / 'templates/student_impl.py', destination / 'student_impl.py')
    write_json(destination / 'source_references.json', {'original_dataset_zip': 'Lab3-Student-Starter.zip',
        'required_restore_directory': 'data/', 'trusted_manifest_sha256': state['trusted_manifest_sha256'],
        'handoff_kind': state['handoff_kind'], 'data_files_omitted': True,
        'manifest_source': 'Separately supplied instructor manifest, mounted read-only by Compose.'})
    inventory = [{'file': p.relative_to(destination).as_posix(), 'bytes': p.stat().st_size, 'sha256': sha256_file(p)}
                 for p in sorted(destination.rglob('*')) if p.is_file()]
    write_json(destination / 'submission_manifest.json', {'created_at_utc': datetime.now(timezone.utc).isoformat(),
        'member_id': '24022463', 'role': 'D', 'group_id': state['group_id'], 'run_label': state['run_label'],
        'roles_with_individual_records': ['A', 'D'],
        'individual_members': [{'role': 'A', 'member_id': '24022484'}, {'role': 'D', 'member_id': '24022463'}],
        'files': inventory, 'technical_checks_passed': True, 'release_status': 'CANDIDATE_NOT_OWNER_PUBLISHED',
        'excluded': ['credentials/.env','raw payloads and source data/','previous-run archive','Python caches']})
    archive = destination.with_suffix('.zip')
    with zipfile.ZipFile(archive, 'w', zipfile.ZIP_DEFLATED, compresslevel=9) as package:
        for path in sorted(destination.rglob('*')):
            if path.is_file():
                package.write(path, path.relative_to(destination.parent).as_posix())
    digest = sha256_file(archive)
    archive.with_suffix('.zip.sha256').write_text(digest + '  ' + archive.name + '\n', encoding='ascii')
    print('Prepared', archive.name, 'SHA256', digest)


if __name__ == '__main__':
    main()
