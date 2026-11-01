# BÁO CÁO CÁ NHÂN — INDIVIDUAL ASSESSMENT

**Học phần:** Cloud-Native Big Data Infrastructure & Governance  
**Bài thực hành:** Lab 3 — Lakehouse Processing & Analytics  
**Package Revision:** 18 September 2026

| Thông tin | Nội dung |
|---|---|
| Họ và tên | Trần Anh Tuấn |
| MSSV | 24022484 |
| Vai trò | Role A — Data Ingress & Runtime Acceptance |
| Nhiệm vụ chính | Task 1 — Accept and verify the Lab 2 handoff, runtime configuration, schema enforcement & execution placement |
| Nhiệm vụ chéo | Cross-check E: Reproduce Member E's source-to-snapshot trace |
| Nhóm thực hiện | `bd-g10` |
| Context / Chế độ chạy | Spark standalone: hai worker container, S3A dùng chung (Spark {{SPARK}}, Delta-Spark {{DELTA}}, Python {{PYTHON}}, Java {{JAVA}}, Hadoop {{HADOOP}}) |
| Trusted Manifest SHA-256 | `3cdfc84b26d290100b39ce65d4ff7cb22839f4021b9caf26897c8a4ff68091a2` |
| Upstream Logical SHA-256 | `c50f8252bfb91890975d26b16fe1d2b32f0e283a58e46bd16260b4a3cca0bf0d` |
| Run ID Preflight | `{{RUN_ID}}` |
| Cặp snapshot kiểm tra chéo E | Silver v{{SILVER_VERSION}} / Gold v{{GOLD_VERSION}} |

---

## Phần 1: Đóng góp kỹ thuật (10 điểm)

### 1.1 Trước khi thực hiện (Bối cảnh và thách thức)

Sau khi hoàn thành Lab 2, nhóm nghiên cứu đã xây dựng được một tập dữ liệu đo đạc nhiệt độ được chuẩn hóa và tinh lọc (`curated dataset`). Tuy nhiên, việc chuyển tiếp dữ liệu sang kiến trúc Lakehouse (Lab 3) đặt ra những thách thức kỹ thuật nghiêm ngặt về tính tin cậy và ranh giới dữ liệu:
1. **Bảo toàn hợp đồng dữ liệu thượng nguồn (Data Contract Integrity):** Không được tự ý thay đổi dữ liệu gốc hay âm thầm sửa các điểm bất đồng đo đạc còn tồn đọng từ Lab 2. Cần phải kiểm chứng toàn bộ 16 tệp artifact đầu vào và lược đồ (schema) theo mã băm tin cậy được phân phối qua kênh độc lập từ giảng viên (`trusted_lab3_inputs.json`).
2. **Minh bạch về bản chất bàn giao (Logical Handoff Identity):** Nếu nhóm sử dụng tệp chuyển giao dự phòng chuẩn của môn học (`data/handoff/curated.csv`), phải ghi nhận trung thực đây là bản fallback logic dạng CSV transport (9.610 dòng, 10 cột), không được mạo nhận đây là bản Parquet đã ký số phát hành của nhóm mình.
3. **Thực thi ràng buộc lược đồ tầng lưu trữ (Delta Schema Enforcement):** Đảm bảo engine Delta Lake có khả năng tự động từ chối ngay lập tức các bản ghi vi phạm cấu trúc (thừa cột, sai kiểu) ở cấp độ phân tích (AnalysisException) mà không làm ô nhiễm bảng dữ liệu hiện có.
4. **Môi trường thực thi và ghi nhận ranh giới (Execution Placement):** Thiết lập môi trường PySpark 4.0.1 và Delta Lake 4.0.1 trên nền tảng Python 3.11, Java 17 và Hadoop 3.4.1. Phải ghi nhận trung thực tác vụ trên hai worker container; không mạo nhận đây là bằng chứng về hai máy chủ vật lý.

Vai trò của tôi (Trần Anh Tuấn — Thành viên A) là kiểm soát toàn bộ cổng tiếp nhận đầu vào (Ingress Gatekeeper), xác minh mã băm, kiểm định tính toàn vẹn của tệp chuyển giao, cấu hình và tinh chỉnh runtime Spark/Delta, thực hiện probe thực thi worker và probe từ chối schema của Delta Lake.

---

### 1.2 Công việc kỹ thuật đã thực hiện

#### 1. Kiểm tra tính toàn vẹn 16 Artifacts đầu vào theo Trusted Manifest
- **Kịch bản kiểm tra độc lập (`tools/check_inputs.py`):**
  Công cụ kiểm tra sử dụng thư viện chuẩn của Python (`argparse`, `hashlib`, `json`, `csv`, `decimal`), hoàn toàn không phụ thuộc vào Spark hay cụm đám mây để kiểm tra 16 tệp dữ liệu và schema khai báo trong `trusted_lab3_inputs.json`.
  ```powershell
  docker compose exec -T driver python tools/check_inputs.py --trusted-manifest /instructor/trusted_lab3_inputs.json
  ```
  Kết quả thực thi:
  ```text
  PASS: all manifest bytes/hashes and transport contracts match.
  Handoff rows: 9610; incoming physical revisions: 140.
  This is an input check, NOT a Spark/Delta/Kubernetes execution test.
  ```
- **Xác thực toàn bộ 16 tệp:** Ghi nhận đầy đủ vào [`evidence/input_integrity.json`](../../evidence/input_integrity.json). Mã băm của bản kê tin cậy đạt chuẩn tuyệt đối: `3cdfc84b26d290100b39ce65d4ff7cb22839f4021b9caf26897c8a4ff68091a2`.

#### 2. Xác thực tính đồng nhất logic của Handoff (Logical Handoff Identity)
- Đọc và thẩm định tệp `data/handoff/handoff_manifest.json`:
  - `handoff_id`: `lab2-research-telemetry-v1-reference-handoff`
  - `handoff_kind`: `course_fallback_not_student_approval`
  - `row_count`: 9.610 bản ghi
  - `data_sha256`: `5770802a599a2931751f5ccd41e0305a03c98a08bdf5504ac8df0d1f990d8c1d`
  - `logical_sha256`: `c50f8252bfb91890975d26b16fe1d2b32f0e283a58e46bd16260b4a3cca0bf0d`
- Kiểm tra toàn bộ 9.610 dòng của `curated.csv` đối chiếu với danh bạ cảm biến (`data/handoff/sensors.csv`), bảo toàn 100% ánh xạ `sensor_id` $\rightarrow$ `site`. Mã băm logic được tính toán theo biểu diễn chính quy (canonical text JSON Lines sắp xếp theo `record_id`) khớp chính xác 100% với `c50f8252...`.
- Xác minh truy vết nguồn gốc dòng (`check_provenance`): 100% bản ghi có `source_object` là `observations_a.csv` hoặc `observations_b.jsonl` đều khớp mã băm gốc của tệp nguồn Lab 2 (`51574189...` và `3072ad0b...`).

#### 3. Cấu hình Runtime Spark 4.0.1 / Delta Lake 4.0.1
Để chuẩn bị môi trường chạy cho cả nhóm, cấu hình runtime được cập nhật theo baseline của bài thực hành:
- **Tương thích Java 17:** Sử dụng Python 3.11, Java 17, Spark 4.0.1 và Delta-Spark 4.0.1; phiên bản thực tế được ghi nhận trong [`evidence/runtime_preflight.json`](../../evidence/runtime_preflight.json).
- **Tích hợp Hadoop/S3A:** Connector Hadoop-AWS khớp Hadoop 3.4.1 đi kèm Spark. Các JAR đã chuẩn bị và mã băm được ghi trong `evidence/jars.lock.json`.
- **Đường dẫn dùng chung:** Bronze, Silver và Gold sử dụng URI `s3a://` để driver và executor cùng truy cập đúng dữ liệu.
- **Cấu hình Spark:** [`src/session.py`](../../src/session.py) giữ UTC, ANSI mode, AQE và tắt tự động mở rộng schema. Chỉ chạy một driver ghi bảng tại một thời điểm.
- **Định danh Python Worker:** Driver và worker sử dụng cùng môi trường Python 3.11 trong image đã chuẩn bị; các phiên bản thư viện và image ID được lưu cùng bằng chứng.

#### 4. Thử nghiệm cơ chế từ chối Schema tự nhiên của Delta Lake (Delta Schema Probe)
- Tại bước Preflight, runner tạo một bảng Delta cô lập tạm thời chứa 8 hàng (`smoke = root + '/preflight-' + run_id`).
- Thử nghiệm chèn một DataFrame đột biến chứa cột lạ `unexpected` (`spark.range(1).withColumn('unexpected', lit('x'))`).
- Delta Lake kích hoạt cơ chế thực thi lược đồ tự nhiên (`AnalysisException: A schema mismatch detected when writing to the Delta table`). Thao tác ghi bị từ chối triệt để.
- Đọc lại bảng sau khi từ chối: số hàng vẫn bảo toàn chính xác **8 hàng**, không có bất kỳ hàng nào bị ghi đè hay hỏng hóc. Kết quả được lưu tại [`evidence/delta_schema_probe.json`](../../evidence/delta_schema_probe.json).

#### 5. Thăm dò và phân bổ thực thi Worker (Worker Probe)
- Chạy tác vụ phân tán song song trên 24 phân vùng (`spark.sparkContext.parallelize(range(24), 24).mapPartitions(f).collect()`).
- Thu thập danh sách `host`, `pid` và `partition` của từng task, ghi nhận vào [`evidence/worker_probe.json`](../../evidence/worker_probe.json).
- Kết quả quan sát: `distinct_hosts = 2`, tác vụ được phân bổ trên hai worker container (`spark-worker-1`, `spark-worker-2`).
- Báo cáo ghi nhận trung thực: Đây là Spark standalone với hai worker container và S3A dùng chung; container placement không chứng minh có hai máy chủ vật lý.

#### 6. Thử nghiệm cách ly các ca đột biến hợp đồng dữ liệu (Input Contract Probes)
- Chạy 3 ca thử nghiệm đột biến dữ liệu trong bộ nhớ (không ghi vào Delta table) trên lô dữ liệu bổ sung (`incoming batch-02`), kết quả ghi nhận tại [`evidence/input_contract_tests.json`](../../evidence/input_contract_tests.json):
  - `VALUE`: Sửa `temperature_c = '100.00'` $\rightarrow$ Bị từ chối với mã lỗi `VALUE` (vượt ngưỡng $[-30, 60]^\circ\text{C}$).
  - `REQUIRED_VALUE`: Sửa `temperature_c = None` $\rightarrow$ Bị từ chối với mã lỗi `VALUE` (thiếu giá trị bắt buộc).
   - `SCHEMA_ALLOWLIST`: Thêm trường email bị hạn chế `operator_email` $\rightarrow$ Bị từ chối với mã lỗi `SCHEMA_ALLOWLIST` (vi phạm danh sách 10 cột cho phép).
  - Cả 3 ca đều có `writes_performed: 0`, xác nhận ba ca này đều bị từ chối trước ghi theo chính sách "Reject before Write".

---

### 1.3 Kết quả kiểm định các tiêu chí H01–H04

Bảng tổng hợp kết quả kiểm tra Task 1:

| Mã kiểm tra | Tên kiểm tra | Điều kiện bắt buộc | Kết quả đo lường thực tế | Trạng thái |
|:---:|---|---|---|:---:|
| **H01** | Input Identity & Released Schema | Cả 16 artifact đầu vào và schema khớp tuyệt đối mã băm trong trusted manifest; đúng 10 cột dữ liệu; đúng kiểu dữ liệu. | Đã xác minh 16/16 tệp khớp SHA-256; `curated.csv` chứa đúng 10 cột allowlist; kiểu dữ liệu timestamp, decimal(8,2), boolean, string, bigint được xác thực hợp lệ. | **PASS** |
| **H02** | Logical Handoff Identity | Tài liệu hóa trung thực nguồn bàn giao (team Parquet hoặc fallback CSV); bảo toàn nguồn gốc và mã băm logic; không thay thế ngầm. | Ghi nhận rõ ràng sử dụng course fallback `data/handoff/curated.csv` (9.610 dòng); mã băm logic `c50f8252...` khớp 100% với `handoff_manifest.json`; nguồn gốc bản ghi giữ nguyên. | **PASS** |
| **H03** | Native Delta Schema Rejection | Thử nghiệm cô lập từ chối cột bất thường dưới dạng lỗi schema analysis; 8 hàng ban đầu còn nguyên vẹn. | Delta Lake ném ngoại lệ `AnalysisException` khi cố tình chèn cột `unexpected`; số dòng trong bảng sau đó vẫn giữ nguyên đúng 8 hàng. | **PASS** |
| **H04** | Runtime & Execution Placement | Thu thập phiên bản Spark, Delta, Java, Hadoop, Application ID và worker; ghi nhận đúng phạm vi thực thi. | Spark {{SPARK}}, Delta-Spark {{DELTA}}, Python {{PYTHON}}, Java {{JAVA}}, Hadoop {{HADOOP}}; Application ID `{{APP_ID}}`; S3A dùng chung và hai worker container; không suy thành hai máy chủ vật lý. | **PASS** |

**Các tệp bằng chứng kỹ thuật nộp kèm:**
- [`evidence/input_integrity.json`](../../evidence/input_integrity.json) — Biên bản xác thực 16 tệp theo `trusted_lab3_inputs.json`.
- [`evidence/preflight.json`](../../evidence/preflight.json) — Trạng thái PASS của preflight, số dòng baseline (9.610) và batch mới (140).
- [`evidence/delta_schema_probe.json`](../../evidence/delta_schema_probe.json) — Bằng chứng từ chối chèn schema lạ và bảo toàn 8 dòng.
- [`evidence/worker_probe.json`](../../evidence/worker_probe.json) — Nhật ký phân bổ tác vụ trên 24 phân vùng và ghi nhận host thực thi.
- [`evidence/runtime_preflight.json`](../../evidence/runtime_preflight.json) — Cấu hình phiên bản runtime và ranh giới hệ thống.
- [`evidence/run_preflight.json`](../../evidence/run_preflight.json) — Trạng thái hoàn thành COMPLETE của job preflight.
- [`evidence/lineage.jsonl`](../../evidence/lineage.jsonl) — Sự kiện OpenLineage ghi nhận bắt đầu và kết thúc tiến trình.
- [`evidence/role_A_prediction.json`](../../evidence/role_A_prediction.json), [`role_A_check.json`](../../evidence/role_A_check.json) — Dự đoán trước kiểm tra và {{N_CHECKS}} assertion thực tế.
- [`evidence/role_A_source_trace.json`](../../evidence/role_A_source_trace.json) — Ba mẫu từ snapshot đã ghim về đúng nguồn/ordinal/hash.
- [`snapshot_binding.json`](../../evidence/release_candidate/snapshot_binding.json) — Cặp phiên bản của candidate do E kiểm tra.
- [`runtime_role_A_check.json`](../../evidence/runtime_role_A_check.json), [`role_A_worker_probe.json`](../../evidence/role_A_worker_probe.json) — Runtime và placement của lần đối chiếu bổ sung.

---

## Phần 2: Kiểm tra chéo độc lập (10 điểm)

### Thao tác được chỉ định: Reproduce Member E's source-to-snapshot trace
*(Tái lập độc lập quy trình truy vết từ tệp nguồn gốc đến bản snapshot của Thành viên E)*

#### 2.1 Mục tiêu kiểm tra
Thành viên E (Release Reviewer — {{MEMBER_E_NAME}}, MSSV {{MEMBER_E_ID}}) chịu trách nhiệm xác thực việc phát hành dữ liệu, trong đó có yêu cầu đảm bảo mọi bản ghi trong snapshot phát hành đều có thể truy ngược về dòng vật lý, tệp nguồn gốc và mã băm SHA-256 từ Lab 2. 

Với tư cách là người phụ trách Ingress (Role A — Trần Anh Tuấn), phần kiểm tra chéo đối chiếu trực tiếp nguồn Lab 2 và batch-02 của Lab 3 với các hàng đọc từ **Silver v{{SILVER_VERSION}}**. Binding của E được dùng để xác định cặp snapshot, sau đó kiểm lại độc lập với `state.json`, byte nguồn và các bảng Delta thực tế.

#### 2.2 Kịch bản và Dự đoán
1. **Mẫu 1 (`R000381` - Nguồn CSV Lab 2):**
   - **Dự đoán:** Bản ghi `R000381` phải xuất hiện tại dòng thứ 381 của `data/lab2_sources/observations_a.csv`. Giá trị đọc gốc là `26.10 C`. Event time `2026-02-02T03:10:30Z`, Ingest time `2026-02-02T03:40:30Z`. Độ trễ arrival lag = 1.800 giây (> 900s) nên `is_late` phải là `true`. Mã băm tệp nguồn phải trùng khớp với mã băm của `observations_a.csv` (`51574189...`). Cột `operator_email` phải bị loại bỏ hoàn toàn trong bản curated.
2. **Mẫu 2 (`R005001` - Nguồn JSON Lines Lab 2):**
   - **Dự đoán:** Bản ghi `R005001` xuất hiện tại dòng 1 của `data/lab2_sources/observations_b.jsonl`. Giá trị đọc gốc `18.10 C`. Độ trễ lag = 60s (<= 900s) nên `is_late` phải là `false`. `source_object` là `observations_b.jsonl`, `source_row` là 1, mã băm nguồn là `3072ad0b...`.
3. **Mẫu 3 (`R010001` - Nguồn mới Lab 3 Batch 02):**
   - **Dự đoán:** Bản ghi `R010001` xuất hiện tại dòng 1 của `data/changes/source_batch02.csv`. Giá trị đọc gốc `19.10 C`. Ingest lag = 1.800 giây nên `is_late` là `true`. `source_object` là `lab3/source_batch02.csv`, `source_row` là 1, mã băm nguồn là `9c61bf8a...`. Batch cũng có source row 131 ở `20.10 C` cùng ingest time và source object; source row 1 phải thắng theo ordinal tăng dần.

#### 2.3 Thực thi kiểm chứng độc lập
Đoạn mã sau đọc trực tiếp các mẫu từ snapshot Silver đã ghim, kiểm tra nguồn và chọn revision theo đúng thứ tự của hợp đồng. Chạy tại thư mục có `compose.yaml` (`03_DU_AN` trong bộ đã quy hoạch):
```python
import sys
from pathlib import Path
sys.path[:0] = ['src', 'tools']
from pyspark.sql import functions as F
from common import read_json, canonical_row, load_csv, sha256_file
from session import make_spark
from run_lab import snapshot
from check_role_a import source_projection, revision_order, SOURCE_PATHS

config = read_json('config/team.json')
ev = Path(config['evidence_dir'])
binding = read_json(ev / 'release_candidate/snapshot_binding.json')
state = read_json(ev / 'state.json')
assert binding['silver_path'] == state['silver_path']
assert binding['silver_version'] == state['silver_final_version']
assert binding['gold_source_silver_version'] == binding['silver_version']
spark = make_spark(config)
try:
    keys = ['R000381', 'R005001', 'R010001']
    frame = snapshot(spark, binding['silver_path'], binding['silver_version'])
    rows = {r.record_id: canonical_row(r.asDict(), allow_event_date=True)
            for r in frame.where(F.col('record_id').isin(keys)).collect()}
    revisions = [r for r in load_csv('data/changes/changes_batch02.csv')
                 if r['record_id'] == 'R010001']
    winner = min(revisions, key=revision_order)
    assert int(winner['source_row']) == rows['R010001']['source_row'] == 1
    assert winner['temperature_c'] == rows['R010001']['temperature_c'] == '19.10'
    for key in keys:
        row = rows[key]
        source = source_projection(row['source_object'], row['source_row'])
        assert all(row[k] == v for k, v in source.items())
        assert sha256_file(SOURCE_PATHS[row['source_object']]) == row['source_sha256']
        print(key, 'Silver v' + str(binding['silver_version']), row)
finally:
    spark.stop()
```

Để tái lập toàn bộ {{N_CHECKS}} kiểm tra, dùng công cụ đã lưu cùng bài:

```powershell
docker compose exec -T driver python tools/check_role_a.py --config config/team.json --trusted-manifest /instructor/trusted_lab3_inputs.json
```

#### 2.4 Kết quả quan sát và Đối chiếu

Kết quả trích xuất từ [`role_A_source_trace.json`](../../evidence/role_A_source_trace.json) của lần kiểm tra thực tế:
- **`R000381`:**
  - Đối chiếu từ nguồn (các trường được phép): `{{SOURCE_R000381}}`
  - Silver v{{SILVER_VERSION}}: `{{SILVER_R000381}}`
  - $\rightarrow$ Khớp 100% dự đoán. Email đã bị loại bỏ, cờ trễ tính đúng $1.800 > 900$, số thứ tự dòng vật lý 381 và mã băm nguồn chính xác.

- **`R005001`:**
  - Đối chiếu từ nguồn (các trường được phép): `{{SOURCE_R005001}}`
  - Silver v{{SILVER_VERSION}}: `{{SILVER_R005001}}`
  - $\rightarrow$ Khớp 100% dự đoán. Khóa chuẩn hóa thành `record_id`, lag 60s không trễ (`is_late = false`), dòng 1, mã SHA-256 khớp tuyệt đối.

- **`R010001`:**
  - Đối chiếu từ nguồn (các trường được phép): `{{SOURCE_R010001}}`
  - Silver v{{SILVER_VERSION}} (winner của Batch 02): `{{SILVER_R010001}}`
  - $\rightarrow$ Khớp 100% dự đoán.

#### 2.5 Kết luận kiểm tra chéo
Dây chuyền truy vết nguồn gốc (provenance trace) của Thành viên E có thể tái lập ở mức byte và số thứ tự dòng vật lý đối với ba mẫu đã kiểm tra. {{N_CHECKS}}/{{N_CHECKS}} assertion đạt; Gold v{{GOLD_VERSION}} khớp bản export của candidate và các hash object khớp manifest. Kết quả được ghi lúc `{{COMPLETED_AT}}`; chương trình không ghi bảng Delta. Candidate chưa owner-published, và kết quả chương trình không thay cho phần trình bày kiểm tra chéo của thành viên.

---

## Phần 3: Trả lời câu hỏi phỏng vấn thoát (10 điểm)

### Câu 1: Tại sao một thao tác merge không thay đổi dữ liệu (no-op merge) vẫn có thể ghi thêm một phiên bản commit mới vào lịch sử Delta mà không vi phạm tính lũy đẳng (idempotence)?

**Trả lời:**  
Tính lũy đẳng (idempotence) trong hệ thống dữ liệu được định nghĩa trên **nội dung logic của dữ liệu (logical dataset state)**: áp dụng cùng một thao tác một lần hay nhiều lần đều đem lại cùng một kết quả trạng thái dữ liệu nghiệp vụ duy nhất ($f(f(x)) = f(x)$). 

Trong Delta Lake, giao thức transaction log (ACID protocol) có thể ghi nhận một thao tác MERGE bằng commit mới (ví dụ từ version $v$ lên $v+1$) kèm theo thông tin commit metrics trong tệp JSON của `_delta_log/` (ví dụ: `numTargetRowsUpdated = 0`, `numTargetRowsInserted = 0`). Việc phiên bản số học tăng lên phản ánh một hành động ghi nhật ký giao dịch đã được thực hiện và kiểm tra thành công tại một thời điểm xác định (timestamp/version metadata). 

Bởi vì toàn bộ các dòng nghiệp vụ, giá trị đo đạc và định danh nguồn gốc vật lý trong bảng Silver ở phiên bản $v+1$ hoàn toàn đồng nhất về mặt logic với phiên bản $v$ (mã băm `logical_sha256` không đổi), nên tính lũy đẳng nghiệp vụ được bảo toàn trọn vẹn. Trong lượt này, Silver v1 và replay v2 có cùng digest; không yêu cầu mọi no-op MERGE đều phải tăng version.

---

### Câu 2: Tại sao một kiểm tra miền giá trị hợp lệ (passing range check) không đồng nghĩa với việc xác lập tính chính xác của phép đo (measurement accuracy)?

**Trả lời:**  
Kiểm tra miền giá trị (range check, ví dụ $-30^\circ\text{C} \le \text{temperature\_c} \le 60^\circ\text{C}$) thuần túy là một **quy tắc hợp lệ cú pháp/miền giá trị (syntactic validity rule)** nhằm loại bỏ các giá trị dị biệt thô thiển do lỗi cảm biến hỏng, tràn bộ đệm hoặc định dạng sai lệch. 

Một giá trị vượt qua kiểm tra miền (chẳng hạn $+22.50^\circ\text{C}$) là một giá trị *khả thi* (plausible value), nhưng hoàn toàn có thể là một giá trị *không chính xác* (inaccurate measurement) nếu nhiệt độ môi trường thực tế tại thời điểm đó là $+10.00^\circ\text{C}$ (do cảm biến bị lệch thang đo calibration drift, bị che khuất hoặc bị nhiễu điện). 

Độ chính xác của phép đo (accuracy) đòi hỏi việc đối chuẩn với thiết bị chuẩn tham chiếu được hiệu chuẩn (calibrated reference standard) hoặc kiểm tra sự đồng thuận với các cảm biến độc lập lân cận (consensus / QA reference agreement). Trong bộ dữ liệu Lab 2 và Lab 3, 5 điểm đo mẫu QA dù nằm hoàn toàn trong dải nhiệt độ hợp lệ $[-30, 60]^\circ\text{C}$ nhưng vẫn có độ lệch đo đạc vượt ngưỡng chấp nhận với mẫu chuẩn QA, minh chứng rõ ràng rằng tính hợp lệ không đồng nghĩa với tính chính xác.

---

### Câu 3: Tại sao tập dữ liệu benchmark nhân bản tổng hợp (replica-expanded benchmark) bắt buộc phải nằm ngoài các bảng Gold vận hành (operational Gold)?

**Trả lời:**  
Tập dữ liệu trong bài toán benchmark ở Task 4 được tạo ra bằng cách nhân bản tổng hợp 100 lần từ bảng Silver sau cập nhật (tăng số dòng từ 9.710 lên 971.000 dòng thông qua cột `replica_id`) với mục đích duy nhất là tạo ra khối lượng dữ liệu đủ lớn để đo lường sự khác biệt về kế hoạch thực thi (physical query plans) và thời gian quét dữ liệu giữa hai kiến trúc bảng (Layout A không phân vùng vs. Layout B phân vùng theo ngày `event_date`).

Các bản ghi nhân bản này là **dữ liệu bản sao nhân tạo (synthetic copies)**, hoàn toàn không phải là các quan sát vật lý độc lập mới từ các cảm biến thực tế. Nếu đưa các dòng này vào bảng Gold vận hành, các chỉ số đo lường kinh doanh cốt lõi (KPIs) như tổng số lượt quan sát (`n_observations`), tổng nhiệt độ (`sum_temperature_c`), số lượng bản ghi trễ (`n_late`) hay số lượng bản ghi vượt ngưỡng 25°C (`n_above_25`) sẽ bị thổi phồng sai lệch gấp 100 lần, phá hủy hoàn toàn tính toàn vẹn và tính chân thực khoa học của nền tảng Lakehouse.

---

### Câu 4: Tại sao một manifest phát hành (release manifest) không phải là một giao dịch đa bảng/đa đối tượng (cross-table transaction)?

**Trả lời:**  
Giao dịch ACID đa bảng (cross-table transaction) là một cơ chế đồng thời ở tầng hệ quản trị lưu trữ (storage engine), trong đó việc ghi dữ liệu vào nhiều bảng độc lập (như Bronze, Silver, Gold) được cam kết thực thi một cách nguyên tử (atomic): hoặc tất cả các bảng cùng cập nhật thành công, hoặc không bảng nào bị thay đổi nếu xảy ra sự cố giữa chừng.

Ngược lại, `release_manifest.json` là một **giao thức quy ước ở tầng ứng dụng/tiêu thụ (consumer-side convention / release protocol)**. Nó hoạt động theo nguyên tắc *manifest-last*: các bảng Delta (Silver, Gold) được ghi nhận độc lập qua các giao dịch riêng biệt trên từng bảng; sau đó, khi tất cả các bảng đã đạt trạng thái mong muốn và vượt qua các cổng kiểm duyệt (release gate), một tệp manifest được tạo ra để liên kết (bind) một bộ các cặp phiên bản cố định (`silver_version`, `gold_version`), mã băm và metadata. 

Manifest này không có cơ chế khóa phân tán, không có tính năng tự động rollback đa bảng nếu việc tải tệp manifest bị ngắt quãng, và không ngăn chặn được việc một tiến trình ghi khác can thiệp vào các bảng bên dưới nếu không có cơ chế phân quyền IAM nghiêm ngặt.

---

### Câu 5: Tại sao việc một hệ thống lưu trữ tương thích giao diện S3 API thô (raw S3 API) vẫn chưa đủ để khẳng định tính tương thích với Delta Lake?

**Trả lời:**  
Giao thức Delta Lake phụ thuộc vào **tính nguyên tử và tính nhất quán** của thao tác ghi/đọc transaction log trong `_delta_log/` (ví dụ `00000000000000000000.json`). LogStore được chọn phải đáp ứng yêu cầu commit của đúng backend và chế độ writer đang sử dụng; chỉ gọi API lưu object thành công chưa kiểm tra được các yêu cầu đó.

Một hệ thống lưu trữ đối tượng dù đáp ứng các lệnh S3 API (`GET`, `PUT`, `DELETE`, `LIST`) vẫn cần được kiểm tra với cấu hình Delta/LogStore cụ thể. Trong phạm vi bài này, các probe schema, MERGE, replay và time travel đã chạy trên backend thực với **một driver ghi tại một thời điểm**. Đây không phải bằng chứng kiểm thử ghi đồng thời nhiều driver và không được suy rộng sang backend hoặc chế độ concurrency khác.

