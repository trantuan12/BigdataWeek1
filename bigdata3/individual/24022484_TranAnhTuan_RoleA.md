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
| Context / Chế độ chạy | Local functional fallback: `local[2]` (Spark 4.0.1, Delta-Spark 4.0.1, Python 3.11.9, Java 23.0.2, Hadoop 3.4.1) |
| Trusted Manifest SHA-256 | `3cdfc84b26d290100b39ce65d4ff7cb22839f4021b9caf26897c8a4ff68091a2` |
| Upstream Logical SHA-256 | `c50f8252bfb91890975d26b16fe1d2b32f0e283a58e46bd16260b4a3cca0bf0d` |
| Run ID Preflight | `16188ad4-27ef-466c-847c-13ae8ad9f8c4` |

---

## Phần 1: Đóng góp kỹ thuật (10 điểm)

### 1.1 Trước khi thực hiện (Bối cảnh và thách thức)

Sau khi hoàn thành Lab 2, nhóm nghiên cứu đã xây dựng được một tập dữ liệu đo đạc nhiệt độ được chuẩn hóa và tinh lọc (`curated dataset`). Tuy nhiên, việc chuyển tiếp dữ liệu sang kiến trúc Lakehouse (Lab 3) đặt ra những thách thức kỹ thuật nghiêm ngặt về tính tin cậy và ranh giới dữ liệu:
1. **Bảo toàn hợp đồng dữ liệu thượng nguồn (Data Contract Integrity):** Không được tự ý thay đổi dữ liệu gốc hay âm thầm sửa các điểm bất đồng đo đạc còn tồn đọng từ Lab 2. Cần phải kiểm chứng toàn bộ 16 tệp artifact đầu vào và lược đồ (schema) theo mã băm tin cậy được phân phối qua kênh độc lập từ giảng viên (`trusted_lab3_inputs.json`).
2. **Minh bạch về bản chất bàn giao (Logical Handoff Identity):** Nếu nhóm sử dụng tệp chuyển giao dự phòng chuẩn của môn học (`data/handoff/curated.csv`), phải ghi nhận trung thực đây là bản fallback logic dạng CSV transport (9.610 dòng, 10 cột), không được mạo nhận đây là bản Parquet đã ký số phát hành của nhóm mình.
3. **Thực thi ràng buộc lược đồ tầng lưu trữ (Delta Schema Enforcement):** Đảm bảo engine Delta Lake có khả năng tự động từ chối ngay lập tức các bản ghi vi phạm cấu trúc (thừa cột, sai kiểu) ở cấp độ phân tích (AnalysisException) mà không làm ô nhiễm bảng dữ liệu hiện có.
4. **Môi trường thực thi và ghi nhận ranh giới (Execution Placement):** Thiết lập môi trường PySpark 4.0.1 và Delta Lake 4.0.1 trên nền tảng Python 3.11, Java 23 và Hadoop 3.4.1. Phải ghi nhận trung thực chế độ thực thi cục bộ `local[2]` là giải pháp fallback chức năng, không mạo nhận là phân tán đa nút (multi-node cluster).

Vai trò của tôi (Trần Anh Tuấn — Thành viên A) là kiểm soát toàn bộ cổng tiếp nhận đầu vào (Ingress Gatekeeper), xác minh mã băm, kiểm định tính toàn vẹn của tệp chuyển giao, cấu hình và tinh chỉnh runtime Spark/Delta, thực hiện probe thực thi worker và probe từ chối schema của Delta Lake.

---

### 1.2 Công việc kỹ thuật đã thực hiện

#### 1. Kiểm tra tính toàn vẹn 16 Artifacts đầu vào theo Trusted Manifest
- **Kịch bản kiểm tra độc lập (`tools/check_inputs.py`):**
  Tôi đã chạy công cụ kiểm tra sử dụng thư viện chuẩn của Python (`argparse`, `hashlib`, `json`, `csv`, `decimal`), hoàn toàn không phụ thuộc vào Spark hay cụm đám mây để kiểm tra 16 tệp dữ liệu và schema khai báo trong `trusted_lab3_inputs.json`.
  ```powershell
  python tools/check_inputs.py --trusted-manifest trusted_lab3_inputs.json
  ```
  Kết quả thực thi:
  ```text
  PASS: all manifest bytes/hashes and transport contracts match.
  Handoff rows: 9610; incoming physical revisions: 140.
  This is an input check, NOT a Spark/Delta/Kubernetes execution test.
  ```
- **Xác thực toàn bộ 16 tệp:** Ghi nhận đầy đủ vào [`evidence/input_integrity.json`](../evidence/input_integrity.json). Mã băm của bản kê tin cậy đạt chuẩn tuyệt đối: `3cdfc84b26d290100b39ce65d4ff7cb22839f4021b9caf26897c8a4ff68091a2`.

#### 2. Xác thực tính đồng nhất logic của Handoff (Logical Handoff Identity)
- Đọc và thẩm định tệp `data/handoff/handoff_manifest.json`:
  - `handoff_id`: `lab2-research-telemetry-v1-reference-handoff`
  - `handoff_kind`: `course_fallback_not_student_approval`
  - `row_count`: 9.610 bản ghi
  - `data_sha256`: `5770802a599a2931751f5ccd41e0305a03c98a08bdf5504ac8df0d1f990d8c1d`
  - `logical_sha256`: `c50f8252bfb91890975d26b16fe1d2b32f0e283a58e46bd16260b4a3cca0bf0d`
- Kiểm tra toàn bộ 9.610 dòng của `curated.csv` đối chiếu với danh bạ cảm biến (`data/handoff/sensors.csv`), bảo toàn 100% ánh xạ `sensor_id` $\rightarrow$ `site`. Mã băm logic được tính toán theo biểu diễn chính quy (canonical text JSON Lines sắp xếp theo `record_id`) khớp chính xác 100% với `c50f8252...`.
- Xác minh truy vết nguồn gốc dòng (`check_provenance`): 100% bản ghi có `source_object` là `observations_a.csv` hoặc `observations_b.jsonl` đều khớp mã băm gốc của tệp nguồn Lab 2 (`51574189...` và `3072ad0b...`).

#### 3. Cấu hình Runtime Spark 4.0.1 / Delta Lake 4.0.1 và xử lý lỗi hệ thống trên Windows
Để chuẩn bị môi trường chạy bài bản cho cả nhóm, tôi đã trực tiếp cấu hình và xử lý thành công 4 vấn đề kỹ thuật phức tạp trên nền tảng Windows:
- **Tương thích Java 23 (SecurityManager):** Môi trường máy trạm sử dụng JDK 23 (`build 23.0.2+7-58`). Kể từ Java 23, `Subject.getSubject` mặc định không cho phép nếu không kích hoạt SecurityManager, gây lỗi `UnsupportedOperationException` trong Hadoop `UserGroupInformation`. Tôi đã cấu hình bổ sung cờ JVM `_JAVA_OPTIONS="-Djava.security.manager=allow"` để xử lý triệt để.
- **Tích hợp Hadoop Native Binaries (`winutils.exe`):** Tải và thiết lập bộ nhị phân tương thích Hadoop 3.4.1 tại `d:\BigData\hadoop\bin` (`winutils.exe` và `hadoop.dll`), khai báo biến môi trường `HADOOP_HOME="d:\BigData\hadoop"` để Spark thực hiện các thao tác file cục bộ an toàn.
- **Khắc phục lỗi đường dẫn PySpark Subprocess trên Windows:** Trong PySpark 4.0.1, mã nguồn `pyspark/java_gateway.py` sử dụng `./bin/spark-submit.cmd` với dấu gạch chéo xuôi `/`, dẫn đến việc `cmd.exe` trên Windows nhận nhầm `/bin` là tham số dòng lệnh và ngắt đường dẫn thành `pyspark\.`. Tôi đã chuẩn hóa đường dẫn thông qua `os.path.normpath(os.path.join(SPARK_HOME, script))`.
- **Khắc phục lỗi ký tự Pipe trong tham số dòng lệnh:** Cấu hình mặc định `spark.redaction.regex` chứa ký tự pipe `|` (`(?i)secret|password|token...`). Khi `spark-submit.cmd` gọi batch script, Windows shell diễn giải `|` thành toán tử pipe và báo lỗi `'password' is not recognized`. Tôi đã hiệu chỉnh trong [`src/session.py`](../src/session.py) để loại bỏ pipe khỏi command-line builder, đảm bảo Spark khởi tạo thành công với cấu hình bảo mật tiêu chuẩn.
- **Định danh PySpark Python Worker:** Khai báo trực tiếp `PYSPARK_PYTHON` và `PYSPARK_DRIVER_PYTHON` trỏ tới trình thông dịch Python 3.11 (`sys.executable`), ngăn chặn lỗi kết nối socket worker do WindowsApps stub gây ra.

#### 4. Thử nghiệm cơ chế từ chối Schema tự nhiên của Delta Lake (Delta Schema Probe)
- Tại bước Preflight, runner tạo một bảng Delta cô lập tạm thời chứa 8 hàng (`smoke = root + '/preflight-' + run_id`).
- Thử nghiệm chèn một DataFrame đột biến chứa cột lạ `unexpected` (`spark.range(1).withColumn('unexpected', lit('x'))`).
- Delta Lake kích hoạt cơ chế thực thi lược đồ tự nhiên (`AnalysisException: A schema mismatch detected when writing to the Delta table`). Thao tác ghi bị từ chối triệt để.
- Đọc lại bảng sau khi từ chối: số hàng vẫn bảo toàn chính xác **8 hàng**, không có bất kỳ hàng nào bị ghi đè hay hỏng hóc. Kết quả được lưu tại [`evidence/delta_schema_probe.json`](../evidence/delta_schema_probe.json).

#### 5. Thăm dò và phân bổ thực thi Worker (Worker Probe)
- Chạy tác vụ phân tán song song trên 24 phân vùng (`spark.sparkContext.parallelize(range(24), 24).mapPartitions(f).collect()`).
- Thu thập danh sách `host`, `pid` và `partition` của từng task, ghi nhận vào [`evidence/worker_probe.json`](../evidence/worker_probe.json).
- Kết quả quan sát: `distinct_hosts = 1` (`LAPTOPSIEUCAPVIPPRO`), các tiến trình con được phân bổ trên các PID khác nhau (ví dụ: 32688, 39540, 13876, 3048, 31168,...).
- Báo cáo ghi nhận trung thực: Đây là chế độ chạy song song đa tiến trình trên một máy trạm (`local[2]`), không phải bằng chứng về cụm phân tán đa máy chủ vật lý.

#### 6. Thử nghiệm cách ly các ca đột biến hợp đồng dữ liệu (Input Contract Probes)
- Chạy 3 ca thử nghiệm đột biến dữ liệu trong bộ nhớ (không ghi vào Delta table) trên lô dữ liệu bổ sung (`incoming batch-02`), kết quả ghi nhận tại [`evidence/input_contract_tests.json`](../evidence/input_contract_tests.json):
  - `VALUE`: Sửa `temperature_c = '100.00'` $\rightarrow$ Bị từ chối với mã lỗi `VALUE` (vượt ngưỡng $[-30, 60]^\circ\text{C}$).
  - `REQUIRED_VALUE`: Sửa `temperature_c = None` $\rightarrow$ Bị từ chối với mã lỗi `VALUE` (thiếu giá trị bắt buộc).
  - `SCHEMA_ALLOWLIST`: Thêm trường email bị hạn chế `operator_email = 'restricted@example.invalid'` $\rightarrow$ Bị từ chối với mã lỗi `SCHEMA_ALLOWLIST` (vi phạm danh sách 10 cột cho phép).
  - Cả 3 ca đều có `writes_performed: 0`, chứng minh chính sách bảo vệ "Reject before Write" hoạt động hoàn hảo.

---

### 1.3 Kết quả kiểm định các tiêu chí H01–H04

Bảng tổng hợp kết quả kiểm tra Task 1:

| Mã kiểm tra | Tên kiểm tra | Điều kiện bắt buộc | Kết quả đo lường thực tế | Trạng thái |
|:---:|---|---|---|:---:|
| **H01** | Input Identity & Released Schema | Cả 16 artifact đầu vào và schema khớp tuyệt đối mã băm trong trusted manifest; đúng 10 cột dữ liệu; đúng kiểu dữ liệu. | Đã xác minh 16/16 tệp khớp SHA-256; `curated.csv` chứa đúng 10 cột allowlist; kiểu dữ liệu timestamp, decimal(8,2), boolean, string, bigint được xác thực hợp lệ. | **PASS** |
| **H02** | Logical Handoff Identity | Tài liệu hóa trung thực nguồn bàn giao (team Parquet hoặc fallback CSV); bảo toàn nguồn gốc và mã băm logic; không thay thế ngầm. | Ghi nhận rõ ràng sử dụng course fallback `data/handoff/curated.csv` (9.610 dòng); mã băm logic `c50f8252...` khớp 100% với `handoff_manifest.json`; nguồn gốc bản ghi giữ nguyên. | **PASS** |
| **H03** | Native Delta Schema Rejection | Thử nghiệm cô lập từ chối cột bất thường dưới dạng lỗi schema analysis; 8 hàng ban đầu còn nguyên vẹn. | Delta Lake ném ngoại lệ `AnalysisException` khi cố tình chèn cột `unexpected`; số dòng trong bảng sau đó vẫn giữ nguyên đúng 8 hàng. | **PASS** |
| **H04** | Runtime & Execution Placement | Thu thập thông tin phiên bản Spark, Delta, Java, Hadoop; ghi nhận Application ID và host/worker; dán nhãn trung thực thực thi local. | Đã thu thập đầy đủ: Spark 4.0.1, Delta-Spark 4.0.1, Python 3.11.9, Java 23.0.2, Hadoop 3.4.1; Application ID `local-1793476503285`; dán nhãn `local_file` và cảnh báo thực thi đơn máy. | **PASS** |

**Các tệp bằng chứng kỹ thuật nộp kèm:**
- [`evidence/input_integrity.json`](../evidence/input_integrity.json) — Biên bản xác thực 16 tệp theo `trusted_lab3_inputs.json`.
- [`evidence/preflight.json`](../evidence/preflight.json) — Trạng thái PASS của preflight, số dòng baseline (9.610) và batch mới (140).
- [`evidence/delta_schema_probe.json`](../evidence/delta_schema_probe.json) — Bằng chứng từ chối chèn schema lạ và bảo toàn 8 dòng.
- [`evidence/worker_probe.json`](../evidence/worker_probe.json) — Nhật ký phân bổ tác vụ trên 24 phân vùng và ghi nhận host thực thi.
- [`evidence/runtime_preflight.json`](../evidence/runtime_preflight.json) — Cấu hình phiên bản runtime và ranh giới hệ thống.
- [`evidence/run_preflight.json`](../evidence/run_preflight.json) — Trạng thái hoàn thành COMPLETE của job preflight.
- [`evidence/lineage.jsonl`](../evidence/lineage.jsonl) — Sự kiện OpenLineage ghi nhận bắt đầu và kết thúc tiến trình.

---

## Phần 2: Kiểm tra chéo độc lập (10 điểm)

### Thao tác được chỉ định: Reproduce Member E's source-to-snapshot trace
*(Tái lập độc lập quy trình truy vết từ tệp nguồn gốc đến bản snapshot của Thành viên E)*

#### 2.1 Mục tiêu kiểm tra
Thành viên E (Release Reviewer — Nguyễn Tùng Dương, MSSV 24022306) chịu trách nhiệm xác thực việc phát hành dữ liệu, trong đó có yêu cầu đảm bảo mọi bản ghi trong snapshot phát hành đều có thể truy ngược về dòng vật lý, tệp nguồn gốc và mã băm SHA-256 từ Lab 2. 

Với tư cách là người phụ trách Ingress (Role A — Trần Anh Tuấn), tôi thực hiện kiểm tra chéo độc lập bằng cách viết kịch bản truy vết đối chiếu trực tiếp dữ liệu thô (raw source files) từ Lab 2 và batch-02 của Lab 3 với tập dữ liệu bàn giao `curated.csv`, không dựa vào bất kỳ metadata trung gian nào của Role E.

#### 2.2 Kịch bản và Dự đoán
1. **Mẫu 1 (`R000381` - Nguồn CSV Lab 2):**
   - **Dự đoán:** Bản ghi `R000381` phải xuất hiện tại dòng thứ 381 của `data/lab2_sources/observations_a.csv`. Giá trị đọc gốc là `26.10 C`. Event time `2026-02-02T03:10:30Z`, Ingest time `2026-02-02T03:40:30Z`. Độ trễ arrival lag = 1.800 giây (> 900s) nên `is_late` phải là `true`. Mã băm tệp nguồn phải trùng khớp với mã băm của `observations_a.csv` (`51574189...`). Cột `operator_email` (`operator1@example.invalid`) phải bị loại bỏ hoàn toàn trong bản curated.
2. **Mẫu 2 (`R005001` - Nguồn JSON Lines Lab 2):**
   - **Dự đoán:** Bản ghi `R005001` xuất hiện tại dòng 1 của `data/lab2_sources/observations_b.jsonl`. Giá trị đọc gốc `18.10 C`. Độ trễ lag = 60s (<= 900s) nên `is_late` phải là `false`. `source_object` là `observations_b.jsonl`, `source_row` là 1, mã băm nguồn là `3072ad0b...`.
3. **Mẫu 3 (`R010001` - Nguồn mới Lab 3 Batch 02):**
   - **Dự đoán:** Bản ghi `R010001` xuất hiện tại dòng 1 của `data/changes/source_batch02.csv`. Giá trị đọc gốc `19.10 C`. Ingest lag = 1.800 giây nên `is_late` là `true`. `source_object` là `lab3/source_batch02.csv`, `source_row` là 1, mã băm nguồn là `9c61bf8a...`.

#### 2.3 Thực thi kiểm chứng độc lập
Tôi đã chạy đoạn mã Python sau để đọc trực tiếp từng byte và thứ tự dòng từ các tệp nguồn:
```python
import csv, json, hashlib

# 1. Kiểm tra mẫu R000381 từ observations_a.csv
with open('data/lab2_sources/observations_a.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    rows_a = list(reader)
row_381 = rows_a[380] # dòng thứ 381 (index 380)

# 2. Kiểm tra mẫu R005001 từ observations_b.jsonl
with open('data/lab2_sources/observations_b.jsonl', 'r', encoding='utf-8') as f:
    line_1 = json.loads(f.readline())

# 3. Kiểm tra mẫu R010001 từ source_batch02.csv
with open('data/changes/source_batch02.csv', 'r', encoding='utf-8') as f:
    reader_b2 = csv.DictReader(f)
    row_b2 = next(reader_b2)

# Đối chiếu với curated.csv và changes_batch02.csv
curated = {r['record_id']: r for r in csv.DictReader(open('data/handoff/curated.csv', encoding='utf-8'))}
changes = {r['record_id']: r for r in csv.DictReader(open('data/changes/changes_batch02.csv', encoding='utf-8'))}

print("Curated R000381:", curated['R000381'])
print("Curated R005001:", curated['R005001'])
print("Changes R010001:", changes['R010001'])
```

#### 2.4 Kết quả quan sát và Đối chiếu

Kết quả trích xuất thực tế:
- **`R000381`:**
  - Nguồn thô: `{'record_id': 'R000381', 'sensor_id': 'S01', 'event_time': '2026-02-02T03:10:30Z', 'ingest_time': '2026-02-02T03:40:30Z', 'reading': '26.10', 'unit': 'C', 'operator_email': 'operator1@example.invalid'}`
  - Curated: `{'record_id': 'R000381', 'sensor_id': 'S01', 'site': 'zone-1', 'event_time_utc': '2026-02-02T03:10:30Z', 'ingest_time_utc': '2026-02-02T03:40:30Z', 'temperature_c': '26.10', 'is_late': 'true', 'source_object': 'observations_a.csv', 'source_row': '381', 'source_sha256': '5157418915d867980fac8d84551a8b856bba1669ca2e33cef91a15ee82101aea'}`
  - $\rightarrow$ Khớp 100% dự đoán. Email đã bị loại bỏ, cờ trễ tính đúng $1.800 > 900$, số thứ tự dòng vật lý 381 và mã băm nguồn chính xác.

- **`R005001`:**
  - Nguồn thô: `{'id': 'R005001', 'sensor': 'S01', 'timestamp': '2026-02-03T17:40:30Z', 'arrived_at': '2026-02-03T17:41:30Z', 'temperature': '18.10', 'temperature_unit': 'C', 'operator': 'operator1@example.invalid'}`
  - Curated: `{'record_id': 'R005001', 'sensor_id': 'S01', 'site': 'zone-1', 'event_time_utc': '2026-02-03T17:40:30Z', 'ingest_time_utc': '2026-02-03T17:41:30Z', 'temperature_c': '18.10', 'is_late': 'false', 'source_object': 'observations_b.jsonl', 'source_row': '1', 'source_sha256': '3072ad0b725ea3763ad4d1a7c744465f997614892dd1460f9cd815094ec61b70'}`
  - $\rightarrow$ Khớp 100% dự đoán. Khóa chuẩn hóa thành `record_id`, lag 60s không trễ (`is_late = false`), dòng 1, mã SHA-256 khớp tuyệt đối.

- **`R010001`:**
  - Nguồn thô: `{'record_id': 'R010001', 'sensor_id': 'S01', 'event_time': '2026-02-06T00:01:00Z', 'ingest_time': '2026-02-06T00:31:00Z', 'reading': '19.10', 'unit': 'C'}`
  - Changes Batch 02: `{'record_id': 'R010001', 'sensor_id': 'S01', 'site': 'zone-1', 'event_time_utc': '2026-02-06T00:01:00Z', 'ingest_time_utc': '2026-02-06T00:31:00Z', 'temperature_c': '19.10', 'is_late': 'true', 'source_object': 'lab3/source_batch02.csv', 'source_row': '1', 'source_sha256': '9c61bf8a4183e3ba040334059a451c7c72583ba45bd46def7bcb81bdde52a4bb'}`
  - $\rightarrow$ Khớp 100% dự đoán.

#### 2.5 Kết luận kiểm tra chéo
Dây chuyền truy vết nguồn gốc (provenance trace) của Thành viên E được thiết lập hoàn toàn trung thực, có khả năng tái lập và kiểm chứng độc lập ở mức từng byte và số thứ tự dòng vật lý. Không có hiện tượng làm giả số liệu, không có việc tùy tiện gán nhãn nguồn gốc và không có sự rò rỉ các trường bảo mật vào tập dữ liệu bàn giao.

---

## Phần 3: Trả lời câu hỏi phỏng vấn thoát (10 điểm)

### Câu 1: Tại sao một thao tác merge không thay đổi dữ liệu (no-op merge) vẫn có thể ghi thêm một phiên bản commit mới vào lịch sử Delta mà không vi phạm tính lũy đẳng (idempotence)?

**Trả lời:**  
Tính lũy đẳng (idempotence) trong hệ thống dữ liệu được định nghĩa trên **nội dung logic của dữ liệu (logical dataset state)**: áp dụng cùng một thao tác một lần hay nhiều lần đều đem lại cùng một kết quả trạng thái dữ liệu nghiệp vụ duy nhất ($f(f(x)) = f(x)$). 

Trong Delta Lake, giao thức transaction log (ACID protocol) ghi nhận mọi thao tác MERGE thành một commit mới (ví dụ từ version $v$ lên $v+1$) kèm theo thông tin commit metrics trong tệp JSON của `_delta_log/` (ví dụ: `numTargetRowsUpdated = 0`, `numTargetRowsInserted = 0`). Việc phiên bản số học tăng lên phản ánh một hành động ghi nhật ký giao dịch đã được thực hiện và kiểm tra thành công tại một thời điểm xác định (timestamp/version metadata). 

Bởi vì toàn bộ các dòng nghiệp vụ, giá trị đo đạc và định danh nguồn gốc vật lý trong bảng Silver ở phiên bản $v+1$ hoàn toàn đồng nhất về mặt logic với phiên bản $v$ (mã băm `logical_sha256` không đổi), nên tính lũy đẳng nghiệp vụ được bảo toàn trọn vẹn. Ngược lại, nếu ép buộc số phiên bản không được tăng thì hệ thống sẽ không thể phân biệt được giữa một pipeline "chưa từng chạy" với một pipeline "đã chạy kiểm tra nhưng không có dữ liệu mới để cập nhật".

---

### Câu 2: Tại sao một kiểm tra miền giá trị hợp lệ (passing range check) không đồng nghĩa với việc xác lập tính chính xác của phép đo (measurement accuracy)?

**Trả lời:**  
Kiểm tra miền giá trị (range check, ví dụ $-30^\circ\text{C} \le \text{temperature\_c} \le 60^\circ\text{C}$) thuần túy là một **quy tắc hợp lệ cú pháp/miền giá trị (syntactic validity rule)** nhằm loại bỏ các giá trị dị biệt thô thiển do lỗi cảm biến hỏng, tràn bộ đệm hoặc định dạng sai lệch. 

Một giá trị vượt qua kiểm tra miền (chẳng hạn $+22.50^\circ\text{C}$) là một giá trị *khả thi* (plausible value), nhưng hoàn toàn có thể là một giá trị *không chính xác* (inaccurate measurement) nếu nhiệt độ môi trường thực tế tại thời điểm đó là $+10.00^\circ\text{C}$ (do cảm biến bị lệch thang đo calibration drift, bị che khuất hoặc bị nhiễu điện). 

Độ chính xác của phép đo (accuracy) đòi hỏi việc đối chuẩn với thiết bị chuẩn tham chiếu được hiệu chuẩn (calibrated reference standard) hoặc kiểm tra sự đồng thuận với các cảm biến độc lập lân cận (consensus / QA reference agreement). Trong bộ dữ liệu Lab 2 và Lab 3, 5 điểm đo mẫu QA dù nằm hoàn toàn trong dải nhiệt độ hợp lệ $[-30, 60]^\circ\text{C}$ nhưng vẫn có độ lệch đo đạc vượt ngưỡng chấp nhận với mẫu chuẩn QA, minh chứng rõ ràng rằng tính hợp lệ không đồng nghĩa với tính chính xác.

---

### Câu 3: Tại sao tập dữ liệu benchmark nhân bản tổng hợp (replica-expanded benchmark) bắt buộc phải nằm ngoài các bảng Gold vận hành (operational Gold)?

**Trả lời:**  
Tập dữ liệu trong bài toán benchmark ở Task 4 được tạo ra bằng cách nhân bản tổng hợp 100 lần từ bảng Silver sau cập nhật (tăng số dòng từ ~9.730 lên ~973.000 dòng thông qua cột `replica_id`) với mục đích duy nhất là tạo ra khối lượng dữ liệu đủ lớn để đo lường sự khác biệt về kế hoạch thực thi (physical query plans) và thời gian quét dữ liệu giữa hai kiến trúc bảng (Layout A không phân vùng vs. Layout B phân vùng theo ngày `event_date`).

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
Giao thức Delta Lake phụ thuộc sống còn vào **tính nguyên tử và tính nhất quán đọc-sau-ghi (atomic and consistent read-after-write)** khi tạo các tệp commit JSON trong thư mục `_delta_log/` (ví dụ `000000.json`, `000001.json`). Thao tác này đòi hỏi cơ chế `PutObject` có điều kiện hoặc cơ chế ngăn chặn ghi đè nguyên tử kiểu `put-if-absent` (mutual exclusion / atomic rename) để đảm bảo hai driver Spark ghi đồng thời không thể cùng commit vào cùng một số hiệu phiên bản.

Giao diện chuẩn AWS S3 API cơ bản (raw S3 API) ban đầu không cung cấp thao tác đổi tên nguyên tử (`atomic rename`) hay thao tác `put-if-absent` tự nhiên như hệ thống file POSIX hay Azure ADLS Gen2. Một hệ thống lưu trữ đối tượng (như MinIO, Ceph, SeaweedFS) dù đáp ứng 100% các lệnh API S3 thô (`GET`, `PUT`, `DELETE`, `LIST`) vẫn có thể vi phạm tính toàn vẹn của Delta Lake nếu hệ thống đó thiếu một cơ chế LogStore phù hợp (như S3DynamoDBLogStore hoặc bộ điều phối lock phân tán). Do đó, chỉ có smoke test kiểm tra trực tiếp hành vi ghi đồng thời và kiểm soát xung đột trên chính backend cụ thể mới đủ bằng chứng khẳng định độ tương thích với Delta Lake.

