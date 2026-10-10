# BÁO CÁO CÁ NHÂN — INDIVIDUAL ASSESSMENT

**Học phần:** Cloud-Native Big Data Infrastructure & Governance

| Thông tin | Nội dung |
|---|---|
| Họ và tên | Nguyễn Tùng Dương |
| MSSV | 24022306 |
| Vai trò | Role A — Ingestion |
| Nhiệm vụ | Task 1 — Freeze inputs and declare rules |
| Bài thực hành | Lab 2 — Data Curation, Quality & Metadata Governance |
| Namespace của nhóm | `bd-g10` |
| Context Kubernetes | `minikube` / Local Governance Sandbox |
| Môi trường chạy Ingestion | Local — Python 3.13.5 (hợp chuẩn Python 3.11 contract), DuckDB 1.5.6 (LTS family) |
| Run ID Curation | `ebf648b9-1e3f-47a5-87f1-e0e8924005b4` |
| Contract SHA-256 | `ea258858e1f3bf879aeb11c536e2debc17ae88d767aa2cb4bee6470396d81856` |
| Store ID | `bd-g10-objects` |

---

## Phần 1: Đóng góp kỹ thuật (10 điểm)

### Trước khi thực hiện

Trong nền tảng nghiên cứu dữ liệu, hai phòng thí nghiệm cung cấp dữ liệu quan trắc nhiệt độ định dạng không đồng nhất: Nguồn A dùng định dạng CSV (5,000 dòng) và Nguồn B dùng định dạng JSON Lines (5,205 dòng), kèm bảng danh mục cảm biến (`sensors.csv`, 20 dòng) và mẫu đối chứng chuẩn (`qa_reference.csv`, 100 dòng). 

Dữ liệu thô tồn tại nhiều khuyết tật có chủ đích: trường dữ liệu bị thiếu, mã định danh trùng lặp, thời gian không hợp lệ, đơn vị chưa chuẩn hóa, và đặc biệt là 5 dòng JSON bị lỗi cú pháp cấu trúc.

Hai cạm bẫy kỹ thuật nghiêm trọng thường gặp trong các pipeline dữ liệu thực tế là:
1. **Mất mát âm thầm (Silent Loss):** Script đọc dữ liệu thường dùng lệnh bỏ qua lỗi (như `on_bad_lines='skip'` hoặc `try...except` rỗng) để hoàn thành việc nạp dữ liệu. Hành vi này làm biến mất các bản ghi lỗi mà không hề để lại dấu vết kiểm toán, vi phạm nguyên tắc bảo toàn dữ liệu.
2. **Tin cậy vòng tròn (Circular Trust):** Kiểm tra tính toàn vẹn bằng cách so khớp file dữ liệu với manifest đi kèm trong cùng bucket (`source_manifest.json`). Vì danh tính `s3-ingestor` có quyền ghi vào bucket thô, nếu dữ liệu bị thay đổi thì manifest đi kèm cũng có thể bị ghi đè, khiến việc kiểm tra mất hoàn toàn ý nghĩa bảo vệ.

Trách nhiệm của tôi (Role A — Ingestion) là xây dựng lớp phòng thủ đầu vào vững chắc:
* Xác thực độc lập đối chiếu 4 file nguồn với bản `trusted_manifest.json` do giảng viên cấp qua kênh riêng biệt.
* Thiết kế cơ chế bọc phong bì (`envelopes.csv`) bảo toàn chính xác 10,205 bản ghi vật lý đầu vào, không bỏ rơi bất kỳ dòng nào (kể cả 5 dòng JSON lỗi cú pháp).
* Gắn định danh nguồn gốc vật lý `(source_object, source_row)` cho từng bản ghi.
* Soạn thảo, chuẩn hóa và đóng băng mã băm SHA-256 của Hợp đồng dữ liệu `contract.json` theo đúng schema quy chuẩn trước khi chuyển giao cho các khâu Quality và Curation.

---

### Công việc đã làm

Toàn bộ quy trình Ingestion được hiện thực hóa qua các script Python, JSON schema validation và các bộ kiểm thử tự động, được quản lý dưới cùng một `STORE_ID = "bd-g10-objects"`:

1. **Xác thực toàn vẹn độc lập và Snapshot đầu vào (`code/stage_inputs.py`):**
   - Đọc và kiểm tra kích thước byte cùng mã SHA-256 của 4 đối tượng đầu vào đối chiếu với `trusted_manifest.json`.
   - Nếu bất kỳ file nào sai lệch dù chỉ 1 byte, pipeline sẽ ném lỗi `INPUT_INTEGRITY` và lập tức dừng lại, ngăn chặn dữ liệu giả mạo xâm nhập vào hệ thống.
   - Sao lưu dữ liệu nguyên trạng vào thư mục `snapshot/` mà không thực hiện bất kỳ biến đổi giá trị nào.
   - Ghi nhận bản kê nguồn đầu vào vào [input_inventory.json](file:///c:/New%20folder/Code/Bigdata/bigdata2/input_inventory.json).

2. **Cơ chế bọc Envelope và bảo toàn lỗi cú pháp (`envelopes.csv`):**
   - Ánh xạ các trường không đồng nhất về danh mục trường chuẩn:
     * CSV: `record_id`, `sensor_id`, `event_time`, `ingest_time`, `reading`, `unit`, `operator_email`.
     * JSON Lines: `id`, `sensor`, `timestamp`, `arrived_at`, `temperature`, `temperature_unit`, `operator`.
   - Đánh số thứ tự vật lý chuẩn xác: CSV `source_row` là số thứ tự dòng dữ liệu (1-based, bỏ qua header); JSON `source_row` là số dòng thực tế trong file.
   - Gắn nhãn `source_sha256` là mã băm của toàn bộ file nguồn (không phải mã băm riêng của từng dòng).
   - Xử lý các dòng JSON lỗi cú pháp bằng khối `try...except`: Khi gặp lỗi parse JSON, gán cờ `parse_ok = false`, để trống các trường nghiệp vụ và lưu nguyên vẹn chuỗi lỗi vào `raw_payload`. Đảm bảo 5 dòng JSON lỗi vẫn hiện diện đầy đủ trong tổng số 10,205 dòng của `envelopes.csv`.

3. **Thiết lập và Đóng băng Hợp đồng Dữ liệu (`contract.json`):**
   - Soạn thảo bản hợp đồng dữ liệu tuân thủ nghiêm ngặt theo schema `observation_contract_schema.json`:
     * `schema_version = "research-telemetry-v1"`, `as_of = "2026-02-09T00:00:00Z"`.
     * Khoảng nhiệt độ vật lý hợp lệ: `[-30.0, 60.0]` °C; đơn vị hỗ trợ: `["C", "F"]`.
     * Định dạng ID: `^R[0-9]{6}$`; ngưỡng độ trễ: `900` giây (15 phút).
     * Thứ tự chống trùng lặp: `ingest_ts DESC, source_object ASC, source_row ASC`.
     * Thứ tự ưu tiên lý do loại trừ: `PARSE -> KEY -> INGEST_PARSE -> REQUIRED -> NUMERIC -> UNIT -> RANGE -> TIME_PARSE -> TIME_ORDER -> REFERENCE`.
     * Các ngưỡng chất lượng bắt buộc: Completeness 100%, Uniqueness 100%, Validity 100%, Sensor 100%, Chronology 100%, Timeliness >= 95%, QA Coverage 100/100, QA Agreement >= 95%.
   - Validate thành công 100% với schema JSON Draft 2020-12 và đóng băng mã SHA-256: `ea258858e1f3bf879aeb11c536e2debc17ae88d767aa2cb4bee6470396d81856`.

4. **Tự động hóa 4 bài kiểm tra chấp nhận I01 – I04 (`code/test_acceptance.py`):**
   - Viết module kiểm thử gọn gàng, tự động xác thực và xuất các minh chứng độc lập.

---

### Dữ liệu đầu vào và Kết quả kiểm thử Task 1

#### Bảng đối soát 4 file đầu vào với Trusted Manifest

| Tên đối tượng | Số bản ghi vật lý | Dung lượng byte | SHA-256 thực tế | Trạng thái đối chiếu |
|---|---:|---:|---|:---:|
| `observations_a.csv` | 5,000 | 421,028 | `5157418915d867980fac8d84551a8b856bba1669ca2e33cef91a15ee82101aea` | MATCH |
| `observations_b.jsonl` | 5,205 | 1,037,439 | `3072ad0b725ea3763ad4d1a7c744465f997614892dd1460f9cd815094ec61b70` | MATCH |
| `qa_reference.csv` | 100 | 1,423 | `129463c2a4e63f2dabfa2a5dd31b92e4fe4858d2415d3c7ef495cc6c6d7b65c2` | MATCH |
| `sensors.csv` | 20 | 256 | `02c390e127c37b9d12e9ca156eae23a450ad1ae037001cddd9651bb6516b9bde` | MATCH |

#### Bảng tổng hợp 4 bài kiểm tra chấp nhận (Acceptance Checks I01 – I04)

| Mã kiểm tra | Mục tiêu kiểm thử | Bằng chứng thực tế ghi nhận | Kết quả | File minh chứng |
|---|---|---|:---:|---|
| **I01** | Kiểm tra toàn vẹn 4 file đối chiếu trusted manifest | 4/4 file khớp tuyệt đối byte size và mã băm SHA-256 | **PASS** | `evidence/I01.json` |
| **I02** | Đếm envelopes và tính duy nhất của khóa vật lý | Đủ 10,205 dòng; 0 trùng lặp `(source_object, source_row)`; bảo toàn 5 dòng lỗi parse | **PASS** | `evidence/I02.json` |
| **I03** | Phát hiện giả mạo byte trên bản copy tạm (Tamper Test) | Đảo 1 byte tại vị trí 100; script ném đúng ngoại lệ `INPUT_INTEGRITY: observations_a.csv`; file gốc nguyên vẹn | **PASS** | `integrity-test.json` & `evidence/I03.json` |
| **I04** | Kiểm tra ranh giới phân quyền vai trò Curator | GET `research-raw`: HTTP 200 (Authorized); PUT `research-release`: HTTP 403 (AccessDenied) | **PASS** | `access-test.json` & `evidence/I04.json` |

---

### Kết quả và giới hạn

* **Kết quả đạt được:** Hoàn thành toàn diện Task 1; bảo toàn 100% bản ghi nguồn (10,205 dòng); đóng băng thành công hợp đồng dữ liệu; vượt qua toàn bộ 4 kiểm thử nghiệm thu.
* **Giới hạn kỹ thuật:** Quá trình staging hiện tại thực hiện theo cơ chế batch bounded ingestion trên single-node. Trong môi trường production thực tế với dữ liệu stream quy mô lớn, quá trình enveloping cần phân bổ theo micro-batches hoặc kiến trúc streaming (Kafka/Pulsar) kèm dead-letter queue phân tán.

---

## Phần 2: Kiểm tra thực hành độc lập (10 điểm)

**Nhiệm vụ kiểm tra chéo của Role A:** Xác minh câu truy vấn truy vết nguồn gốc (Source-to-output trace) do **Role D (Lê Ngọc Minh Cống — Metadata & Lineage)** xây dựng.

### Kiểm thử đã có trong phần Ingestion

Script kiểm thử `code/test_acceptance.py` được thực thi và xác nhận:

```powershell
python code/test_acceptance.py
```
*Kết quả:* `Acceptance Tests (I01-I04): ALL PASS -> {'I01': True, 'I02': True, 'I03': True, 'I04': True}`

### Kiểm tra chéo theo trách nhiệm Role A

Khi nhận các artifact từ Role D (`lineage.jsonl`, `catalog.json`, và các truy vấn trace), tôi thực hiện quy trình kiểm tra độc lập gồm 3 bước: Dự đoán kết quả (Predict) $\to$ Thực thi (Execute) $\to$ Giải thích minh chứng (Interpret):

#### 1. Truy vết bản ghi được chấp nhận (Check L02 — Accepted Row Trace)
* **Dự đoán kết quả:** Một bản ghi bất kỳ trong `curated.parquet` phải truy vết ngược được chính xác 1-1 về file nguồn gốc, số dòng gốc, mã SHA-256 nguồn; giá trị nhiệt độ được chuyển đổi theo đúng công thức; cột nhạy cảm `operator_email` phải bị loại bỏ hoàn toàn.
* **Thực thi truy vết trên bản ghi mẫu `R000025`:**
  - Khóa vật lý từ `curated.parquet`: `source_object = 's3://research-raw/lab2/inputs/batch-01/observations_a.csv'`, `source_row = 25`.
  - Đối chiếu ngược về `envelopes.csv` và `snapshot/observations_a.csv`:
    * Giá trị gốc: `reading = '66.56'`, `unit = 'F'`, `operator_email = 'operator0@example.invalid'`, `sensor_id = 'S05'`.
    * Giá trị xuất bản: `temperature_c = 19.20` ($(66.56 - 32) \times 5/9 = 19.20$), `site = 'zone-1'` (join registry sensors), cột `operator_email` hoàn toàn không tồn tại trong release schema.
* **Đánh giá minh chứng:** Truy vết khớp tuyệt đối 100%, bảo đảm nguyên tắc thu nhỏ dữ liệu và bảo mật quyền riêng tư.

#### 2. Truy vết bản ghi bị cách ly (Check L03 — Quarantined Row Trace)
* **Dự đoán kết quả:** Bản ghi bị loại trừ vào `quarantine.csv` phải giải trình được nguyên nhân bị cách ly và truy ngược được bằng chứng dòng lỗi ở đầu vào.
* **Thực thi truy vết trên bản ghi lỗi cú pháp JSON:**
  - Khóa vật lý: `source_object = 's3://research-raw/lab2/inputs/batch-01/observations_b.jsonl'`, `source_row = 5201`.
  - Đối chiếu ngược về `envelopes.csv`: Có cờ `parse_ok = false`, lưu trữ nguyên vẹn `raw_payload`.
  - Kiểm tra trong `quarantine.csv`: Bản ghi có mã lý do chính là `PARSE`, mã SHA-256 trùng khớp với file `observations_b.jsonl`.
* **Đánh giá minh chứng:** Không có bản ghi nào bị "mồ côi" (orphan record); mọi bản ghi bị loại trừ đều có lý do chẩn đoán rõ ràng.

---

## Phần 3: Câu hỏi vấn đáp (10 điểm)

Các câu hỏi dưới đây giải trình các quyết định thiết kế cốt lõi của phần Ingestion:

### Câu 1

**Vì sao mã băm kiểm tra tính toàn vẹn (integrity hash) bắt buộc phải đối chiếu với manifest độc lập (`trusted_manifest.json`) thay vì dùng manifest đi kèm dữ liệu trong bucket thô (`source_manifest.json`), và vì sao ETag không thể thay thế cho SHA-256?**

1. **Vấn đề ranh giới bảo mật và Tin cậy vòng tròn (Circular Trust):**
   Trong kiến trúc phân quyền của Lab 1 và Lab 2, workload identity `s3-ingestor` (Curator Pod) sở hữu quyền ghi (`s3:PutObject`) vào bucket `research-raw`. Nếu kẻ tấn công hoặc sự cố phần mềm làm thay đổi dữ liệu trong bucket thô, kẻ đó hoàn toàn có quyền tính lại mã hash và ghi đè file `source_manifest.json` nằm trong chính bucket đó. Khi đó, việc so sánh dữ liệu với một manifest do cùng một writer kiểm soát sẽ biến thành kiểm tra vòng tròn vô nghĩa. `trusted_manifest.json` được phân phối qua kênh độc lập chỉ đọc (read-only instructor channel), đóng vai trò là "Nguồn chân lý bất biến" (Authoritative Source of Truth) nằm ngoài tầm kiểm soát của bên ghi dữ liệu.
2. **Hạn chế của ETag so với SHA-256:**
   ETag trong giao thức S3 thường chỉ là mã băm MD5 cho các upload đơn giản dưới 5GB. Khi đối tượng được tải lên qua cơ chế multipart upload, ETag là mã băm của các checksum từng phần ghép lại kèm hậu tố số part (ví dụ: `checksum-N`), không phản ánh mã băm mật mã của toàn bộ tệp. Hơn nữa, ETag là thuộc tính do hệ thống S3 tự sinh ra (inferred attribute), không phải là chứng từ kiểm toán độc lập được ký duyệt trước khi tải. Do đó, SHA-256 là bắt buộc để chứng minh tính toàn vẹn byte-level tuyệt đối.

### Câu 2

**Vì sao các dòng JSON lỗi cú pháp không được loại bỏ ngay trong lúc ingest mà phải bọc vào envelope với `parse_ok = false` và bảo toàn `raw_payload`? Điều này liên hệ thế nào với Định luật bảo toàn ($N_{raw} = N_{curated} + N_{quarantine} + N_{duplicates}$)?**

1. **Nguyên tắc "Không thất thoát âm thầm" (No Silent Loss):**
   Nếu script ingestion âm thầm loại bỏ (`drop` hoặc `skip`) 5 dòng JSON hỏng, tổng số bản ghi đầu vào sẽ bị giảm từ 10,205 xuống 10,200. Các nhà phân tích hoặc kiểm toán viên dữ liệu ở các khâu sau sẽ hoàn toàn không biết rằng có 5 quan trắc đã từng được gửi đến trạm thu thập, dẫn đến việc diễn giải sai lệch về tình trạng hoạt động của các sensor (ví dụ: tưởng sensor ngừng phát sóng thay vì biết sensor gửi tin nhắn lỗi cú pháp).
2. **Định luật bảo toàn (Conservation Law):**
   Định luật yêu cầu tập hợp các khóa vật lý đầu vào phải phân hoạch chính xác thành 3 tập đầu ra rời nhau:
   $$N_{raw} (10,205) = N_{curated} + N_{quarantine} + N_{duplicates}$$
   Việc gán `parse_ok = false` cho phép các dòng hỏng tiếp tục di chuyển qua pipeline như những thực thể dữ liệu được định danh bằng `(source_object, source_row)`. Ở Task 3, các dòng này sẽ được phân luồng trực tiếp vào `quarantine.csv` với lý do `PARSE`. Việc bảo lưu `raw_payload` cung cấp bằng chứng pháp y (forensic evidence) giúp kỹ sư hệ thống có thể điều tra nguyên nhân hỏng hóc ở phía thiết bị gửi tin.

### Câu 3

**Vì sao mã băm của `contract.json` phải được đóng băng trước khi đo lường chất lượng, và ranh giới bảo mật của workload identity `s3-ingestor` (Curator) được kiểm soát như thế nào để ngăn chặn can thiệp vào bucket phát hành (`research-release`)?**

1. **Tính bất biến của Hợp đồng dữ liệu (Data Contract Freezing):**
   Hợp đồng dữ liệu quy định các luật chơi khách quan (phạm vi nhiệt độ, thứ tự chọn winner, ngưỡng release). Nếu không đóng băng mã SHA-256 của hợp đồng trước khi đo lường, một kỹ sư có thể nảy sinh động cơ "sửa luật cho hợp với dữ liệu" (ví dụ: nới rộng biên độ nhiệt độ từ $[-30, 60]$ lên $[-40, 80]$ để biến các hàng lỗi thành hàng hợp lệ nhằm vượt qua release gate). Việc đóng băng mã băm và ràng buộc nó vào `run-record.json` cùng `catalog.json` đảm bảo mọi kết quả đo lường chất lượng đều được đối chiếu với một phiên bản quy tắc cố định, có thể tái lập (reproducible). Bất kỳ sự thay đổi nào đối với hợp đồng đều tạo ra một run mới.
2. **Ranh giới bảo mật fail-closed của Curator Pod:**
   Theo mô hình bảo mật Lab 1, Pod Curator chỉ được cấp Secret chứa thông tin xác thực của `s3-ingestor`. Trên hệ thống lưu trữ S3, bucket policy chỉ cấp quyền `s3:GetObject` trên prefix `research-raw/lab2/inputs/` và `s3:PutObject` trên prefix `research-raw/lab2/staging/`. Chính sách hoàn toàn không cấp quyền ghi trên bucket `research-release`. Kiểm thử I04 đã chứng minh thực tế rằng yêu cầu PUT vào `research-release` bị chặn dứt khoát với mã HTTP 403 AccessDenied, ngăn ngừa việc một curator tự ý công bố dữ liệu chưa qua kiểm duyệt của Owner.

---

**Nguyễn Tùng Dương — MSSV: 24022306 — Role A**

**Trạng thái hoàn thiện:** Đã hoàn thành 100% nội dung kỹ thuật Task 1 (snapshot, envelopes, contract, 4 acceptance checks I01–I04). Đã xây dựng đầy đủ kịch bản kiểm tra chéo độc lập trên artifact của Role D và hoàn thiện toàn bộ các câu trả lời vấn đáp Exit Response.
