# BÁO CÁO CÁ NHÂN — INDIVIDUAL ASSESSMENT

**Học phần:** Cloud-Native Big Data Infrastructure & Governance

| Thông tin | Nội dung |
|---|---|
| Họ và tên | Lê Ngọc Minh Cường |
| MSSV | 24022277 |
| Vai trò | Role D — Metadata |
| Nhiệm vụ | Task 4 — Make metadata operational |
| Bài thực hành | Lab 2 — Data Curation, Quality & Metadata Governance |
| Namespace của nhóm | `bd-g10` |
| Context Kubernetes | `minikube` |
| Môi trường chạy Metadata | Local — Python 3.11, DuckDB 1.4 LTS |
| Run ID Curation | `ebf648b9-1e3f-47a5-87f1-e0e8924005b4` |
| Contract SHA-256 | `c5be4371036fc51d0688fd676e6d85b463404536f3ee11c10ec0838626862520` |
| Code Bundle SHA-256 | `0c16c3f44e823009422b8b26e05de0c25c6822d216cf2820f5465e71c57398b3` |
| Store ID | `bd-g10-objects` |

---

## Phần 1: Đóng góp kỹ thuật (10 điểm)

### Trước khi thực hiện

Sau khi Role C (Curation) xuất ra `curated.parquet`, `quarantine.csv` và `duplicates.csv`, nhóm đã có một candidate dataset đúng kỹ thuật. Tuy nhiên, nếu không có mô tả rõ ràng và có thể xác minh máy móc về các dataset đó, một người phân tích khác sẽ không biết:

- Dataset này dùng đơn vị gì, cutoff đánh giá ở thời điểm nào?
- Dữ liệu được tạo bằng code nào, từ input nào?
- Có những hạn chế chất lượng nào cần lưu ý?
- Dữ liệu được phép dùng vào mục đích gì và ai chịu trách nhiệm?
- Khi nào dữ liệu sẽ hết hạn lưu trữ và quy tắc nào ưu tiên?

Hai lỗ hổng phổ biến trong thực tế mà tôi phải tránh:

1. **Metadata được viết tay hoặc phỏng đoán:** Số dòng, schema, nullable được điền mà không đo từ artifact thực tế. Nếu code thay đổi dữ liệu nhưng metadata không cập nhật, analyst sẽ nhận thông tin sai. Gate G08 sẽ phát hiện ngay khi schema hay row_count trong catalog không khớp với file Parquet thực tế.

2. **Lineage chỉ là sơ đồ không gắn với thực thi:** Vẽ một sơ đồ luồng dữ liệu nhưng không gắn với run UUID, input SHA-256, output S3 URI thực tế thì không thể kiểm chứng. Định dạng OpenLineage yêu cầu mỗi event phải gắn run ID, job name và danh sách dataset thực tế đã đọc/ghi.

Trách nhiệm của tôi (Role D — Metadata) là:
- Xây dựng `catalog.json` đo trực tiếp từ DuckDB với đầy đủ 18 trường bắt buộc, gắn hash thực thi.
- Phát sinh và validate 4 OpenLineage events (2 UUID riêng biệt cho curate và publish).
- Tạo 2 trace queries (L02: hàng được chấp nhận, L03: hàng bị cách ly).
- Viết `retention-plan.json` với 4 quyết định dry-run.
- Trả lời đúng 5 câu bàn giao consumer (M03) chỉ từ catalog và README.

---

### Công việc đã làm

Toàn bộ phần Metadata được hiện thực hóa qua `code/metadata.py`, với các artifact đo từ DuckDB và gắn với cùng run ID, input manifest hash, contract hash và code bundle hash.

**1. Catalog JSON (`catalog.json`) — ba dataset records:**

Catalog được tạo tự động bằng hàm `make_catalog()` trong `code/metadata.py`. Mỗi dataset record được đo trực tiếp từ DuckDB:

- `columns`: lấy từ `DESCRIBE SELECT * FROM {relation}`, không nhập tay.
- `nullable`: đo bằng `SELECT count(*) WHERE "{name}" IS NULL` cho từng cột.
- `row_count`: đo bằng `SELECT count(*) FROM {relation}`.
- `input_manifest_sha256`, `contract_sha256`, `code_sha256`: tính từ file thực tế trên đĩa.

Ba dataset được mô tả:

| Dataset ID | Phân loại | Vị trí | Số dòng |
|---|---|---|---:|
| `raw-observations` | RESTRICTED | `research-raw/lab2/inputs/batch-01/observations_a.csv` + `observations_b.jsonl` | 10.205 |
| `quarantine` | RESTRICTED | `research-raw/lab2/staging/.../restricted/quarantine.csv` | 395 |
| `curated-candidate` | INTERNAL_APPROVED_ON_PUBLICATION | `research-raw/lab2/staging/.../candidate/curated.parquet` | 9.610 |

Mỗi dataset có đủ 18 trường theo yêu cầu: `dataset_id`, `title`, `purpose`, `owner`, `steward`, `classification`, `allowed_use`, `location`, `schema_version`, `columns`, `as_of`, `row_count`, `input_manifest_sha256`, `contract_sha256`, `code_sha256`, `quality_report`, `retention`, `provenance`.

Riêng `raw-observations` có thêm `source_aliases` mô tả ánh xạ tên trường giữa hai nguồn:

| Tên canonical | CSV (`observations_a.csv`) | JSON Lines (`observations_b.jsonl`) |
|---|---|---|
| `record_id` | `record_id` | `id` |
| `sensor_id` | `sensor_id` | `sensor` |
| `event_time` | `event_time` | `timestamp` |
| `ingest_time` | `ingest_time` | `arrived_at` |
| `reading` | `reading` | `temperature` |
| `unit` | `unit` | `temperature_unit` |
| `operator_email` | `operator_email` | `operator` |

Mỗi entry được validate bằng `metadata_catalog_schema.json` (Draft 2020-12) ngay sau khi tạo. Nếu bất kỳ trường nào bị thiếu hoặc sai kiểu, script sẽ ném lỗi trước khi đẩy lên S3.

**2. Hai truy vấn catalog và kiểm tra consumer (M01–M03):**

- **M01:** Tìm dataset curated có `temperature_c` đơn vị Celsius và link quality-report không rỗng → trả về `curated-candidate` với owner, steward, row_count, location, schema_version.
- **M02:** Tìm tất cả dataset RESTRICTED có retention và hold/dependency rule → trả về `raw-observations` (30 ngày) và `quarantine` (7 ngày); xác nhận schema release không có trường email.
- **M03:** Năm câu bàn giao consumer được trả lời hoàn toàn từ catalog và README, không mở notebook:
  - Đơn vị: **Celsius**
  - Mẫu QA: **100 ID tham chiếu được cung cấp**
  - Cutoff đánh giá: **2026-02-09T00:00:00Z**
  - Hạn chế đã biết: fixture đơn node; agreement QA không chứng minh độ chính xác mọi quan trắc; 200 hàng trễ hợp lệ được giữ lại
  - Liên hệ: **24022277 — Lê Ngọc Minh Cường / Role D (metadata steward)**

**3. OpenLineage events (`lineage.jsonl`) — 4 events, 2 UUID:**

| Event | Run UUID | Job | Inputs | Outputs |
|---|---|---|---|---|
| START | `ebf648b9-...` | `curate` | 3 S3 objects + contract SHA | — |
| COMPLETE | `ebf648b9-...` | `curate` | 3 S3 objects + contract SHA | curated.parquet + quarantine.csv + duplicates.csv |
| START | `32ce3332-...` | `validate-and-publish` | candidate + catalog + QA + 4 sources + 2 SHA | — |
| COMPLETE | `32ce3332-...` | `validate-and-publish` | như START | 6 release artifacts |

Tất cả 4 events được validate theo OpenLineage schema 2-0-2 chính thức đã cache. Mỗi dataset trong event mang `namespace = "urn:bigdata:bd-g10-objects"` và `name` là URI S3 thực tế.

**4. Hai trace queries (`evidence/traces.json` + `code/trace_queries.sql`):**

- **L02 — Hàng được chấp nhận (R000381):**
  - `source_object = s3://research-raw/lab2/inputs/batch-01/observations_a.csv`
  - `source_row = 381`, `version_rank = 1`, `unit = C`, `value_num = 26.1`
  - `temperature_c = 26.10 DECIMAL(8,2)`, `is_late = true` (lag > 900 giây)
  - Quy tắc áp dụng: trim/uppercase identifiers; UTC parsing; C/F conversion; registry-derived site; DECIMAL(8,2) sau validation

- **L03 — Hàng bị cách ly (R000001):**
  - `source_object = s3://research-raw/lab2/inputs/batch-01/observations_a.csv`
  - `source_row = 1`, `source_sha256 = 5157418915d867...`
  - `primary_reason = REQUIRED`, `all_reasons = REQUIRED|NUMERIC`

Một peer có thể tái lập cả hai trace bằng cách chạy `code/trace_queries.sql` với DuckDB trên `envelopes.csv`, `curated.parquet` và `quarantine.csv`.

**5. Kế hoạch retention (`retention-plan.json`) — 4 quyết định dry-run:**

| Object | Chính sách | Clock start | Hold | Dependency | Kết quả | Lý do |
|---|---|---|---|---|---|---|
| `expired-scratch` | 1 ngày | 3 ngày trước | Không | Không | **ELIGIBLE** | EXPIRED |
| `held-quarantine` | 7 ngày | 10 ngày trước | Có | Không | **KEEP** | HOLD |
| `dependent-raw` | 30 ngày | 40 ngày trước | Không | Có (active release) | **KEEP** | ACTIVE_DEPENDENCY |
| `unexpired` | 90 ngày | 1 ngày trước | Không | Không | **KEEP** | NOT_EXPIRED |

Thứ tự ưu tiên: HOLD > ACTIVE_DEPENDENCY > NOT_EXPIRED > ELIGIBLE. Không xóa object nào thật; clock retention dùng thời điểm vận hành, không dùng AS_OF fixture.

---

### Kết quả và giới hạn

- **Kết quả đạt được:** Catalog 3 dataset được validate bằng JSON Schema; 18 trường mỗi record đủ và đúng; các trường kỹ thuật (schema, nullable, row_count, hash) đo từ artifact thực tế; 4 OpenLineage events đúng cặp START/COMPLETE; 2 trace queries tái lập được; 4 retention decisions đúng logic.
- **Giới hạn kỹ thuật:** Catalog là artifact JSON tĩnh, không phải catalog server thời gian thực. Nếu chạy lại pipeline với input mới, catalog phải được tái tạo để số dòng và hash khớp với run mới. Không cài DataHub/OpenMetadata/Marquez trong core lab; những hệ thống đó là phần mở rộng tùy chọn.

---

## Phần 2: Kiểm tra thực hành độc lập (10 điểm)

**Nhiệm vụ của Role D:** Xác minh rằng ca thử nghiệm **P06 (Tamper Test — Input Integrity)** của Role E từ chối đúng khi hash của source bị giả mạo, và cổng KHÔNG tạo approved manifest cho ca này.

### Kiểm thử đã có trong phần Metadata

Khi xây dựng catalog, tôi đã thực hiện:

```python
# Mỗi trường hash trong catalog được tính trực tiếp từ file đĩa:
'input_manifest_sha256': sha(trusted_path()),    # 77dacfdb...
'contract_sha256':       sha(ROOT/'contract.json'),  # c5be4371...
'code_sha256':           bundle['sha256'],           # 0c16c3f4...
```

Gate G08 kiểm tra lại ba hash này độc lập. Nếu bất kỳ hash nào không khớp, G08 sẽ thêm lý do `METADATA` vào danh sách từ chối.

Có thể tái lập từ thư mục gốc:

```powershell
python code/driver.py
# Sau đó xem catalog.json và evidence/code-bundle.json để kiểm tra hash
```

### Kiểm tra chéo theo trách nhiệm Role D

Khi nhận bộ `gate-tests.json` từ Role E, tôi thực hiện xác minh ca **P06** theo quy trình: Dự đoán → Thực thi → Giải thích minh chứng.

#### Xác minh ca P06 — Tamper Test (Source Integrity)

**Dự đoán kết quả:**

Ca P06 đảo một byte trong bản sao tạm của `observations_a.csv` rồi nạp vào gate với hash nguồn được khai báo sai. Tôi dự đoán:
- Gate G01 (`integrity()`) phải phát hiện `source.sha256 ≠ trusted_manifest.sha256`.
- Kết quả tổng thể phải là `REJECT` với lý do `INPUT_INTEGRITY`.
- Không có `release_manifest.json` nào được tạo cho ca này.
- File gốc `snapshot/observations_a.csv` phải nguyên vẹn, SHA-256 vẫn là `5157418915d867980fac8d84551a8b856bba1669ca2e33cef91a15ee82101aea`.

**Thực thi và kết quả quan sát từ `gate-tests.json`:**

```json
{
  "case_id": "P06",
  "changed_artifact": "one byte in disposable observation A",
  "expected_result": "REJECT",
  "actual_result": "REJECT",
  "correctly_interpreted": true,
  "measured_failures": ["INPUT_INTEGRITY"],
  "gate": {
    "result": "REJECT",
    "checks": [
      {
        "check_id": "G01",
        "status": "REJECT",
        "observed": {
          "sources": [{"key": "observations_a.csv", "matches": false}, ...]
        },
        "reasons": ["INPUT_INTEGRITY"]
      }
    ]
  },
  "approved_manifest_created": false
}
```

**Giải thích minh chứng:**

- `observations_a.csv → matches: false` vì byte bị đảo khiến SHA-256 thực tế không khớp với `5157418...` trong `trusted_manifest.json`.
- Các check G02–G09 vẫn chạy nhưng tổng kết quả là REJECT vì G01 đã thất bại.
- `approved_manifest_created: false` xác nhận không có dữ liệu giả mạo nào được đưa vào release bucket.
- File gốc trong `restricted/snapshot/` không bị ảnh hưởng; ca thử nghiệm chỉ làm việc trên bản sao tạm.

Kết quả xác minh: **Ca P06 được từ chối đúng theo thiết kế.** Logic integrity check của Role E hoạt động chính xác — cổng không thể bị qua mặt bằng cách thay đổi nguồn sau khi pipeline đã chạy.

---

## Phần 3: Câu hỏi vấn đáp (10 điểm)

Các câu dưới đây trình bày những quyết định thiết kế cốt lõi của phần Metadata để chuẩn bị giải thích.

### Câu 1

**Vì sao catalog phải đo các trường kỹ thuật (schema, nullable, row_count, hash) từ artifact thực tế thay vì điền tay, và vì sao JSON Schema validation cấu trúc chưa đủ để đảm bảo catalog chính xác?**

1. **Vấn đề metadata không đồng bộ với code:**
   Trong vòng đời dự án, code curation có thể thay đổi (ví dụ thêm cột, sửa kiểu dữ liệu, thay đổi logic lọc). Nếu metadata được điền tay một lần và không gắn với thực thi, catalog sẽ mô tả một dataset cũ không còn tồn tại. Gate G08 trong `gate.py` kiểm tra lại `[(c['name'], c['type']) for c in e['columns']] != schema_of(con, relation)` — nghĩa là nếu cột thực tế khác với catalog, gate sẽ REJECT với lý do `METADATA`. Chỉ bằng cách đo trực tiếp từ DuckDB mới đảm bảo catalog luôn phản ánh artifact thực tế.

2. **JSON Schema chỉ kiểm tra cấu trúc, không kiểm tra ngữ nghĩa:**
   Một file JSON `{"row_count": 0}` vẫn vượt qua JSON Schema nếu schema chỉ yêu cầu `"type": "integer"`. Nhưng 0 là sai nếu curated thực tế có 9.610 dòng. Tương tự, `"nullable": false` cho cột `temperature_c` sẽ vượt qua schema validation dù thực tế có dòng NULL, trừ khi validator tự đếm `SELECT count(*) WHERE "temperature_c" IS NULL`. Vì vậy, Lab 2 yêu cầu thêm **semantic validator** kiểm tra row_count, schema, hash và nullability đều khớp với artifact thực tế.

### Câu 2

**Vì sao cần hai UUID riêng biệt cho curate run và publish run trong OpenLineage, và vì sao một ca thất bại không được phép có event COMPLETE?**

1. **Hai UUID — hai quyết định độc lập:**
   Curate và publish là hai giai đoạn xử lý được thực hiện bởi hai workload identity khác nhau (`s3-ingestor` và `s3-owner`) trong hai Pod riêng biệt. Việc dùng chung một UUID sẽ làm mờ đi ranh giới trách nhiệm: không thể biết output nào được tạo bởi bước nào, lỗi xảy ra ở giai đoạn nào, và ai là người thực thi. Mỗi UUID gắn với một job name riêng (`curate` và `validate-and-publish`), một bộ inputs/outputs riêng và một khoảng thời gian riêng — giúp audit trail rõ ràng và tái lập được.

2. **Event COMPLETE chỉ được phép khi thực sự hoàn thành:**
   OpenLineage phân biệt rõ bốn trạng thái cuối: `COMPLETE` (thành công), `ABORT` (chính sách từ chối), `FAIL` (lỗi thực thi). Nếu gate từ chối candidate, publisher phát `ABORT` — không bao giờ `COMPLETE` — và không tạo approved manifest. Nếu script bị lỗi exception, pipeline phát `FAIL`. Việc phát `COMPLETE` giả sẽ là bằng chứng giả mạo lineage: một audit sau này sẽ thấy "COMPLETE" nhưng không tìm thấy artifact tương ứng trên release bucket, tạo ra mâu thuẫn không thể giải thích.

### Câu 3

**Vì sao retention policy cần clock riêng cho từng loại artifact (retrieval_time, run_completion_time, publication_time) thay vì dùng một mốc chung, và vì sao không dùng AS_OF của fixture làm clock?**

1. **Mỗi loại artifact có vòng đời và rủi ro khác nhau:**
   - **Raw input** (30 ngày từ `retrieval_time`): Rủi ro chính là lưu trữ lâu dài dữ liệu thô có thể chứa thuộc tính nhạy cảm (operator_email). Clock bắt đầu từ thời điểm tải về vì đó là khi trách nhiệm lưu trữ bắt đầu.
   - **Quarantine** (7 ngày từ `run_completion_time`): Dữ liệu bị loại cần giữ đủ lâu để điều tra nhưng không quá lâu. Clock bắt đầu từ khi run kết thúc vì đó là khi quarantine được tạo ra.
   - **Approved release** (90 ngày từ `publication_time`): Dataset được phê duyệt để phân tích cần thời gian lưu trữ dài hơn để analyst có thể sử dụng sau buổi lab.
   
   Nếu dùng một mốc chung, ví dụ tất cả đều tính từ ngày thu thập raw, quarantine sẽ hết hạn quá sớm (trước khi run hoàn tất trong trường hợp curation chậm) và release có thể hết hạn ngay khi publish.

2. **AS_OF là cutoff đánh giá dữ liệu, không phải clock vận hành:**
   `AS_OF = 2026-02-09T00:00:00Z` là điểm cắt để đánh giá tính nhất quán thời gian của dữ liệu quan trắc (ingest_time ≤ AS_OF). Dùng nó làm clock retention sẽ cho kết quả vô nghĩa: mọi artifact sẽ được đánh dấu hết hạn ngay từ đầu vì AS_OF trong quá khứ so với thời điểm chạy lab (tháng 10/2026). Retention phải đo từ thời điểm vận hành thực tế (`datetime.now(timezone.utc)`) để thể hiện đúng trạng thái eligibility tại thời điểm kiểm tra.

---

**Lê Ngọc Minh Cường — MSSV: 24022277 — Role D**

**Trạng thái hoàn thiện:** Đã hoàn thành 100% nội dung kỹ thuật Task 4 — catalog 3 dataset với 18 trường đo từ artifact thực tế, 4 OpenLineage events đúng cặp và validate schema 2-0-2, 2 trace queries tái lập được (L02/L03), 4 retention dry-run decisions đúng logic, M01–M03 đủ 5 câu trả lời consumer. Đã hoàn thiện phiên kiểm tra chéo độc lập trên ca P06 của Role E với đầy đủ bằng chứng dự đoán–thực thi–giải thích.
