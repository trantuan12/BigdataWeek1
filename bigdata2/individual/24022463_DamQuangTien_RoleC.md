# BÁO CÁO CÁ NHÂN — INDIVIDUAL ASSESSMENT

**Học phần:** Cloud-Native Big Data Infrastructure & Governance

| Thông tin | Nội dung |
|---|---|
| Họ và tên | Đàm Quang Tiến |
| MSSV | 24022463 |
| Vai trò | Role C — Curation |
| Nhiệm vụ | Task 3 — Curate without losing evidence |
| Bài thực hành | Lab 2 — Data Curation, Quality & Metadata Governance |
| Namespace của nhóm | `bd-g10` |
| Môi trường thực thi | Pod `curate` trên Kubernetes; Linux, Python 3.11.17, DuckDB 1.4.5 |
| Identity của workload | `s3-ingestor` |
| Run ID Curation | `ebf648b9-1e3f-47a5-87f1-e0e8924005b4` |
| Mốc đánh giá dữ liệu | `2026-02-09T00:00:00Z` |

---

## Phần 1: Đóng góp kỹ thuật (10 điểm)

### Trước khi thực hiện

Sau bước thu nhận của Role A, dữ liệu quan sát được đưa về dạng envelope, giữ định danh nguồn, ordinal và SHA-256 của từng đối tượng. Hai nguồn quan sát có tổng cộng **10.205 bản ghi vật lý**, bao gồm dữ liệu lỗi parse, thiếu trường, đơn vị không hỗ trợ, giá trị không hợp lệ và nhiều phiên bản của cùng một `record_id`.

Nếu chỉ lọc các hàng lỗi rồi dùng `DISTINCT` để xóa trùng, hệ thống có thể mất dấu bản ghi bị loại, chọn phiên bản tùy theo thứ tự đọc hoặc thay bản mới không hợp lệ bằng một bản cũ. Khi đó, kết quả có vẻ sạch nhưng không giải thích được quá trình biến đổi.

Phạm vi phụ trách của Role C là tinh lọc theo đúng `contract.json`: chuẩn hóa kiểu dữ liệu, chọn winner có tính xác định, kiểm tra winner và phân chia mọi quan sát vào **curated**, **quarantine** hoặc **duplicates**. Đầu ra phải có schema rõ ràng, bảo toàn dấu vết và giữ nguyên kết quả logic khi thứ tự đọc nguồn thay đổi.

### Công việc đã làm

Phần Curation được thể hiện qua bốn file kỹ thuật cùng các bằng chứng kiểm tra. Mã, hợp đồng và đầu vào được gắn với run ID và các hash trong `run-record.json` để đối chiếu đúng phiên bản thực thi.

- **Chuẩn hóa và parse:** `code/typed.sql` chuẩn hóa `record_id`, `sensor_id`, đơn vị và giá trị trống; parse timestamp theo định dạng UTC trong hợp đồng; chuyển giá trị đo sang kiểu số. Giá trị không parse được được giữ để phân loại lỗi.
- **Chọn phiên bản xác định:** `code/curate.sql` dùng `ROW_NUMBER()` theo `record_id`, sắp xếp ingest time giảm dần, sau đó source object và source row tăng dần. Các phiên bản không được chọn vào duplicate ledger với lý do `SUPERSEDED` và khóa vật lý của winner.
- **Đánh giá winner:** kiểm tra trường bắt buộc, số hữu hạn, đơn vị, khoảng nhiệt độ, chronology và registry; giữ toàn bộ lý do lỗi và chọn một lý do chính theo precedence. Winner không hợp lệ vào quarantine, không thay bằng phiên bản cũ.
- **Tạo đầu ra tối thiểu:** chuyển nhiệt độ sang Celsius trước khi kiểm tra khoảng, xuất `DECIMAL(8,2)`, bổ sung `site` từ registry và gắn cờ `is_late`. Chỉ xuất 10 cột allowlist; không đưa `operator_email` hoặc `raw_payload` vào curated.
- **Kiểm chứng và điều phối:** `code/checks.py` thực hiện bảo toàn tập bản ghi, kiểm tra schema, replay và counterexamples; `code/driver.py` điều phối curation, tạo output và ghi bằng chứng của lần chạy.

Các artifact chính:

| Artifact | Nội dung |
|---|---|
| [typed.sql](<../Lab 2 - Data Curation, Quality & Metadata Governance/code/typed.sql>) | Chuẩn hóa và parse dữ liệu |
| [curate.sql](<../Lab 2 - Data Curation, Quality & Metadata Governance/code/curate.sql>) | Chọn winner, đánh giá và phân chia đầu ra |
| [checks.py](<../Lab 2 - Data Curation, Quality & Metadata Governance/code/checks.py>) | Các phép kiểm chứng C01–C04 |
| [driver.py](<../Lab 2 - Data Curation, Quality & Metadata Governance/code/driver.py>) | Điều phối pipeline curation |
| [artifact-manifest.json](<../Lab 2 - Data Curation, Quality & Metadata Governance/candidate/artifact-manifest.json>) | Count, byte size và SHA-256 của ba đầu ra |
| [conservation.json](<../Lab 2 - Data Curation, Quality & Metadata Governance/evidence/conservation.json>) | Bảo toàn khóa vật lý và duy nhất khóa nghiệp vụ |
| [schema-check.json](<../Lab 2 - Data Curation, Quality & Metadata Governance/evidence/schema-check.json>) | Schema Parquet và loại bỏ cột hạn chế |
| [replay-check.json](<../Lab 2 - Data Curation, Quality & Metadata Governance/evidence/replay-check.json>) | So sánh nội dung logic khi đảo nguồn và xáo trộn |
| [edge-cases.json](<../Lab 2 - Data Curation, Quality & Metadata Governance/evidence/edge-cases.json>) | Counterexamples và disposition quan sát được |
| [run-record.json](<../Lab 2 - Data Curation, Quality & Metadata Governance/run-record.json>) | Run ID, hash phiên bản, runtime và output thực tế |

### Trình tự curation và các phép kiểm chứng

Thứ tự xử lý là một phần của hợp đồng, vì đổi thứ tự có thể làm thay đổi winner và che giấu lỗi của phiên bản mới.

| Giai đoạn | Quy tắc áp dụng | Đầu ra hoặc dấu vết |
|---|---|---|
| Loại trước chọn winner | Cách ly lỗi parse, ID không hợp lệ hoặc ingest time không parse được | Giữ mọi lý do; precedence `PARSE → KEY → INGEST_PARSE` |
| Chọn winner | Ingest time giảm dần; source object và source row tăng dần để phá hòa | `version_rank = 1`; phiên bản còn lại vào duplicates |
| Kiểm tra winner | Áp dụng các quy tắc bắt buộc, số, đơn vị, range, thời gian và registry | Winner lỗi vào quarantine, không dùng bản cũ thay thế |
| Giữ winner hợp lệ | Chuyển đơn vị, lấy site từ registry và gắn cờ late | Curated với 10 cột allowlist |
| Bảo toàn chứng cứ | So tập khóa vật lý và kiểm tra khóa nghiệp vụ | Mỗi quan sát xuất hiện đúng một lần trong ba đầu ra |

Kết quả kiểm chứng của lần chạy:

| Mã | Nội dung | Kết quả quan sát | Trạng thái |
|:---:|---|---|:---:|
| C01 | Bảo toàn bản ghi | Không thiếu, không thừa, không giao nhau hoặc lặp khóa vật lý; không trùng khóa nghiệp vụ trong curated | PASS |
| C02 | Replay xác định | Đảo thứ tự nguồn và xáo trộn envelopes; logical SHA-256 của cả ba đầu ra đều khớp | PASS |
| C03 | Schema và tối thiểu hóa dữ liệu | Đúng 10 cột, đúng kiểu; không có cột hạn chế | PASS |
| C04 | Counterexamples | Bốn nhóm counterexample chính và ba kiểm tra bổ sung đều đạt | PASS |

Các tình huống biên được ghi trong `evidence/edge-cases.json`:

| Tình huống | Hành vi cần có | Kết quả quan sát |
|---|---|---|
| Giá trị 32 °F, các trường còn lại hợp lệ | Chuyển thành 0,00 °C và giữ | CURATED, `temperature_c = 0.00` |
| Đơn vị K không được hỗ trợ | Cách ly, giữ lý do đơn vị | QUARANTINE / `UNIT` |
| Giá trị số không hữu hạn | Cách ly, giữ lý do số | QUARANTINE / `NUMERIC` |
| Email tùy chọn để trống | Giữ hàng nếu các điều kiện khác hợp lệ; không xuất cột email | CURATED; schema không có email |
| Bản cũ 20 °C, bản mới 100 °C của `R990005` | Không lấy bản cũ thay winner lỗi | Bản cũ: DUPLICATE / `SUPERSEDED`; bản mới: QUARANTINE / `RANGE` |
| Winner hợp lệ nhưng muộn | Giữ và đánh dấu late | CURATED, `is_late = true` |
| Timestamp sai định dạng UTC | Cách ly | QUARANTINE / `TIME_PARSE` |
| ID lỗi đồng thời ingest time không parse được | Giữ cả hai lý do | QUARANTINE / `KEY\|INGEST_PARSE` |

### Kết quả và giới hạn

Ba đầu ra thỏa mãn phương trình bảo toàn:

**10.205 = 9.610 curated + 395 quarantine + 200 duplicates.**

| Đầu ra | Số dòng | Byte size | Mục đích |
|---|---:|---:|---|
| `candidate/curated.parquet` | 9.610 | 209.278 | Các winner hợp lệ, phục vụ kiểm tra và phát hành |
| `restricted/quarantine.csv` | 395 | 171.635 | Bản ghi bị cách ly và lý do lỗi |
| `restricted/duplicates.csv` | 200 | 43.206 | Các phiên bản bị thay thế và khóa của winner |

Curated có **9.610 `record_id` khác nhau**. Quarantine và duplicates được giữ trong phạm vi hạn chế để đối chiếu, không đưa vào bản analyst release.

Schema được xác minh:

| Cột | Kiểu |
|---|---|
| `record_id`, `sensor_id`, `site` | `VARCHAR` |
| `event_time_utc`, `ingest_time_utc` | `TIMESTAMP`, UTC theo hợp đồng |
| `temperature_c` | `DECIMAL(8,2)` |
| `is_late` | `BOOLEAN` |
| `source_object`, `source_sha256` | `VARCHAR` |
| `source_row` | `BIGINT` |

Các winner hợp lệ nhưng muộn vẫn được giữ: **200 dòng late** trong 9.610 dòng curated. Dữ liệu tham chiếu QA được dùng để đánh giá, không dùng để tạo hoặc sửa giá trị đo trong curated.

*Giới hạn thực tế:* replay chứng minh nội dung logic tương đương, không khẳng định hai file Parquet giống từng byte. Việc giữ source identifier và SHA-256 hỗ trợ truy nguyên nhưng không cấp quyền đọc raw. Kết quả thuộc fixture nhỏ, tổng hợp và môi trường một node; không suy rộng thành bằng chứng hiệu năng phân tán hay tính chính xác của mọi phép đo.

---

## Phần 2: Kiểm tra thực hành độc lập (10 điểm)

**Nhiệm vụ của Role C:** kiểm tra khả năng từ chối đối tượng bị sửa của Role A bằng cách đối chiếu SHA-256 độc lập với manifest tin cậy.

### Kiểm thử đã có trong phần Curation

Bốn nhóm C01–C04 xác nhận bảo toàn bản ghi, replay, schema và các tình huống biên của curation. Những phép thử này kiểm tra phần kỹ thuật của Role C; phép kiểm tra chéo với Role A đánh giá riêng tính toàn vẹn đầu vào.

### Kiểm tra chéo theo trách nhiệm Role C

Phép kiểm tra sử dụng bytes của nguồn gốc và một bản sao được sửa, rồi tính SHA-256 độc lập trong `code/independent_checks.py`. Giá trị kỳ vọng đến từ `trusted_manifest.json`, không sửa manifest để làm bản sao bị thay đổi trở nên hợp lệ.

| Kiểm tra | Dự đoán | Kết quả đo |
|---|---|---|
| Đối chiếu hash của nguồn gốc với trusted manifest | Khớp | `original_matches = true` |
| Đối chiếu hash của bản sao bị sửa | Không khớp | `tampered_matches = false` |
| Xử lý khi integrity không đạt | Từ chối nguồn trước curation | I03 ghi nhận `INPUT_INTEGRITY: observations_a.csv` |
| Bảo vệ fixture ban đầu | Chỉ sửa bản sao dùng cho phép thử | I03 ghi nhận `original_unchanged = true` |

Kết quả đo độc lập cho Role C trong [independent-checks.json](<../Lab 2 - Data Curation, Quality & Metadata Governance/evidence/independent-checks.json>):

```json
{
  "tampered_matches": false,
  "original_matches": true
}
```

Bằng chứng đối chiếu từ Role A tại [integrity-test.json](<../Lab 2 - Data Curation, Quality & Metadata Governance/integrity-test.json>):

```json
{
  "check_id": "I03",
  "status": "PASS",
  "test": "one-byte disposable-copy mutation",
  "observed_rejection": "INPUT_INTEGRITY: observations_a.csv",
  "original_unchanged": true
}
```

Kết quả chứng minh nội dung bị thay đổi không còn khớp với nguồn được tin cậy. Hai file do cùng một writer kiểm soát cùng khớp nhau chưa đủ; manifest kỳ vọng phải được cấp độc lập và giữ chỉ đọc khi workload thực thi.

**Trạng thái bằng chứng:** phép kiểm tra đã chạy qua pipeline và có kết quả PASS. Record ghi nhận thực thi tự động; chưa có chứng cứ về một phiên demo cá nhân trực tiếp của sinh viên.

---

## Phần 3: Câu hỏi vấn đáp (10 điểm)

Ba câu dưới đây phân tích các quyết định thiết kế của phần Curation. Câu hỏi exit response chính thức được giảng viên giao riêng vào cuối buổi.

### Câu 1

**Vì sao phải chuyển nhiệt độ sang Celsius trước khi kiểm tra khoảng hợp lệ và chỉ làm tròn sau khi kiểm tra?**

Giới hạn trong `contract.json` là **từ −30 đến 60 °C**, trong khi nguồn hỗ trợ cả Celsius và Fahrenheit. Vì vậy, phải đưa các giá trị về cùng đơn vị trước khi so sánh. Với Fahrenheit, công thức là `temperature_c = (reading - 32) × 5 / 9`. Chẳng hạn, 77 °F tương đương 25 °C; so trực tiếp giá trị 77 với ngưỡng Celsius sẽ loại nhầm một phép đo hợp lệ.

Kiểm tra range áp dụng lên giá trị đã chuyển đổi nhưng chưa làm tròn. Nếu làm tròn trước, một giá trị hơi vượt ngưỡng có thể được kéo về đúng biên và được chấp nhận sai. Chỉ sau khi đạt các điều kiện hợp lệ, pipeline mới xuất `temperature_c` dưới dạng `DECIMAL(8,2)` để thống nhất độ chính xác trình bày.

Counterexample trong `evidence/edge-cases.json` xác nhận 32 °F được chuyển thành 0,00 °C và giữ lại. Đơn vị K không được hỗ trợ bị cách ly với lý do `UNIT`, còn giá trị số không hữu hạn bị cách ly với lý do `NUMERIC`. Pipeline không tự đoán đơn vị hay thay giá trị lỗi bằng một nhiệt độ mặc định.

### Câu 2

**Vì sao bản ghi đến muộn vẫn được giữ trong curated thay vì tự động đưa vào quarantine?**

Đến muộn phản ánh độ trễ tiếp nhận, không tự động chứng minh giá trị đo sai. Hợp đồng đặt ngưỡng late là **900 giây**: nếu `ingest_time - event_time > 900` giây, bản ghi được gắn `is_late = true`. Nếu các điều kiện khác vẫn hợp lệ, winner được giữ trong curated để người sử dụng thấy cả giá trị và tình trạng chậm trễ.

Cần phân biệt late với chronology không hợp lệ. Một bản ghi có event time sau ingest time, hoặc ingest time vượt cutoff `AS_OF`, vi phạm quy tắc thời gian và bị cách ly. Ngược lại, một quan sát được tiếp nhận chậm nhưng có `event_time ≤ ingest_time ≤ AS_OF` vẫn có thể hợp lệ. Loại tất cả các dòng late sẽ làm mất dữ liệu và khiến tỷ lệ timeliness trông tốt hơn do thay đổi quần thể.

Kết quả thực tế giữ **200 dòng late** trong **9.610 dòng curated**. Số dòng kịp thời là **9.410/9.610**, khoảng **97,92%**, đạt ngưỡng 95% của release. Counterexample `valid late retained` trong `evidence/edge-cases.json` cũng xác nhận hàng hợp lệ được giữ với cờ late. Việc giữ cờ giúp consumer quyết định dữ liệu có phù hợp với yêu cầu độ trễ của mình hay không.

### Câu 3

**Vì sao đầu ra curated phải dùng danh sách cột cho phép thay vì `SELECT *` từ dữ liệu nguồn?**

`SELECT *` có thể đưa cả trường không cần thiết hoặc hạn chế từ nguồn vào bản bàn giao. Khi nguồn bổ sung một cột mới, cột đó cũng có thể được xuất theo mà không có quyết định rõ ràng. Danh sách cột cho phép giúp schema đầu ra ổn định và giới hạn dữ liệu đúng phạm vi consumer cần dùng.

Curated của lab chỉ có **10 cột** đã quy định. Các cột phục vụ phân tích gồm ID, sensor, site, timestamp UTC, nhiệt độ Celsius và cờ late; ba cột provenance là `source_object`, `source_row`, `source_sha256`. `site` lấy từ registry để thống nhất với sensor. Các trường `operator_email` và `raw_payload` được giữ ở phạm vi hạn chế, không xuất vào curated.

`evidence/schema-check.json` xác nhận tên và kiểu của cả 10 cột, đồng thời ghi `restricted_columns_absent = true`. Danh sách cột không chỉ phục vụ quyền riêng tư mà còn giúp consumer kiểm tra đúng schema, chẳng hạn `temperature_c` phải là `DECIMAL(8,2)` và `source_row` là `BIGINT`. Provenance hỗ trợ truy nguyên nhưng không cấp quyền đọc raw; loại bỏ các trường hạn chế cũng không đồng nghĩa dữ liệu đã được chứng minh ẩn danh hoàn toàn.

---

**Đàm Quang Tiến — MSSV: 24022463 — Role C**

**Trạng thái hoàn thiện:** phần curation có bằng chứng bảo toàn 10.205 bản ghi, schema 10 cột, replay xác định và các counterexample đạt. Phép kiểm tra chéo integrity với Role A đã có kết quả thực thi. Ba câu trả lời vấn đáp trình bày quyết định thiết kế và giới hạn của bằng chứng; việc demo cá nhân được đánh giá riêng.
