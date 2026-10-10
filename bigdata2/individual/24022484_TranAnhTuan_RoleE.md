# BÁO CÁO CÁ NHÂN — INDIVIDUAL ASSESSMENT

**Học phần:** Cloud-Native Big Data Infrastructure & Governance

| Thông tin | Nội dung |
|---|---|
| Họ và tên | Trần Anh Tuấn |
| MSSV | 24022484 |
| Vai trò | Role E — Release |
| Nhiệm vụ | Task 5 — Challenge a fail-closed release gate, owner publication and consumer handover |
| Bài thực hành | Lab 2 — Data Curation, Quality & Metadata Governance |
| Namespace của nhóm | `bd-g10` |
| Context Kubernetes | `minikube` |
| Môi trường chạy Release | Local — Python 3.11, DuckDB 1.4 LTS |
| Run ID Curation | `ebf648b9-1e3f-47a5-87f1-e0e8924005b4` |
| Run ID Publish | `32ce3332-6804-4e66-b2e9-19899af64171` |
| Release ID | `bd-g10-lab2-20261010T154147Z-32ce3332` |

---

## Phần 1: Đóng góp kỹ thuật (10 điểm)

### Trước khi thực hiện

Sau khi các thành viên hoàn thành thu thập (Role A), đo lường chất lượng (Role B), tinh lọc dữ liệu (Role C) và mô tả metadata/lineage (Role D), nhóm có một candidate dataset và các bằng chứng đi kèm. Tuy nhiên, một kịch bản làm sạch chạy thành công chưa đủ để khẳng định dữ liệu có thể bàn giao cho người phân tích (analyst). Nếu không có cổng phát hành kiểm tra lại độc lập, những lỗi như giá trị nhiệt độ bất thường, dữ liệu thiếu, trùng lặp hoặc vi phạm metadata vẫn có thể bị bỏ lọt.

Vai trò của tôi là phụ trách phần Release (Task 5): xây dựng cổng phát hành fail-closed đánh giá 9 vị từ G01–G09, thiết lập bộ harness kiểm thử 8 ca đột biến P01–P08, thực hiện quy trình phát hành manifest-last dưới quyền s3-owner và kiểm chứng việc tiếp nhận dữ liệu phía s3-analyst. Kết quả cần chứng minh rằng cổng phát hành chỉ cho phép bản ghi đạt chuẩn đi qua và từ chối dứt khoát mọi trường hợp vi phạm.

### Công việc đã làm

Phần Release được thể hiện qua mã nguồn kiểm tra cổng, kịch bản thử thách đột biến, quy trình xuất bản của Owner và kiểm chứng của Consumer. Các artifact được gắn với cùng run ID, input manifest hash, contract hash và release ID để có thể đối chiếu phiên bản thực thi.

- **Cổng phát hành G01–G09:** viết hàm `gate()` trong `code/gate.py` để tính toán lại trực tiếp từ candidate bytes, trusted inputs và metadata. Không dựa vào cờ boolean do script trước ghi nhận.
- **Harness thử thách đột biến:** tạo 8 ca kiểm thử P01–P08 trong `code/gate_tests.py`, chạy trên bản sao tạm (disposable copy) để không làm hỏng dữ liệu gốc. Ghi nhận chi tiết kết quả và lý do từ chối.
- **Quy trình xuất bản manifest-last:** trong `code/publisher.py`, sau khi candidate và các artifact được kiểm tra đạt chuẩn, tải tuần tự các file lên release prefix, xác minh SHA-256 sau khi tải và ghi `release_manifest.json` ở bước cuối cùng.
- **Kiểm chứng phía người tiêu dùng:** viết `code/consumer.py` để s3-analyst tải manifest, kiểm tra hash của từng artifact, đếm lại số dòng và schema của Parquet, đồng thời đo lường việc bị từ chối truy cập raw bucket và quyền ghi release.
- **Kiểm tra chéo độc lập:** viết hàm kiểm tra trong `code/independent_checks.py` dùng module `Decimal` để tính lại QA coverage và QA agreement mà không phụ thuộc vào code của Role B.

Các artifact chính:

- [gate.py](../code/gate.py) — logic cổng phát hành kiểm định 9 vị từ G01–G09.
- [gate_tests.py](../code/gate_tests.py) — harness thực thi 8 ca đột biến P01–P08.
- [publisher.py](../code/publisher.py) — quy trình xuất bản manifest-last của Owner.
- [consumer.py](../code/consumer.py) — kiểm chứng tiếp nhận dữ liệu và ranh giới quyền của Analyst.
- [gate-tests.json](../gate-tests.json) — kết quả chi tiết của 8 ca thử nghiệm đột biến.
- [owner-gate.json](../evidence/owner-gate.json) — kết quả tính toán G01–G09 trên candidate chuẩn.
- [owner-review.json](../evidence/owner-review.json) — biên bản phê duyệt kỹ thuật của Owner.
- [publication-record.json](../evidence/publication-record.json) — nhật ký tải lên 6 file theo trình tự manifest-last.
- [release_manifest.json](../release_manifest.json) — bản kê phát hành chính thức cho consumer.
- [consumer-check.json](../consumer-check.json) — kết quả xác minh tải về và mã lỗi 403 của analyst.

### Cổng phát hành và các phép thử đột biến

Cổng phát hành đánh giá 9 vị từ kiểm tra độc lập trước khi Owner đưa ra quyết định phát hành:

| Vị từ | Tên kiểm tra | Điều kiện bắt buộc | Kết quả quan sát trên Candidate chuẩn | Trạng thái |
|---|---|---|---|:---:|
| G01 | Source & Stage Integrity | File nguồn và artifact staging khớp mã băm tin cậy | 4/4 source objects và 3 artifact staging khớp SHA-256 | PASS |
| G02 | Record Conservation | Bảo toàn tuyệt đối $10.205 = 9.610 + 395 + 200$, không trùng, không mất | 0 physical ID thiếu hoặc thừa giữa 3 tập output | PASS |
| G03 | Released Schema & Privacy | Đúng 10 cột allowlist, không có email hay raw payload | Đúng 10 cột, định dạng Parquet, không có cột hạn chế | PASS |
| G04 | Required Values | Không có giá trị NULL trong các cột bắt buộc | 0 hàng có trường bắt buộc bị NULL | PASS |
| G05 | Values & Keys | Nhiệt độ $[-30, 60]$ °C, `record_id` duy nhất, sensor hợp lệ | 9.610 business IDs duy nhất, 100% sensor map đúng site | PASS |
| G06 | Time & Timeliness | Chronology hợp lệ, timeliness $\ge 95\%$ | Chronology 100%, timeliness đạt 9.410/9.610 = 97,92% | PASS |
| G07 | QA Reference | Coverage đạt 100/100, agreement $\ge 95\%$ với sai số 0,05 °C | Coverage đạt 100/100, agreement đạt 95/100 (95,0%) | PASS |
| G08 | Metadata Catalog | Catalog hợp lệ schema, đủ 18 trường, số dòng và hash khớp | 3 dataset hợp lệ Draft 2020-12, số dòng 9.610 khớp | PASS |
| G09 | Lineage & Replay | Event OpenLineage hợp lệ, đủ trace links, replay deterministic | Sự kiện curate hợp lệ 2-0-2, 2 traces đúng ordinal | PASS |

Harness kiểm thử thử thách cổng bằng 1 ca chuẩn và 7 ca đột biến có chủ đích:

| Mã ca | Đột biến áp dụng | Kỳ vọng | Thực tế | Mã lỗi đo được (`measured_failures`) | Đánh giá |
|:---:|---|:---:|:---:|---|:---:|
| P01 | Candidate gốc kèm metadata/lineage đầy đủ | PASS | PASS | Không có lỗi (Đủ điều kiện để Owner review) | Đạt |
| P02 | Sửa một `temperature_c` thành 100.00 °C | REJECT | REJECT | `VALUE`, `STAGED_INTEGRITY`, `METADATA`, `LINEAGE` | Đạt |
| P03 | Sửa một `temperature_c` bắt buộc thành NULL | REJECT | REJECT | `REQUIRED`, `VALUE`, `STAGED_INTEGRITY`, `METADATA`, `LINEAGE` | Đạt |
| P04 | Chèn thêm một bản ghi trùng `record_id` | REJECT | REJECT | `UNIQUENESS`, `RECONCILIATION`, `STAGED_INTEGRITY`, `METADATA`, `LINEAGE` | Đạt |
| P05 | Xóa trường `steward` trong catalog của candidate | REJECT | REJECT | `METADATA` | Đạt |
| P06 | Sửa một byte trong bản sao nguồn hoặc sai hash | REJECT | REJECT | `INPUT_INTEGRITY` | Đạt |
| P07 | Xóa output khỏi lineage của job curate | REJECT | REJECT | `LINEAGE` | Đạt |
| P08 | Xóa một bản ghi lệch chuẩn QA khỏi candidate | REJECT | REJECT | `COVERAGE`, `RECONCILIATION`, `STAGED_INTEGRITY`, `METADATA`, `LINEAGE` | Đạt |

Mỗi ca đột biến bắt đầu từ một bản sao tạm riêng biệt của candidate hoặc metadata để không làm thay đổi các file gốc. Kết quả cho thấy cả 7 ca vi phạm đều bị từ chối dứt khoát. File `evidence/rejected-probes-no-release.json` xác nhận không có bất kỳ thư mục phát hành nào được tạo cho các ca từ P02 đến P08.

### Kết quả và giới hạn

Sau khi cổng phát hành và Owner phê duyệt thành công ca P01, 5 artifact được tải lên prefix `lab2/releases/bd-g10-lab2-20261010T154147Z-32ce3332/` trước khi ghi manifest cuối cùng:

| Artifact | Byte size | SHA-256 |
|---|---:|---|
| `curated.parquet` | 209.278 | `70bf2ea29c04949e0084cbcb4ce006910e3cdee9e22771d3ae3763aa4f1134ca` |
| `catalog.json` | 4.270 | `4c1fe3c7675f311a46100bc0dc28b4aff4d9c2b656d3642912a32278fe7f6c01` |
| `quality_after.json` | 12.200 | `4d4d84979042cb9ae034695716f2834744f31e7fa9d67e367959b50bfa737bbd` |
| `lineage-summary.json` | 819 | `bee836cda2601fda0648478a206232c3759f722c4fe532830e9d56af8846a3ba` |
| `README.md` | 776 | `9566a445e4ffc4d0d9181fd2967563311ddfaec97e81e84fbcea7e8d99a9d5c3` |

File [`release_manifest.json`](../release_manifest.json) (1.928 bytes, SHA-256: `718e6104...`) được tải lên thứ sáu để đánh dấu bản phát hành hoàn tất.

Phía s3-analyst chạy kịch bản tiêu thụ dữ liệu:
- Xác minh 5/5 artifact khớp mã băm và kích thước byte.
- Số bản ghi Parquet đọc được là **9.610 dòng**, đúng 10 cột allowlist.
- Yêu cầu đọc raw `GET research-raw/lab2/inputs/batch-01/observations_a.csv` bị từ chối với mã lỗi **HTTP 403 AccessDenied**.
- Yêu cầu ghi release `PUT research-release/lab2/forbidden-analyst/...` cũng bị từ chối với **HTTP 403 AccessDenied**.
- 5 câu hỏi bàn giao M03 đều được trả lời chính xác từ catalog/README: đơn vị là Celsius, mẫu QA là 100 ID, mốc đánh giá là `2026-02-09T00:00:00Z`, giới hạn là fixture đơn node và dữ liệu trễ hợp lệ được giữ lại, liên hệ steward Role D.

*Giới hạn thực tế:* Giao thức manifest-last là quy ước phía ứng dụng, không phải multi-object transaction nguyên tử của S3. Nếu quá trình tải bị ngắt quãng trước khi manifest được ghi, các file tải trước sẽ trở thành orphan objects. Consumer bắt buộc phải kiểm tra sự tồn tại và tính hợp lệ của `release_manifest.json` trước khi sử dụng dữ liệu.

---

## Phần 2: Kiểm tra thực hành độc lập (10 điểm)

**Nhiệm vụ của Role E:** tính lại độ bao phủ mẫu QA (coverage) và độ đồng thuận (agreement) trên candidate được bàn giao, dự đoán kết quả, thực hiện truy vấn và giải thích bằng chứng.

### Kiểm thử đã có trong phần Release

Log kiểm thử của Task 5 ghi nhận toàn bộ các phép thử đều đạt:

| Nhóm kiểm thử | Kết quả cần chứng minh |
|---|---|
| Cổng G01–G09 trên candidate chuẩn | 9/9 vị từ đạt PASS; đủ điều kiện để Owner phê duyệt |
| 7 ca đột biến P02–P08 | Cổng từ chối đúng lỗi; không tạo release manifest cho ca lỗi |
| Thứ tự manifest-last | 5 artifact tải lên trước, đối chiếu hash thành công mới ghi manifest |
| Ranh giới quyền Analyst | Analyst đọc được release nhưng nhận HTTP 403 khi GET raw hoặc PUT release |
| Bàn giao M03 | Trả lời đúng 5 câu hỏi về dữ liệu mà không cần mở notebook |

Có thể tái lập từ thư mục gốc của project:

```powershell
py -3.11 code/gate_tests.py
py -3.11 code/independent_checks.py
```

Các kiểm thử này hỗ trợ xác minh logic Release Gate; chưa phải bằng chứng của phiên kiểm tra cá nhân độc lập trên artifact của Role B.

### Kiểm tra chéo theo trách nhiệm Role E

Khi nhận `curated.parquet` từ Role C và kết quả chất lượng từ Role B, trách nhiệm của tôi là kiểm tra chéo các chỉ số liên quan đến mẫu QA (`qa_reference.csv`).

| Kiểm tra | Điều kiện đạt |
|---|---|
| Khớp reference ID | Cả 100 ID trong `qa_reference.csv` đều phải có mặt trong candidate |
| QA Coverage | Số ID match chia cho 100 reference ID phải bằng 100/100 (1.0) |
| Sai số đồng thuận | Với mỗi ID match, $|T_{candidate} - T_{reference}| \le 0,05$ °C |
| QA Agreement | Số ID trong dung sai chia cho số ID match phải $\ge 95\%$ |
| Tính toán số học | Dùng kiểu `Decimal` để so sánh, tránh sai số float nhị phân |

Việc dùng `Decimal` là cần thiết vì nhiệt độ tham chiếu có hai chữ số thập phân (ví dụ 26.10). Phép trừ trên kiểu `float` thông thường có thể sinh ra phần dư nhị phân làm sai lệch điều kiện so sánh $\le 0,05$.

Kết quả đo độc lập của Role E trên file candidate thực tế:

```json
{
  "coverage_n": 100,
  "coverage_d": 100,
  "agreement_n": 95,
  "agreement_d": 100
}
```

- **Coverage:** tìm thấy 100/100 ID tham chiếu trong candidate Parquet, đạt **100,0%** (ngưỡng 100/100).
- **Agreement:** 95 ID có sai số trong khoảng $[-0,05; +0,05]$ °C, 5 ID có độ lệch lớn hơn. Tỷ lệ là **95/100 = 95,0%** (đạt ngưỡng tối thiểu 95%).

Số liệu đo độc lập này trùng khớp hoàn toàn với hai chỉ số `Q_QA_COVERAGE` và `Q_QA_AGREEMENT` trong file `quality_after.json` của Role B.

**Trạng thái hiện tại:** kết quả kiểm tra chéo đã được tích hợp và chạy thành công qua `code/independent_checks.py`, lưu bằng chứng tại `evidence/independent-checks.json` với trạng thái PASS cho cả 5 vai trò.

---

## Phần 3: Câu hỏi vấn đáp (10 điểm)

Các câu dưới đây trình bày những quyết định thiết kế của phần Release để chuẩn bị giải thích. Câu hỏi exit response chính thức được giảng viên giao riêng vào cuối buổi.

### Câu 1

**Vì sao cổng phát hành phải tính toán lại trực tiếp từ candidate bytes và trusted inputs thay vì tin vào cờ boolean "passed" do kịch bản trước ghi ra?**

Nếu cổng phát hành chỉ đọc một file JSON trung gian chứa `"status": "passed"` do curator tự tạo, cổng đó không thực sự kiểm tra dữ liệu mà chỉ lặp lại lời khẳng định của người làm sạch.

Kịch bản làm sạch có thể gặp lỗi ngầm, vô tình bỏ qua một bước kiểm tra, hoặc dữ liệu bị sửa đổi sau khi kịch bản kết thúc. Trong ca thử nghiệm P02 và P03, nếu chỉ kiểm tra file báo cáo của kịch bản trước, hệ thống sẽ không phát hiện được việc một hàng dữ liệu trong file Parquet bị đổi thành 100.00 °C hoặc NULL. 

Cổng phát hành của Role E đọc trực tiếp file Parquet bằng DuckDB, quét qua toàn bộ 9.610 dòng dữ liệu thực tế trên đĩa, tính toán lại min/max, kiểm tra giá trị NULL và so sánh với file tham chiếu gốc. Cổng được thiết kế theo nguyên tắc fail-closed: nếu thiếu file, sai định dạng hoặc không kết nối được S3, kết quả mặc định là REJECT hoặc BLOCKED, không bao giờ mặc định cho qua.

### Câu 2

**Giao thức manifest-last hoạt động như thế nào, và vì sao nó không phải là một giao dịch nguyên tử trên S3?**

S3 chỉ đảm bảo tính nguyên tử cho từng file đơn lẻ (single-object atomicity). Khi cần đưa một bộ 6 file lên S3, hệ thống không có thao tác "commit" đồng thời cho tất cả các file cùng lúc.

Để giải quyết vấn đề này, quy trình xuất bản áp dụng thỏa ước manifest-last:
1. Publisher tải lên 5 file dữ liệu và bằng chứng trước: `curated.parquet`, `catalog.json`, `quality_after.json`, `lineage-summary.json`, `README.md`.
2. Sau khi 5 file đã nằm trên S3 và được đối chiếu mã băm SHA-256 thành công, file `release_manifest.json` mới được tải lên cuối cùng.

Nếu quá trình tải bị mất kết nối ở file thứ 3, trên S3 sẽ có các file mồ côi (orphan files) nhưng chưa có manifest. Phía consumer được cấu hình để không dùng lệnh liệt kê thư mục tìm file, mà bắt buộc phải tải và kiểm tra `release_manifest.json` trước. Nếu không có manifest hợp lệ, consumer xem như đợt phát hành đó chưa từng diễn ra.

### Câu 3

**Vì sao xóa một QA ID không đồng thuận (ca P08) vẫn phải bị từ chối dù tỷ lệ agreement biểu kiến tăng lên?**

Độ đồng thuận (agreement) đo tỷ lệ các điểm đo gần giá trị tham chiếu trên tập ID đã match. Độ bao phủ (coverage) đo tỷ lệ các ID tham chiếu có mặt trong candidate trên toàn bộ 100 ID chuẩn.

Trong dữ liệu này, candidate ban đầu match đủ 100 ID, trong đó 95 ID đồng thuận và 5 ID bị lệch. Agreement là 95/100 = 95,0% (vừa đủ đạt ngưỡng). Nếu một người muốn làm đẹp chỉ số bằng cách xóa 1 ID đang bị lệch khỏi candidate:
- Số ID match giảm còn 99, làm coverage chỉ còn 99/100 (99,0%).
- Agreement biểu kiến tăng lên thành 95/99 = 95,96%.

Hành vi này loại bỏ các quan trắc không vừa ý để che giấu sai số cảm biến, vi phạm tính trung thực của dữ liệu nghiên cứu. Trong ca kiểm thử P08, cổng phát hành lập tức từ chối với lý do `COVERAGE` vì không đạt 100/100, đồng thời báo lỗi `RECONCILIATION` vì tổng số dòng candidate bị giảm còn 9.609, phá vỡ phương trình bảo toàn 10.205 dòng.

---

**Trần Anh Tuấn — MSSV: 24022484 — Role E**

**Trạng thái hoàn thiện:** nội dung kỹ thuật, 9 vị từ cổng phát hành, 8 ca thử nghiệm đột biến, quy trình manifest-last và kiểm chứng consumer đã có bằng chứng đầy đủ. Phiên kiểm tra cá nhân độc lập trên artifact của Role B đã được tích hợp qua `independent_checks.py`. Các câu trả lời vấn đáp đã sẵn sàng cho buổi exit check.
