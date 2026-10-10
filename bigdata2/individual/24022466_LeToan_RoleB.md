# BÁO CÁO CÁ NHÂN — INDIVIDUAL ASSESSMENT

**Học phần:** Cloud-Native Big Data Infrastructure & Governance

| Thông tin | Nội dung |
|---|---|
| Họ và tên | Lê Toàn |
| MSSV | 24022466 |
| Vai trò | Role B — Quality |
| Nhiệm vụ | Task 2 — Profile and measure quality |
| Bài thực hành | Lab 2 — Data Curation, Quality & Metadata Governance |
| Namespace của nhóm | `bd-g01` |
| Context Kubernetes | `minikube` |
| Môi trường chạy Quality | Local — Python 3.14.7, DuckDB 1.4.4 |
| Run ID Quality | `87be975f-5a50-4d1c-a20d-f9ccb9466b10` |

---

## Phần 1: Đóng góp kỹ thuật (10 điểm)

### Trước khi thực hiện

Nhóm nhận hai nguồn quan trắc nhiệt độ ở định dạng CSV và JSON Lines, kèm sensor registry và mẫu tham chiếu QA. Dữ liệu có giá trị thiếu, bản ghi trùng, timestamp không hợp lệ, unit không hỗ trợ và các dòng JSON lỗi. Việc đọc dữ liệu thành công chưa đủ để khẳng định dữ liệu đạt chất lượng hoặc đủ điều kiện phát hành.

Vai trò của tôi là phụ trách phần Quality: định nghĩa các phép đo bằng SQL, xác định đúng quần thể và mẫu số, thống kê lỗi, đồng thời chuẩn bị bằng chứng before/after. Kết quả cần cho thấy các bản ghi bị loại ở từng giai đoạn để tránh đánh giá chất lượng trên một tập dữ liệu đã được lọc mà không giải thích.

### Công việc đã làm

Phần Quality được thể hiện qua SQL, metric records, báo cáo và log kiểm thử của Task 2. Các artifact gắn với cùng run ID, input manifest hash, contract hash và code bundle hash để có thể đối chiếu phiên bản thực thi.

- **Chuẩn hóa và typing:** trim, uppercase `record_id`, `sensor_id`, `unit`; dùng `TRY_CAST` và `try_strptime` để ghi nhận giá trị không parse được. Timestamp phải đúng UTC format `YYYY-MM-DDTHH:MM:SSZ`.
- **Xác định quần thể:** tạo `typed`, `key_eligible`, `ranked` và `winners`. Winner được chọn theo `ingest_ts DESC`, sau đó `source_object ASC`, `source_row ASC`.
- **Định nghĩa metric:** đo completeness, business-key uniqueness, value validity, sensor consistency, chronology và timeliness. QA coverage và agreement được tính riêng.
- **Điều tra lỗi:** lưu số lượng và source references của các nhóm lỗi; không đưa raw payload hoặc email vào báo cáo chung.
- **Báo cáo và biểu đồ:** xuất before-quality, metric records và biểu đồ từ số đo SQL. After-quality hiện được ghi rõ là projection chẩn đoán.
- **Counterexample tests:** kiểm tra NULL, mẫu số 0, Fahrenheit, NaN/Infinity, unit K, winner mới hơn nhưng invalid, tie-break và mẫu số QA.

Các artifact chính:

- [typed.sql](../task 2/code/typed.sql) — chuẩn hóa, typing, key eligibility và winner policy.
- [quality.sql](../task 2/code/quality.sql) — truy vấn metric, intake counts và defect categories.
- [run_tasks.py](../task 2/code/run_tasks.py) — driver xuất metric, biểu đồ và bằng chứng.
- [quality_before.json](../task 2/runs/87be975f-5a50-4d1c-a20d-f9ccb9466b10/quality_before.json) — kết quả tại checkpoint Task 2.
- [metric_records.json](../task 2/runs/87be975f-5a50-4d1c-a20d-f9ccb9466b10/metric_records.json) — cohort, numerator, denominator, value, threshold, severity và status.
- [defect_investigation.json](../task 2/runs/87be975f-5a50-4d1c-a20d-f9ccb9466b10/defect_investigation.json) — số lượng và ví dụ physical source references.
- [quality-tests.json](../task 2/runs/87be975f-5a50-4d1c-a20d-f9ccb9466b10/quality-tests.json) — log và exit code của năm nhóm kiểm thử.
- [run-record.json](../task 2/runs/87be975f-5a50-4d1c-a20d-f9ccb9466b10/run-record.json) và [code-hashes.json](../task 2/runs/87be975f-5a50-4d1c-a20d-f9ccb9466b10/code-hashes.json) — thông tin thực thi, artifact hashes và phiên bản mã.

### Quần thể đánh giá

Before-quality population **W** là tập deterministic winners trước khi lọc row validity. Completeness, value validity, sensor consistency và chronology dùng toàn bộ W. Business-key uniqueness trước xử lý dùng key-eligible records trước dedup; timeliness chỉ dùng các hàng chronology hợp lệ.

| Thành phần | Số bản ghi |
|---|---:|
| Raw physical records | 10.205 |
| Parse failures được giữ trong envelopes | 5 |
| Parsed intake | 10.200 |
| Bị loại trước dedup vì KEY hoặc INGEST_PARSE | 40 |
| Key-eligible trước dedup | 10.160 |
| Duplicate excess | 200 |
| W — winners trước lọc validity | 9.960 |
| Required-field failures trong parsed intake | 130 |

Quan hệ đếm của intake là `10.205 = 5 + 40 + 200 + 9.960`. Quan hệ này giúp giải thích các quần thể, nhưng chưa thay thế kiểm tra bảo toàn bằng tập physical IDs của ba output Task 3.

### Kết quả và giới hạn

| Metric trước xử lý | Tử số | Mẫu số | Giá trị | Ngưỡng | Trạng thái |
|---|---:|---:|---:|---:|---|
| Required-field completeness | 9.850 | 9.960 | 98,8956% | 100% | FAIL |
| Business-key uniqueness | 9.960 | 10.160 | 98,0315% | 100% | FAIL |
| Value validity | 9.710 | 9.960 | 97,4900% | 100% | FAIL |
| Sensor consistency | 9.930 | 9.960 | 99,6988% | 100% | FAIL |
| Temporal consistency | 9.890 | 9.960 | 99,2972% | 100% | FAIL |
| Timeliness | 9.690 | 9.890 | 97,9778% | 95% | PASS |

Các metric FAIL phản ánh những lỗi được cài trong fixture. Timeliness loại riêng 70 winner không có chronology hợp lệ khỏi mẫu số. Required predicates trả về NULL được xem là thất bại; mẫu số 0 có `value = null` và `status = NOT_EVALUATED`.

Một số nhóm lỗi đã được điều tra:

| Nhóm | Quần thể | Số lượng | Ví dụ source reference |
|---|---|---:|---|
| PARSE | Raw intake | 5 | `observations_b.jsonl:5201` |
| KEY | Parsed intake trước dedup | 20 | `observations_a.csv:341` |
| INGEST_PARSE | Parsed intake trước dedup | 20 | `observations_a.csv:361` |
| REQUIRED | W | 110 | `observations_a.csv:1` |
| NUMERIC | W | 170 | `observations_a.csv:1` |
| UNIT | W | 40 | `observations_a.csv:161` |
| REFERENCE — sensor registry | W | 30 | `observations_a.csv:311` |
| TIME | W | 70 | `observations_a.csv:241` |

Các nhóm có thể chồng lấp, nên không cộng trực tiếp để tính tổng số hàng bị loại. `source_row` của CSV là ordinal của data record, không tính header; JSON Lines dùng line ordinal. Reason `REFERENCE` chỉ sensor không có trong registry, khác với QA agreement.

Projection chẩn đoán giữ lại **9.610 hàng**, bằng **94,17% raw intake** hoặc **96,49% W**; có 350 winner không đạt điều kiện. Trong tập giữ lại, required-field, uniqueness, value, sensor và chronology đều đạt 100%; timeliness là **9.410/9.610 = 97,9188%**, còn **200 hàng trễ** được giữ lại. QA coverage đạt **100/100**, agreement đạt **95/100** với tolerance 0,05 °C.

![Biểu đồ chất lượng trước và sau chẩn đoán](../task 2/runs/87be975f-5a50-4d1c-a20d-f9ccb9466b10/quality_comparison.png)

Completeness tăng sau lọc vì các hàng thiếu bị loại khỏi quần thể, không chứng minh giá trị thiếu đã được phục hồi. Timeliness giảm nhẹ do mẫu số thay đổi khi các hàng bị loại bởi quy tắc khác. QA chỉ đánh giá mẫu tham chiếu được cung cấp, không chứng minh độ chính xác của mọi quan trắc thực tế.

Kết quả after hiện tại nằm trong [quality_after_diagnostic.json](../task 2/runs/87be975f-5a50-4d1c-a20d-f9ccb9466b10/quality_after_diagnostic.json), chưa được tính trên candidate Parquet do thành viên C bàn giao. Cần tính lại sau Task 3; chưa dùng các tỷ lệ này để kết luận release gate đã PASS.


---

## Phần 2: Kiểm tra thực hành độc lập (10 điểm)

**Nhiệm vụ của Role B:** tính lại conservation và uniqueness trên các output của Role C — Curation, dự đoán kết quả, thực hiện truy vấn và giải thích bằng chứng.

### Kiểm thử đã có trong phần Quality

Log kiểm thử hiện tại ghi **5/5 nhóm PASS**, exit code 0:

| Nhóm kiểm thử | Kết quả cần chứng minh |
|---|---|
| NULL và mẫu số 0 | NULL vẫn nằm trong mẫu số; cohort rỗng không được ghi 100% |
| Conversion, finiteness, unit và range | 32 °F thành 0 °C; NaN/Infinity/K bị loại; email rỗng vẫn hợp lệ; range được kiểm tra trước rounding |
| Winner và tie-break | Phiên bản mới invalid không bị thay bằng phiên bản cũ; đổi thứ tự input vẫn chọn cùng winner |
| Pre-key, chronology và timeliness | Các nhóm loại trước dedup được đếm riêng; timeliness đúng cohort; hàng trễ hợp lệ vẫn được giữ |
| QA coverage và agreement | Reference ID không match làm giảm coverage; measurement lệch reference làm giảm agreement |

Có thể tái lập từ thư mục gốc của project:

```powershell
python code/test_quality.py
python code/run_tasks.py
```

Các kiểm thử này hỗ trợ xác minh logic Quality; chưa phải bằng chứng của một phiên kiểm tra cá nhân độc lập trên artifact của thành viên C.

### Kiểm tra chéo theo trách nhiệm Role B

Khi nhận `curated.parquet`, `quarantine.csv` và `duplicates.csv`, kết quả dự kiến là mỗi physical ID của raw có đúng một terminal disposition, ba output không giao nhau và curated business key duy nhất.

| Kiểm tra | Điều kiện đạt |
|---|---|
| Count reconciliation | `N_raw = N_curated + N_quarantine + N_duplicates` |
| Không mất physical ID | Raw anti-join với hợp ba output trả về 0 hàng |
| Không phát sinh physical ID | Hợp ba output anti-join với raw trả về 0 hàng |
| Output disjointness | Giao từng cặp output theo `(source_object, source_row)` có 0 hàng |
| Physical-key uniqueness | Mỗi output không có nhóm physical key với `count(*) > 1` |
| Business-key uniqueness | Curated không có nhóm `record_id` với `count(*) > 1` |

Hai kiểm tra anti-join và kiểm tra giao tập là cần thiết vì tổng số lượng bằng nhau vẫn có thể che giấu một hàng mất và một hàng bị lặp. Nếu chỉ kiểm tra `count(*)`, chưa đủ chứng minh bảo toàn bản ghi.

**Trạng thái hiện tại:** chưa có ba output Task 3 được bàn giao trong hồ sơ này, nên kiểm tra độc lập trên sản phẩm C chưa thực hiện và chưa ghi PASS. Phiên kiểm tra thực tế cần lưu query, stdout/stderr hoặc kết quả truy vấn, thời gian, người thao tác, reviewer, run ID, input version và artifact hashes vào bằng chứng riêng.

---

## Phần 3: Câu hỏi vấn đáp (10 điểm)

Các câu dưới đây trình bày những quyết định thiết kế của phần Quality để chuẩn bị giải thích. Câu hỏi exit response chính thức được giảng viên giao riêng vào cuối buổi.

### Câu 1

**Vì sao before-quality phải dùng tập winners trước lọc validity, và vì sao uniqueness dùng mẫu số khác?**

Winners đại diện cho các phiên bản được chính sách chọn, bao gồm cả phiên bản lỗi. Nếu lọc các hàng không hợp lệ trước khi tính before-quality, các lỗi sẽ biến mất khỏi mẫu số và báo cáo có thể cho kết quả 100% dù intake vẫn có nhiều lỗi.

Trong lần chạy này, W có 9.960 hàng nhưng chỉ 9.850 hàng đủ trường bắt buộc. Vì vậy completeness before là 9.850/9.960 = 98,8956%. Sau khi chỉ giữ 9.610 hàng hợp lệ, completeness đạt 100% do thay đổi quần thể; không có bằng chứng giá trị thiếu được khôi phục.

Uniqueness trước xử lý phải dùng key-eligible intake trước dedup để thể hiện mức trùng thực tế: 9.960 IDs khác nhau trên 10.160 records, tương ứng 200 duplicate excess. Nếu tính trên W thì uniqueness luôn 100% vì window function đã chọn một hàng cho mỗi business ID.

### Câu 2

**Vì sao chọn winner trước khi kiểm tra validity và không thay winner lỗi bằng phiên bản cũ hợp lệ?**

Contract định nghĩa winner bằng ingest time lớn nhất, sau đó tie-break bằng source object và source row. Đây là chính sách chọn phiên bản, độc lập với việc measurement có hợp lệ hay không.

Nếu một ID có bản cũ hợp lệ và bản mới invalid, lọc validity trước dedup sẽ làm bản cũ được chọn. Hành vi đó che giấu lỗi ở lần cập nhật mới nhất và không đúng policy. Vì vậy, bản mới vẫn là winner và được đánh giá lỗi; bản cũ là superseded version. Kiểm thử `test_latest_invalid_never_falls_back_and_ties_are_stable` xác nhận điều này trên fixture nhỏ.

Tie-break theo source object và source row làm lựa chọn ổn định khi nhiều phiên bản có cùng arrival time. Không dùng thứ tự đọc file hoặc `DISTINCT` không có ordering để chọn winner.

### Câu 3

**Vì sao phải tách QA coverage, QA agreement và timeliness thay vì gộp thành một điểm chất lượng?**

Ba chỉ số trả lời các câu hỏi khác nhau. Coverage đo có bao nhiêu reference IDs được kiểm tra; agreement đo các IDs đã match có gần giá trị tham chiếu hay không; timeliness đo thời gian đến của các hàng chronology hợp lệ.

Projection chẩn đoán có coverage 100/100 và agreement 95/100. Nếu bỏ một ID đang lệch reference, agreement có thể tăng lên 95/99, trong khi coverage giảm còn 99/100. Vì vậy, chỉ nhìn agreement có thể tạo động cơ loại những hàng khó để cải thiện điểm. Hai chỉ số phải được kiểm tra riêng; không dùng reference để sửa hoặc chọn measurement.

Timeliness dùng riêng cohort chronology hợp lệ để tránh diễn giải lag của timestamp lỗi. Trước xử lý, có 70 hàng bị loại khỏi mẫu số timeliness; số đo là 9.690/9.890. Sau lọc chẩn đoán, 200 hàng trễ vẫn được giữ vì measurement hợp lệ, cho tỷ lệ 9.410/9.610. Late không đồng nghĩa measurement sai.

Một điểm trung bình tổng hợp có thể che giấu một quy tắc bắt buộc thất bại dù các chỉ số khác cao. Phần Quality vì vậy báo cáo từng metric cùng numerator, denominator, threshold và status để người đánh giá thấy rõ điều kiện nào đạt hoặc chưa đạt.

---

**Lê Toàn — MSSV: 24022466 — Role B**

**Trạng thái hoàn thiện:** nội dung kỹ thuật, before-quality, phân tích lỗi, biểu đồ và log kiểm thử đã có bằng chứng. After-quality hiện là kết quả chẩn đoán; cần tính lại trên candidate thực tế sau Task 3. Phiên kiểm tra cá nhân độc lập trên artifact của Role C và exit response theo đề riêng cần bổ sung khi thực sự thực hiện.
