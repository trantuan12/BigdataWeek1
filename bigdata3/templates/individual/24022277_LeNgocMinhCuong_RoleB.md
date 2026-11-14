# BÁO CÁO CÁ NHÂN — INDIVIDUAL ASSESSMENT

**Học phần:** Cloud-Native Big Data Infrastructure & Governance  
**Bài thực hành:** Lab 3 — Lakehouse Processing & Analytics  
**Package Revision:** 18 September 2026

| Thông tin | Nội dung |
|---|---|
| Họ và tên | {{MEMBER_B_NAME}} |
| MSSV | {{MEMBER_B_ID}} |
| Vai trò | Role B — Silver/Gold Modeling & Aggregation Semantics |
| Nhiệm vụ chính | Task 2 — Materialize Bronze, Silver and Gold; implement `build_gold`; verify `B01`–`B04` & weighted rollup semantics |
| Nhiệm vụ chéo | Cross-check C: Verify Member C's stale and tied revision results (`B_CHECK_C`) |
| Nhóm thực hiện | `{{GROUP_ID}}` |
| Run Label | `{{RUN_LABEL}}` |
| Context / Chế độ chạy | Spark standalone: hai worker container, S3A dùng chung (Spark {{SPARK}}, Delta-Spark {{DELTA}}, Python {{PYTHON}}, Java {{JAVA}}, Hadoop {{HADOOP}}) |
| Trusted Manifest SHA-256 | `{{TRUSTED_SHA}}` |
| Upstream Logical SHA-256 | `{{HANDOFF_LOGICAL_SHA}}` |
| Run ID Bootstrap / Incremental | `{{BOOTSTRAP_RUN_ID}}` / `{{INCREMENTAL_RUN_ID}}` |
| Cặp snapshot mô hình hóa | Bronze v{{BRONZE_INIT}} → v{{BRONZE_FINAL}}; Silver v{{SILVER_INIT}} → v{{SILVER_FINAL}} (replay v{{SILVER_REPLAY}}); Gold v{{GOLD_INIT}} → v{{GOLD_FINAL}} (ghim từ Silver v{{GOLD_SOURCE_SILVER}}) |

---

## Phần 1: Đóng góp kỹ thuật (10 điểm)

### 1.1 Trước khi thực hiện (Bối cảnh và thách thức)

Sau khi Thành viên A xác nhận tính toàn vẹn của bản bàn giao thượng nguồn từ Lab 2 (`{{BASELINE_ROWS}}` quan sát đã qua kiểm định hợp đồng `research-telemetry-v1`), hệ thống Lakehouse ở Task 2 phải hiện thực hóa ba tầng bảng Delta (`Bronze`, `Silver`, `Gold`) với ngữ nghĩa tách biệt rõ ràng và xây dựng hàm tổng hợp `build_gold` phục vụ cả truy vấn phân tích lẫn bản phát hành có ghim phiên bản.

Quá trình mô hình hóa Silver/Gold và thiết kế `build_gold` đặt ra bốn thách thức kỹ thuật cốt lõi:
1. **Phân tách ngữ nghĩa ba tầng bảng (Three-Layer Lakehouse Semantics — `B01`, `B02`):**
   - **Bronze** là tầng biên nhận vật lý (*receipt layer*) tại ranh giới pipeline, được định danh duy nhất bởi bộ ba provenance `(source_object, source_row, source_sha256)` kèm `receipt_batch_id`. Bronze lưu giữ mọi phiên bản vật lý hợp lệ (kể cả các bản sửa đổi bị thay thế hoặc cũ hơn), nhưng không chèn trùng cùng một biên nhận khi phát lại.
   - **Silver** lưu đúng **một bản ghi hiện hành cho mỗi khóa nghiệp vụ `record_id`**, bảo toàn nguyên vẹn 10 cột phát hành từ Lab 2, tuyệt đối **không loại bỏ các quan sát đến trễ hợp lệ (`is_late = true`)**, và chỉ bổ sung duy nhất cột ngày lịch `event_date` tính theo chuẩn múi giờ **UTC** từ `event_time_utc`.
   - **Gold** là bảng tổng hợp theo từng cặp `(site, event_date)` được tái tạo tường minh từ một phiên bản Silver đã ghim (`versionAsOf`), không tự động cập nhật ngầm và không nối thêm (append) chồng chéo khi Silver thay đổi.
2. **Độ chính xác kiểu dữ liệu và công thức tổng hợp (`Gold Calculation Contract`):** Bảng Gold bắt buộc trả về đúng **11 cột** theo đúng thứ tự và kiểu dữ liệu trong [`schemas/gold_schema.json`](../../schemas/gold_schema.json): các số đếm (`n_observations`, `n_late`, `n_above_25`, `n_sensors`) dùng `BIGINT`; các tổng và cực trị (`sum_temperature_c`, `min_temperature_c`, `max_temperature_c`) dùng `DECIMAL(18,2)`; các trung bình và tỷ lệ (`mean_temperature_c`, `late_rate`) dùng `DECIMAL(18,6)`. Ngưỡng nhiệt độ cao phải là bất đẳng thức nghiêm ngặt **`temperature_c > 25`** (không tính giá trị đúng bằng `25.00 °C`), và `n_sensors` phải đếm số cảm biến phân biệt `COUNT(DISTINCT sensor_id)` trong phạm vi từng `(site, event_date)`.
3. **Bảo toàn thành phần cộng tính và giải bài toán trung bình có trọng số (`B03`, `B04`):** Một trung bình nhóm (`mean_temperature_c`) hay số đếm phân biệt (`n_sensors`) không phải là đại lượng cộng tính qua các nhóm có kích thước mẫu khác nhau. Do đó, Gold phải lưu đồng thời tử số cộng tính (`sum_temperature_c`) và mẫu số (`n_observations`) để mọi phép cuộn dữ liệu (rollup) theo ngày hoặc toàn kỳ tính được chính xác **trung bình có trọng số quan sát** (`SUM(sum_temperature_c) / SUM(n_observations)`), tránh sai số khi lấy trung bình cộng đơn giản của các trung bình site (`AVG(mean_temperature_c)`).
4. **Ràng buộc thực thi phân tán thuần Spark SQL:** Hàm `build_gold` không được gọi `.collect()`, không chuyển sang Pandas (`toPandas()`), không dùng Python UDF cho các phép tính số học cơ bản, và tuyệt đối không `JOIN` với bộ mẫu `qa_reference.csv` để sửa giá trị nhiệt độ quan sát.

Vai trò của tôi ({{MEMBER_B_NAME}} — Thành viên B) là phụ trách mô hình hóa Silver/Gold, cài đặt hàm `build_gold` trong [`templates/student_impl.py`](../../templates/student_impl.py), chứng minh đầy đủ các tiêu chí `B01`–`B04` trên cả hai trạng thái nền (`v0`) và sau cập nhật (`v1`), đồng thời thực hiện kiểm tra chéo độc lập đối với kết quả xử lý bản ghi cũ/đồng hạng (`stale/tied revisions`) của Thành viên C (`B_CHECK_C`).

---

### 1.2 Công việc kỹ thuật đã thực hiện

#### 1. Hiện thực hóa ba tầng bảng Delta và xác minh ngữ nghĩa Silver (`B01`, `B02`)

- Ba bảng Delta thực tế được khởi tạo ở bước `bootstrap` (`v0`) và cập nhật ở bước `incremental` (`v1`) trên object store dùng chung S3A (`s3a://research-lakehouse/lab3/{{GROUP_ID}}/{{RUN_LABEL}}/tables/...`). Lịch sử commit và trạng thái tại phiên bản ghim được xác minh trực tiếp qua [`evidence/live_table_verification.json`](../../evidence/live_table_verification.json) và [`evidence/role_B_gold_model.json`](../../evidence/role_B_gold_model.json):

{{TABLE_LAYERS_SUMMARY}}

- **Kiểm chứng `B02` (Tính tương đương logic với Handoff và ngày lịch UTC):**
  - Ở phiên bản nền `Silver v0`, toàn bộ **9,610 dòng** có mã băm chính quy `c50f8252bfb91890975d26b16fe1d2b32f0e283a58e46bd16260b4a3cca0bf0d`, khớp tuyệt đối 100% với tệp bàn giao thượng nguồn (`data/handoff/curated.csv`). Toàn bộ **200 quan sát đến trễ hợp lệ** (`is_late = true`, khi `ingest_time_utc - event_time_utc > 900s`) ở `v0` và **220 quan sát đến trễ** ở `v1` đều được giữ nguyên trong Silver và đưa vào mẫu số `n_observations` của Gold.
  - Cột `event_date` được suy dẫn trong phiên Spark đã ghim `spark.sql.session.timeZone = UTC`. Kiểm tra độ nhạy múi giờ trong `tools/check_role_b.py` chứng minh: nếu tính `event_date` theo giờ địa phương `UTC+7` thay vì `UTC`, có tới **2,520 / 9,710 bản ghi** (các sự kiện diễn ra từ `17:00:00Z` đến `23:59:59Z`) bị đẩy sang ngày kế tiếp, làm thay đổi giá trị của **16 / 20 nhóm `(site, event_date)`** trong bảng Gold. Việc ép chuẩn UTC đảm bảo tính tất định của khóa nhóm `event_date` trên mọi máy trạm.

#### 2. Cài đặt hàm `build_gold` tuân thủ hợp đồng 11 cột và kiểu dữ liệu (`B01`, `B03`)

- Hàm [`build_gold`](../../templates/student_impl.py) thực hiện gom nhóm phân tán theo `('site', 'event_date')`, tính toán các thành phần nguyên tử trong một bước `.agg(...)`, sau đó dẫn xuất `mean_temperature_c` và `late_rate` trực tiếp từ các cột tổng/đếm vừa tính để đảm bảo tính nhất quán nội tại:

```python
{{BUILD_GOLD_FUNCTION}}
```

- **Kiểm toán tĩnh mã nguồn (Static AST Audit) và xác nhận mã thực thi (`B01_executed_build_gold_code_binding`):**
  - Phân tích cây cú pháp trừu tượng (AST) của `build_gold` xác nhận tập lời gọi hàm chỉ gồm `['agg', 'alias', 'cast', 'col', 'count', 'countDistinct', 'groupBy', 'lit', 'max', 'min', 'otherwise', 'select', 'sum', 'when']` — hoàn toàn không chứa bất kỳ lệnh cấm nào (`collect`, `toPandas`, `udf`, `pandas_udf`, `join`).
  - Mã băm SHA-256 chuẩn hóa LF của `templates/student_impl.py` khớp tuyệt đối với mã đã thực thi trong pipeline (`{{CODE_SHA}}`) và bản lưu trữ tại [`evidence/code_versions/{{CODE_SHA}}.py`](../../evidence/code_versions/{{CODE_SHA}}.py).
- **Đối chiếu lược đồ 11 cột thực tế của bảng Delta Gold với `schemas/gold_schema.json` (`B01_exact_schema_types`):**

{{GOLD_SCHEMA_TABLE}}

#### 3. Chứng minh định luật bảo toàn thành phần cộng tính giữa Silver và Gold (`B03`)

- Kiểm tra `B03_conservation_counts_and_sums` đối chiếu trực tiếp tổng các thành phần cộng tính của bảng Gold với dữ liệu chi tiết của bảng Silver trên cả hai snapshot `v0` (trước khi áp dụng batch 02) và `v1` (sau khi áp dụng batch 02):

{{CONSERVATION_TABLE}}

- Toàn bộ **20 nhóm `(site, event_date)`** của `Gold v1` (4 site × 5 ngày UTC từ `2026-02-02` đến `2026-02-06`) khớp tuyệt đối 100% trên cả 11 trường với bộ tính toán độc lập bằng `Decimal` trong `tools/check_role_b.py`:

{{GOLD_V1_TABLE}}

#### 4. Giải thích `B04` & Phần trình bày 2 phút của Role B: Trung bình ngày có trọng số quan sát vs. Trung bình cộng các trung bình site

- **Cơ sở toán học (`B04`):** Giả sử trong một ngày cố định $d$, có $K = 4$ site với số lượng quan sát lần lượt là $n_1, n_2, \dots, n_K$ và tổng nhiệt độ tương ứng là $S_1, S_2, \dots, S_K$. Trung bình nhiệt độ của từng site là $\bar{T}_k = \frac{S_k}{n_k}$.
  - **Trung bình ngày có trọng số quan sát (Observation-weighted daily mean):** Mỗi phép đo vật lý có trọng số đóng góp ngang nhau ($1/N$ với $N = \sum_{k=1}^K n_k$), nên trung bình thực sự của toàn bộ các quan sát trong ngày là:
    $$\bar{T}_{\text{weighted}}(d) = \frac{\sum_{k=1}^K S_k}{\sum_{k=1}^K n_k} = \sum_{k=1}^K \left(\frac{n_k}{N}\right) \bar{T}_k$$
  - **Trung bình không trọng số của các trung bình site (Unweighted/naive mean of site means):** Gán cho mỗi site cùng một trọng số cố định $1/K = 1/4$ bất kể site đó có nhiều hay ít quan sát:
    $$\bar{T}_{\text{naive}}(d) = \frac{1}{K} \sum_{k=1}^K \bar{T}_k = \frac{1}{K} \sum_{k=1}^K \frac{S_k}{n_k}$$
  - **Điều kiện bằng nhau và lệch nhau:** Hai công thức chỉ luôn bằng nhau khi mọi site có cùng số lượng quan sát ($n_1 = n_2 = \dots = n_K = \frac{N}{K}$, khi đó $\frac{n_k}{N} = \frac{1}{K}$). Khi kích thước mẫu $n_k$ giữa các site không đều nhau, $\bar{T}_{\text{naive}}$ sẽ phóng đại ảnh hưởng của các site có ít quan sát và làm giảm ảnh hưởng của các site có nhiều quan sát. Tương tự, khi cuộn dữ liệu trên toàn kỳ 5 ngày (20 nhóm site/ngày), trung bình của 20 giá trị `mean_temperature_c` (`22.677356 °C`) bị kéo lệch mạnh về phía ngày `2026-02-06` (vốn chỉ có 25 quan sát/site ở nhiệt độ thấp `21.30`–`21.60 °C`), trong khi trung bình có trọng số thực sự của cả 9,710 quan sát là `222930.50 / 9710 = 22.958857 °C`.
- **Đối chiếu định lượng từng ngày từ các thành phần `sum_temperature_c` và `n_observations` của `Gold v1` (`B04_weighted_vs_naive_from_gold_components`):**

{{DAILY_WEIGHTED_TABLE}}

- **Phân tích cụ thể ngày `2026-02-02` (Dùng cho phần trình bày 2 phút của Role B):**
  - Trong ngày `2026-02-02`, số quan sát của 4 site không bằng nhau: `zone-1` có **625** quan sát (`sum = 13972.50`, `mean = 22.356000`), `zone-2` có **625** quan sát (`sum = 14293.00`, `mean = 22.868800`), `zone-3` có **620** quan sát (`sum = 14508.00`, `mean = 23.400000`), và `zone-4` có **619** quan sát (`sum = 14510.00`, `mean = 23.441034`).
  - Tính từ thành phần cộng tính của Gold:
    $$\bar{T}_{\text{weighted}}(\text{2026-02-02}) = \frac{13972.50 + 14293.00 + 14508.00 + 14510.00}{625 + 625 + 620 + 619} = \frac{57283.50}{2489} \approx \mathbf{23.014665\ ^\circ\text{C}}$$
  - Trong khi đó, trung bình không trọng số của 4 trung bình site là:
    $$\bar{T}_{\text{naive}}(\text{2026-02-02}) = \frac{22.356000 + 22.868800 + 23.400000 + 23.441034}{4} = \frac{92.065834}{4} \approx \mathbf{23.016459\ ^\circ\text{C}}$$
  - Vì `zone-3` (`620` dòng) và `zone-4` (`619` dòng) có nhiệt độ trung bình cao hơn (`~23.40`–`23.44 °C`) nhưng lại có ít quan sát hơn `zone-1` và `zone-2` (`625` dòng, `~22.36`–`22.87 °C`), phép tính `AVG(mean_temperature_c)` đã cấp dư trọng số ($1/4 = 25\%$ thay vì $619/2489 \approx 24.87\%$) cho hai site nóng hơn, làm kết quả bị lệch dương **`+0.001794 °C`**. Ngược lại, ở các ngày `2026-02-03` (`720` dòng/site), `2026-02-04` (`720` dòng/site) và `2026-02-06` (`25` dòng/site), do cả 4 site có số quan sát bằng hệt nhau nên hai công thức cho kết quả trùng khớp (`difference = 0.000000 °C`).

---

### 1.3 Kết quả kiểm định các tiêu chí Task 2 (`B01`–`B04`)

| Mã kiểm tra | Nội dung kiểm định | Bằng chứng quan sát thực tế | Trạng thái |
|:---:|---|---|:---:|
| **B01** | Ba bảng Delta, URI, phiên bản và lược đồ kiểu dữ liệu | `bronze` (`v0→v1`, 9,750 dòng), `silver` (`v0→v1`, 9,710 dòng, 11 cột), `gold` (`v0→v1`, 20 nhóm, 11 cột) khớp `gold_schema.json`; SHA-256 LF code khớp `{{CODE_SHA}}` | **PASS** |
| **B02** | Tương đương logic Handoff & suy dẫn `event_date` theo UTC | `Silver v0` khớp 100% handoff (`9,610` dòng, SHA-256 `c50f8252...`); giữ đủ `200`/`220` dòng `is_late`; kiểm tra độ nhạy `UTC+7` chứng minh 2,520 dòng sẽ lệch ngày nếu sai múi giờ | **PASS** |
| **B03** | Bảo toàn số đếm, tổng nhiệt độ và công thức nội tại Gold | Bảo toàn tuyệt đối ở cả `v0` (`9,610` dòng, `220,745.50 °C`) và `v1` (`9,710` dòng, `222,930.50 °C`); kiểm toán AST xác nhận không dùng `collect`/`toPandas`/`udf`/`join` | **PASS** |
| **B04** | Giải thích và tính lại trung bình có trọng số vs. mean-of-means | Ngày `2026-02-02`: `57283.50 / 2489 = 23.014665 °C` vs `23.016459 °C` (`+0.001794 °C`); ngày `2026-02-05`: `31350.00 / 1361 = 23.034533 °C` vs `23.034212 °C` (`-0.000321 °C`) | **PASS** |
| **B→C** | Kiểm tra chéo kết quả bản ghi cũ (stale) và đồng hạng (tied) của C | 10/10 khóa stale (`R003001`–`R003010`) giữ nguyên bản ghi `v0`; 10/10 khóa tied chọn đúng `source_row` nhỏ hơn; bất biến qua 200 lần xáo trộn và khớp bảng chân trị SQL | **PASS** |

**Các tệp bằng chứng kỹ thuật nộp kèm:**
- [`role_B_prediction.json`](../../evidence/role_B_prediction.json), [`role_B_gold_model.json`](../../evidence/role_B_gold_model.json), [`role_B_check.json`](../../evidence/role_B_check.json), [`role_B_unit_tests.json`](../../evidence/role_B_unit_tests.json) — Dự đoán trước khi chạy, kết quả kiểm tra mô hình `B01`–`B04`, kết quả kiểm tra chéo `B_CHECK_C` (**{{N_CHECKS}}/{{N_CHECKS}} checks PASS**) và bộ 5 unit tests của Role B.
- [`bootstrap.json`](../../evidence/bootstrap.json), [`state.json`](../../evidence/state.json), [`live_table_verification.json`](../../evidence/live_table_verification.json) — Trạng thái khởi tạo ba bảng Delta, phiên bản ghim và xác minh trực tiếp lịch sử giao dịch Delta.
- [`gold_baseline.json`](../../evidence/gold_baseline.json), [`gold_after_batch02.json`](../../evidence/gold_after_batch02.json), [`Q2_weighted_day.json`](../../evidence/Q2_weighted_day.json) — Kết quả xuất bảng Gold ở `v0` (16 nhóm), `v1` (20 nhóm) và bảng đối chiếu trung bình ngày có trọng số.

---

## Phần 2: Kiểm tra chéo độc lập (10 điểm)

### Thao tác được chỉ định: Check Member C's stale/tied revision result (`B_CHECK_C`)
*(Kiểm tra độc lập kết quả xử lý bản ghi cũ và bản ghi đồng hạng thời gian của Thành viên C, cùng tác động của chúng lên bảng Gold)*

#### 2.1 Mục tiêu kiểm tra

Thành viên C ({{MEMBER_C_NAME}} — MSSV `{{MEMBER_C_ID}}`) phụ trách cài đặt hai hàm `choose_revisions` và `merge_silver` ở Task 3 để xử lý lô sửa đổi `data/changes/changes_batch02.csv` (**140 dòng vật lý**). Vì bảng Gold `v1` do tôi (`build_gold`) tính toán phụ thuộc trực tiếp vào trạng thái của bảng Silver `v1` sau khi hợp nhất, bất kỳ sai sót nào trong việc chọn winner đồng hạng (tied revisions) hoặc để lọt bản ghi cũ (stale revisions) ghi đè lên bản ghi mới hơn đều sẽ làm sai lệch trực tiếp `sum_temperature_c`, `mean_temperature_c` và `min_temperature_c` của bảng Gold.

Mục tiêu kiểm tra chéo của tôi (Role B) là thiết lập một bộ đối chiếu hoàn toàn độc lập (không sử dụng hàm tuple-key của Role C mà cài đặt bộ so sánh phân nhánh tường minh `precedes(incoming, current)` và thực thi trực tiếp biểu thức SQL `precedes` của `merge_silver` trên SQLite in-memory) để xác minh 7 tiêu chí (`B_C01`–`B_C07`):
1. **Stale revisions (`B_C01`):** Toàn bộ 10 khóa có bản ghi đến trễ bị cũ (`R003001`–`R003010`) phải bị chặn hoàn toàn ở Silver `v1` (giữ nguyên giá trị và provenance của Silver `v0`), nhưng biên nhận vật lý của chúng vẫn phải được lưu đầy đủ ở tầng Bronze (`9,750` dòng).
2. **Same-time tied revisions (`B_C02`):** Toàn bộ 10 khóa có 2 dòng vật lý trùng `ingest_time_utc` và trùng `source_object` trong `changes_batch02.csv` (`R002001`–`R002005` và `R010001`–`R010005`) phải chọn đúng dòng có `source_row` nhỏ hơn làm winner, và dòng bị thay thế (`superseded`) tuyệt đối không được xuất hiện trong Silver `v1`.
3. **Tính bất biến theo thứ tự đầu vào (`B_C03`):** Kết quả chọn winner của cả 130 khóa phải bất biến khi đảo thứ tự từng cặp đồng hạng, đảo ngược toàn bộ danh sách, hoặc xáo trộn ngẫu nhiên 200 lần.
4. **Kiểm tra bảng chân trị của vị từ SQL `precedes` (`B_C04`):** Vì lô `changes_batch02.csv` thực tế chỉ có trường hợp đồng hạng *giữa các dòng cùng đến trong batch* chứ không có trường hợp `incoming.ingest_time_utc == current_silver.ingest_time_utc`, cần trích xuất nguyên văn chuỗi SQL `precedes` từ `merge_silver` của Role C và kiểm thử trên toàn bộ 8 nhánh chân trị (bao gồm cả nhánh hòa `ingest_time_utc` nhưng khác `source_object` hoặc khác `source_row`).
5. **Tính ổn định khi phát lại (`B_C05`) và Độ nhạy ngược lên bảng Gold (`B_C06`, `B_C07`):** Chứng minh rằng khi replay lần hai (`Silver v2`), 10 khóa stale vẫn là `unchanged_stale` và 10 khóa tied trở thành `unchanged_equal`; đồng thời định lượng chính xác mức sai lệch nhiệt độ sẽ xảy ra trên bảng Gold `v1` nếu Role C xử lý sai stale hoặc tied.

#### 2.2 Kịch bản và Dự đoán

Trước khi đọc các tệp xuất Silver của Role C, tôi ghi nhận dự đoán định lượng tại [`evidence/role_B_prediction.json`](../../evidence/role_B_prediction.json) (lúc `{{PREDICTED_AT}}`) dựa trên việc phân tích trực tiếp tệp gốc `curated.csv` và `changes_batch02.csv`:
1. **Dự đoán cho 10 khóa Stale (`R003001`–`R003010`):** Trong `changes_batch02.csv` (dòng `121`–`130`), cả 10 khóa này đều có `ingest_time_utc` sớm hơn **30 giây** và nhiệt độ thấp hơn **`1.00 °C`** so với bản ghi đang lưu trong `curated.csv` (dòng `3001`–`3010`). Ví dụ với khóa `R003005` (`sensor_id = S05`, `site = zone-1`, ngày `2026-02-03`): bản ghi đến ở dòng `125` có `ingest_time_utc = 2026-02-03T01:03:00Z` và `temperature_c = 17.50 °C`, cũ hơn bản ghi Silver `v0` ở dòng `3005` (`ingest_time_utc = 2026-02-03T01:03:30Z`, `temperature_c = 18.50 °C`). Dự đoán: Silver `v1` giữ nguyên dòng `3005` (`18.50 °C`) cho `R003005` và giữ nguyên toàn bộ 10 khóa `R003001`–`R003010`.
2. **Dự đoán cho 10 khóa Tied (`R002001`–`R002005` và `R010001`–`R010005`):**
   - Nhóm 5 khóa đã tồn tại `R002001`–`R002005` (`site = zone-1`, ngày `2026-02-02`): mỗi khóa xuất hiện 2 lần trong `changes_batch02.csv` (một dòng ở `101`–`105` và một dòng ở `136`–`140`) với cùng `ingest_time_utc` và cùng `source_object = 'lab3/source_batch02.csv'`. Ví dụ `R002005` có dòng `105` (`20.50 °C`) và dòng `140` (`21.50 °C`) cùng ở `2026-02-02T16:52:30Z`. Dự đoán: `choose_revisions` chọn dòng `105` (`105 < 140`), sau đó `merge_silver` so sánh dòng `105` (`ingest = 16:52:30Z`) với bản ghi cũ trong Silver `v0` (`observations_a.csv` dòng `2005`, `ingest = 16:43:30Z`, `18.50 °C`) và quyết định `updated_newer` thành `20.50 °C`.
   - Nhóm 5 khóa mới `R010001`–`R010005` (`site = zone-1`, ngày `2026-02-06`): mỗi khóa xuất hiện ở dòng `1`–`5` (`19.10`–`19.50 °C`) và dòng `131`–`135` (`20.10`–`20.50 °C`) với cùng `ingest_time_utc`. Dự đoán: các dòng `1`–`5` thắng nhờ `source_row` nhỏ hơn và được `inserted` vào Silver `v1`.
3. **Dự đoán tác động ngược lên bảng Gold `v1` (`Gold Sensitivity`):**
   - Nếu `merge_silver` thiếu điều kiện chặn stale và để 10 dòng `121`–`130` ghi đè: tổng nhiệt độ ngày `2026-02-03` của `zone-1` (chứa `R003001`–`R003005`, cảm biến `S01`–`S05`) sẽ bị sụt giảm **`5.00 °C`** (từ `16046.00` xuống `16041.00 °C`), và của `zone-2` (chứa `R003006`–`R003010`, cảm biến `S06`–`S10`) cũng sụt giảm **`5.00 °C`** (từ `16406.00` xuống `16401.00 °C`).
   - Nếu `choose_revisions` lấy dòng xuất hiện cuối (`131`–`140`) thay vì dòng có `source_row` nhỏ nhất: tổng nhiệt độ của `zone-1` ngày `2026-02-02` sẽ bị đội lên **`+5.00 °C`** (từ `13972.50` lên `13977.50 °C`) và của `zone-1` ngày `2026-02-06` bị đội lên **`+5.00 °C`** (từ `532.50` lên `537.50 °C`).

#### 2.3 Thực thi kiểm chứng độc lập

Chạy công cụ kiểm tra tự động của Role B (chỉ đọc dữ liệu, không ghi bảng Delta):

```powershell
python tools/check_role_b.py --config config/team.json
```

Đoạn mã Python dưới đây minh họa trực tiếp cả hai nhiệm vụ của Role B: (1) tính lại trung bình ngày có trọng số từ các thành phần cộng tính của Gold so với trung bình không trọng số, và (2) kiểm tra chéo kết quả xử lý bản ghi stale/tied của Role C:

```python
import json
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
import sys
sys.path[:0] = ['src', 'tools']
from common import load_csv, load_registry, read_json, validate_rows
from check_role_b import pick_winner, precedes

# 1. Role B Demo: Tính lại trung bình có trọng số từ thành phần Gold vs. Unweighted mean of site means
gold_v1 = read_json(Path('evidence/gold_after_batch02.json'))
feb02_groups = [g for g in gold_v1 if g['event_date'] == '2026-02-02']
total_temp = sum(Decimal(g['sum_temperature_c']) for g in feb02_groups)
total_obs = sum(g['n_observations'] for g in feb02_groups)
weighted_mean = (total_temp / total_obs).quantize(Decimal('0.000001'), rounding=ROUND_HALF_UP)
naive_mean = (sum(Decimal(g['mean_temperature_c']) for g in feb02_groups) / len(feb02_groups)).quantize(
    Decimal('0.000001'), rounding=ROUND_HALF_UP)
assert (total_temp, total_obs, str(weighted_mean), str(naive_mean)) == (
    Decimal('57283.50'), 2489, '23.014665', '23.016459')

# 2. Cross-check B_CHECK_C: Kiểm tra độc lập Stale (R003005) và Tied (R002005, R010005) của Role C
registry = load_registry(Path('data/handoff/sensors.csv'))
base_by_id = {r['record_id']: r for r in validate_rows(load_csv('data/handoff/curated.csv'), registry, True)}
incoming = validate_rows(load_csv('data/changes/changes_batch02.csv'), registry, False)
silver_v1 = {json.loads(line)['record_id']: json.loads(line)
             for line in Path('evidence/silver_after_batch02.jsonl').read_text(encoding='utf-8').splitlines()}

stale_in = next(r for r in incoming if r['record_id'] == 'R003005')
assert not precedes(stale_in, base_by_id['R003005']) and silver_v1['R003005'] == base_by_id['R003005']

tied_2005 = [r for r in incoming if r['record_id'] == 'R002005']
winner_2005 = pick_winner(tied_2005)
assert winner_2005['source_row'] == 105 and precedes(winner_2005, base_by_id['R002005'])
assert silver_v1['R002005'] == winner_2005
print('PASS Role B Demo & B_CHECK_C:', weighted_mean, 'vs', naive_mean, '| R003005 row=3005 | R002005 row=105')
```

#### 2.4 Kết quả quan sát và Đối chiếu

**Bảng 1 — Kiểm tra toàn bộ 10 khóa Stale (`B_C01_stale_revisions_never_overwrite`):**

{{STALE_REVISIONS_TABLE}}

**Bảng 2 — Kiểm tra toàn bộ 10 khóa Tied có nhiều dòng cùng thời gian tiếp nhận trong Batch 02 (`B_C02_same_time_ties_smaller_source_row_wins`):**

{{TIED_REVISIONS_TABLE}}

**Bảng 3 — Kiểm thử bảng chân trị biểu thức SQL `precedes` của `merge_silver` trên SQLite (`B_C04_actual_merge_predicate_truth_table`):**

{{PREDICATE_TRUTH_TABLE}}

**Bảng 4 — Phân tích độ nhạy ngược lên bảng Gold `v1` nếu xử lý sai Stale hoặc Tied (`B_C06_gold_reflects_correct_stale_and_tie_decisions`):**

{{GOLD_SENSITIVITY_TABLE}}

Quá trình kiểm tra hoàn tất lúc `{{COMPLETED_AT}}`, vượt qua tuyệt đối **{{N_CHECKS}}/{{N_CHECKS}} kiểm tra tự động** (gồm 7 kiểm tra mô hình `B01`–`B04` và 7 kiểm tra chéo `B_C01`–`B_C07`), cùng **{{N_UNIT_TESTS}}/{{N_UNIT_TESTS}} unit tests** của Role B và **{{N_TESTS}}/{{N_TESTS}} integration tests** của toàn nhóm:
- Cả 10 khóa stale (`R003001`–`R003010`) đều được giữ nguyên ở phiên bản `observations_a.csv` (`source_row` `3001`–`3010`), trong khi tầng Bronze vẫn ghi nhận đủ 140 biên nhận mới (`9,610 + 140 = 9,750` dòng).
- Cả 10 khóa tied đều chọn đúng dòng có `source_row` nhỏ hơn (`101`–`105` cho `R002001`–`R002005` và `1`–`5` cho `R010001`–`R010005`), bất biến tuyệt đối qua 200 lần xáo trộn ngẫu nhiên.
- Khi phát lại lần hai (`Silver v2`), 10 khóa stale giữ nguyên trạng thái `unchanged_stale`, 10 khóa tied chuyển sang trạng thái `unchanged_equal` (no-op), và bảng Gold `v1` vẫn được ghim chặt vào `gold_source_silver_version = 1`.

#### 2.5 Kết luận kiểm tra chéo

Hai hàm `choose_revisions` và `merge_silver` do Thành viên C cài đặt đã xử lý chính xác tuyệt đối mọi ca bản ghi cũ (`stale`) và đồng hạng thời gian (`same-time ties`), cả trên dữ liệu thực của `changes_batch02.csv` lẫn trên các nhánh biên lý thuyết trong bảng chân trị SQL. Nhờ đó, bảng Silver `v1` (`9,710` dòng) cung cấp đầu vào hoàn toàn chính xác để `build_gold` tạo ra bảng Gold `v1` (`20` nhóm, tổng nhiệt độ `222,930.50 °C`).

---

## Phần 3: Trả lời câu hỏi phỏng vấn thoát (10 điểm)

### Câu 1: Tại sao một thao tác merge không thay đổi dữ liệu (no-op merge) vẫn có thể ghi thêm một phiên bản commit mới vào lịch sử Delta mà không vi phạm tính lũy đẳng (idempotence)?

**Trả lời:**  
Tính lũy đẳng của pipeline Lakehouse yêu cầu việc áp dụng lặp lại cùng một lô sửa đổi không làm thay đổi **nội dung logic của các bản ghi được chọn** và **các chỉ số tổng hợp đầu ra**, chứ không yêu cầu số thứ tự phiên bản vật lý trong `_delta_log` phải đứng yên. Khi thực thi `merge_silver` lần thứ hai lên `Silver v1`, mọi dòng trong `winners_df` đều rơi vào nhánh không thỏa vị từ cập nhật `precedes` (120 khóa có định danh và thời gian trùng hệt nên `precedes = False`, 10 khóa stale có `ingest_time_utc` nhỏ hơn nên `precedes = False`). Tuy nhiên, thao tác `MERGE` của Delta Lake vẫn thực hiện viết lại tệp dữ liệu đích theo cơ chế copy-on-write (`numTargetRowsCopied = 9710`, `numTargetRowsInserted = 0`, `numTargetRowsMatchedUpdated = 0`) và tạo ra commit `Silver v2`. Vì tập bản ghi của `v2` và `v1` có cùng mã băm chính quy `{{SILVER_AFTER_SHA}}`, bất kỳ phép tính `build_gold` nào đọc `v1` hay `v2` cũng cho ra đúng 20 nhóm site/ngày với cùng tổng nhiệt độ `222,930.50 °C`. Trong metadata phát hành, bảng Gold `v1` vẫn phải được ghim tường minh với `gold_source_silver_version = 1` là phiên bản thực tế mà nó đã đọc.

---

### Câu 2: Tại sao một kiểm tra miền giá trị hợp lệ (passing range check) không đồng nghĩa với việc xác lập tính chính xác của phép đo (measurement accuracy)?

**Trả lời:**  
Kiểm tra miền giá trị (`-30.00 <= temperature_c <= 60.00`, đúng định dạng `DECIMAL(8,2)`, thời gian hợp lệ `event_time_utc <= ingest_time_utc <= AS_OF`) chỉ chứng minh dữ liệu **hợp lệ về mặt cú pháp và miền ràng buộc hợp đồng** (contract compliance), giúp các phép cộng và chia trong `build_gold` không gặp lỗi tràn số hay `NULL`. Nó không chứng minh cảm biến vật lý tại hiện trường đã hiệu chuẩn đúng hay phản ánh đúng nhiệt độ thực của môi trường. Trong dữ liệu kế thừa từ Lab 2, một số quan sát hoàn toàn hợp lệ vẫn có chênh lệch nhất định so với mẫu tham chiếu `qa_reference.csv`. Theo đúng quy tắc quản trị dữ liệu của bài lab, `build_gold` tuyệt đối không được `JOIN` với `qa_reference.csv` để thay thế hay "sửa" nhiệt độ quan sát; các giới hạn chất lượng này phải được công bố minh bạch trong catalog và chú thích biểu đồ.

---

### Câu 3: Tại sao tập dữ liệu benchmark nhân bản tổng hợp (replica-expanded benchmark) bắt buộc phải nằm ngoài các bảng Gold vận hành (operational Gold)?

**Trả lời:**  
Tập dữ liệu benchmark (`971,000` dòng) được nhân bản gấp 100 lần từ `9,710` dòng của `Silver v1` bằng cách ghép thêm cột `replica_id` (`0..99`) chỉ để tạo khối lượng dữ liệu đủ lớn cho thí nghiệm so sánh bố trí vật lý Layout A và Layout B. Về mặt ngữ nghĩa thống kê, 100 bản sao của cùng một `record_id` không phải là 100 quan sát nhiệt độ độc lập. Nếu đưa dữ liệu nhân bản này vào `build_gold` hoặc ghi đè vào bảng Gold vận hành, các thành phần cộng tính sẽ bị phóng đại sai sự thật gấp 100 lần (`n_observations` từ `9,710` thành `971,000`, `sum_temperature_c` từ `222,930.50 °C` thành `22,293,050.00 °C`, `n_late` từ `220` thành `22,000`). Dù `mean_temperature_c` có thể không đổi khi cả tử và mẫu cùng nhân 100, nhưng mẫu số thống kê, cỡ mẫu $n$ và tính toàn vẹn provenance của bản phát hành đã bị phá hỏng hoàn toàn. Do đó, các bảng benchmark phải nằm ở đường dẫn tách biệt và không bao giờ đi vào bảng Gold vận hành.

---

### Câu 4: Tại sao một manifest phát hành (release manifest) không phải là một giao dịch đa bảng/đa đối tượng (cross-table transaction)?

**Trả lời:**  
Trong Delta Lake, mỗi bảng (`bronze`, `silver`, `gold`) có nhật ký giao dịch `_delta_log` độc lập; mỗi lệnh `.save()` hoặc `.merge().execute()` chỉ đảm bảo tính nguyên tử (ACID) trong phạm vi một bảng duy nhất. Khi pipeline chạy tuần tự từ Bronze sang Silver rồi tính `build_gold` để ghi bảng Gold, không có cơ chế khóa hay commit nguyên tử liên bảng nào ngăn cản một reader đọc phải trạng thái trung gian nếu nó chỉ đọc phiên bản `latest` của từng bảng, hoặc tự động rollback bảng Silver nếu bước ghi bảng Gold bị lỗi giữa chừng. Tệp `snapshot_binding.json` và `release_manifest.json` giải quyết vấn đề này ở **tầng giao thức người tiêu dùng** (consumer protocol): bằng cách ghi nhận chính xác cặp phiên bản tương thích (`silver_version = 1`, `gold_version = 1`, `gold_source_silver_version = 1`) và chỉ ghi tệp manifest sau cùng sau khi đã kiểm tra toàn bộ cổng ràng buộc, hệ thống đảm bảo analyst luôn đọc đúng cặp snapshot khớp nhau mà không cần một giao dịch phân tán đa bảng.

---

### Câu 5: Tại sao việc một hệ thống lưu trữ tương thích giao diện S3 API thô (raw S3 API) vẫn chưa đủ để khẳng định tính tương thích với Delta Lake?

**Trả lời:**  
Tính đúng đắn của các bảng Delta (`bronze`, `silver`, `gold`) phụ thuộc hoàn toàn vào việc các tệp JSON trong thư mục `_delta_log` (như `00000000000000000000.json`, `00000000000000000001.json`) được ghi một cách **nguyên tử, tuần tự và không bị ghi đè** khi tạo phiên bản mới, đồng thời các thao tác đọc và liệt kê thư mục (`LIST`) phải có tính nhất quán đọc-sau-khi-ghi (read-after-write consistency). Một dịch vụ lưu trữ chỉ hỗ trợ các lệnh S3 API cơ bản (`GET`, `PUT`, `LIST`) có thể thiếu cơ chế đảm bảo tính loại trừ tương hỗ (`put-if-absent`) hoặc có độ trễ nhất quán danh sách tệp, dẫn đến nguy cơ hai trình ghi đồng thời tạo trùng một số hiệu version làm hỏng lịch sử bảng. Vì vậy, bên cạnh việc dùng S3A, bài lab bắt buộc áp dụng kỷ luật **single-writer** (chạy tuần tự từng bước trên một Spark driver duy nhất tại một thời điểm) và kiểm chứng trực tiếp bằng các thao tác Delta thực tế trên hạ tầng lưu trữ đích.
