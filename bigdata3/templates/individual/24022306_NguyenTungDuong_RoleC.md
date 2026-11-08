# BÁO CÁO CÁ NHÂN — INDIVIDUAL ASSESSMENT

**Học phần:** Cloud-Native Big Data Infrastructure & Governance  
**Bài thực hành:** Lab 3 — Lakehouse Processing & Analytics  
**Package Revision:** 18 September 2026

| Thông tin | Nội dung |
|---|---|
| Họ và tên | {{MEMBER_C_NAME}} |
| MSSV | {{MEMBER_C_ID}} |
| Vai trò | Role C — Revision Processing & Replay Integrity |
| Nhiệm vụ chính | Task 3 — `choose_revisions`, `merge_silver`, deterministic tie-breaking, replay idempotence, time travel & invalid-batch refusal (`M01`–`M08`) |
| Nhiệm vụ chéo | Cross-check D: Verify Member D's result-equivalence evidence across Layout A and Layout B (`C_CHECK_D`) |
| Nhóm thực hiện | `{{GROUP_ID}}` |
| Run Label | `{{RUN_LABEL}}` |
| Context / Chế độ chạy | Spark standalone: hai worker container, S3A dùng chung (Spark {{SPARK}}, Delta-Spark {{DELTA}}, Python {{PYTHON}}, Java {{JAVA}}, Hadoop {{HADOOP}}) |
| Trusted Manifest SHA-256 | `{{TRUSTED_SHA}}` |
| Upstream Logical SHA-256 | `{{HANDOFF_LOGICAL_SHA}}` |
| Run ID Incremental | `{{INCREMENTAL_RUN_ID}}` |
| Cặp snapshot xử lý & replay | Bronze v{{BRONZE_INIT}} → v{{BRONZE_FINAL}} (replay v{{BRONZE_REPLAY}}); Silver v{{SILVER_INIT}} → v{{SILVER_FINAL}} (replay v{{SILVER_REPLAY}}); Gold v{{GOLD_INIT}} → v{{GOLD_FINAL}} |

---

## Phần 1: Đóng góp kỹ thuật (10 điểm)

### 1.1 Trước khi thực hiện (Bối cảnh và thách thức)

Sau khi thiết lập ba tầng bảng nền tảng (Bronze, Silver, Gold) từ bản bàn giao của Lab 2 ở Task 2 (`{{BASELINE_ROWS}}` quan sát), hệ thống Lakehouse cần tiếp nhận lô sửa đổi hợp lệ `lakehouse-valid-revisions-v1` (`data/changes/changes_batch02.csv`, gồm **{{INCOMING_ROWS}} dòng vật lý**). Khác với tập phát hành tĩnh của Lab 2, một lô sửa đổi trong thực tế có thể chứa đồng thời khóa mới, bản hiệu chỉnh mới hơn, bản ghi đến trễ bị cũ (stale), và nhiều phiên bản vật lý của cùng một khóa nghiệp vụ (`record_id`) có cùng thời điểm tiếp nhận (`ingest_time_utc`).

Việc xử lý lô sửa đổi đặt ra bốn thách thức kỹ thuật cốt lõi:
1. **Khử trùng lặp đầu vào và phá hòa xác định (Deterministic Incoming Winner Selection):** Trước khi thực hiện `MERGE INTO` bảng Delta Silver, tập nguồn phải có tối đa một dòng cho mỗi `record_id` (nếu một khóa nguồn khớp nhiều dòng với một dòng đích trong `MERGE`, Delta Lake sẽ báo lỗi bất định). Quy tắc chọn winner bắt buộc kế thừa hợp đồng Lab 2: `ingest_time_utc DESC, source_object ASC, source_row ASC`, hoàn toàn độc lập với thứ tự duyệt file hay phân vùng.
2. **Vị từ cập nhật nghiêm ngặt (Strict Merge Update Predicate):** Khi hợp nhất vào Silver, bản ghi mới chỉ được phép ghi đè bản ghi đang lưu nếu nó đứng **trước** trong cùng thứ tự ưu tiên trên. Một bản ghi cũ hơn (stale) tuyệt đối không được ghi đè giá trị mới hơn; một bản ghi trùng hệt thời gian tiếp nhận và định danh nguồn phải là thao tác không đổi dữ liệu (no-op).
3. **An toàn phát lại (Replay Idempotence) và Đọc lịch sử (Time Travel):** Khi toàn bộ lô sửa đổi được nạp lại lần hai, tầng Bronze không được nhân đôi biên nhận vật lý `(source_object, source_row, source_sha256)`, và tầng Silver phải bảo toàn 100% giá trị đo đạc lẫn provenance (`logical_sha256` không đổi dù lịch sử giao dịch Delta có thể tăng thêm một version commit). Đồng thời, snapshot lịch sử `versionAsOf={{SILVER_INIT}}` phải đọc lại được nguyên vẹn sau khi cập nhật.
4. **Kiểm định toàn lô trước khi ghi (Whole-batch Validation Before Write):** Mọi vi phạm hợp đồng dữ liệu (ngoài miền giá trị, thiếu trường bắt buộc, hoặc xuất hiện cột hạn chế như `operator_email`) phải bị chặn đứng ở mức toàn lô trước khi bất kỳ lệnh ghi Delta nào xảy ra, không âm thầm thay thế bằng bản ghi cũ.

Vai trò của tôi ({{MEMBER_C_NAME}} — Thành viên C) là thiết kế và cài đặt hai hàm biến đổi cốt lõi của Task 3 (`choose_revisions` và `merge_silver`), xác minh toàn bộ tám tiêu chí bắt buộc `M01`–`M08` trên dữ liệu thực và lịch sử giao dịch Delta, đồng thời thực hiện kiểm tra chéo độc lập đối với bằng chứng tương đương kết quả thí nghiệm Layout A/B của Thành viên D (`C_CHECK_D`).

---

### 1.2 Công việc kỹ thuật đã thực hiện

#### 1. Cài đặt `choose_revisions`: Chọn một winner duy nhất cho mỗi `record_id` (`M01`, `M03`)

- Hàm [`choose_revisions`](../../templates/student_impl.py) sử dụng cửa sổ phân hoạch theo `record_id` (`Window.partitionBy('record_id')`) và đánh số thứ tự bằng `F.row_number()` theo đúng thứ tự hợp đồng: `ingest_time_utc DESC`, `source_object ASC`, `source_row ASC`.
- Sau khi lọc `_revision_rank == 1`, hàm chỉ chiếu lại đúng 10 cột gốc (`.select(*changes_df.columns)`), đảm bảo không làm rò rỉ cột phụ `_revision_rank` vào bảng Delta Silver:

```python
{{CHOOSE_REVISIONS_FUNCTION}}
```

- Implementation SHA-256: `{{CODE_SHA}}`, được lưu trữ bất biến tại [`evidence/code_versions/{{CODE_SHA}}.py`](../../evidence/code_versions/{{CODE_SHA}}.py).
- **Kết quả thực thi trên `changes_batch02.csv` (`M01`):** Từ **{{INCOMING_ROWS}} dòng vật lý**, hàm chọn ra chính xác **{{INCOMING_WINNERS}} winners** tương ứng với 130 khóa `record_id` phân biệt, và loại bỏ **{{SUPERSEDED_COUNT}} phiên bản bị thay thế** (superseded) ngay trong cùng batch. Cả 10 trường hợp bị thay thế đều là các cặp bản ghi trùng `record_id`, trùng `ingest_time_utc` và trùng `source_object = 'lab3/source_batch02.csv'`, được phân định thắng thua một cách xác định nhờ `source_row ASC`:

{{SUPERSEDED_TABLE}}

- **Chứng minh tính bất biến khi đảo thứ tự dòng (`M03`):** Thử nghiệm đảo ngược thứ tự dòng và xáo trộn ngẫu nhiên (repartition/shuffle) cho kết quả 130 winners và provenance giống hệt 100% (`winner_reordering_passed = true`). Ngược lại, nếu dùng cách duyệt tuần tự giữ dòng cuối cùng (như dictionary comprehension `{r['record_id']: r for r in incoming}`), cả **10/10 khóa đồng hạng** (`R002001`–`R002005` và `R010001`–`R010005`) đều chọn sai sang các dòng `131`–`140` có nhiệt độ lệch `+1.00 °C`.

#### 2. Cài đặt `merge_silver`: Hợp nhất có điều kiện nghiêm ngặt vào Delta Silver (`M02`)

- Hàm [`merge_silver`](../../templates/student_impl.py) thực hiện câu lệnh `MERGE INTO` giữa bảng đích Silver (`t`) và DataFrame `winners_df` (`s`) khớp trên khóa nghiệp vụ `t.record_id = s.record_id`.
- Vị từ cập nhật `precedes` biểu diễn chính xác thứ tự từ điển (lexicographical order) của bộ ba `(ingest_time_utc DESC, source_object ASC, source_row ASC)`:

```python
{{MERGE_SILVER_FUNCTION}}
```

- **Phân tích 130 quyết định hợp nhất (`M02`):** Đối chiếu 130 incoming winners với **{{BASELINE_ROWS}} dòng** của Silver `v{{SILVER_INIT}}`, quá trình merge tạo ra Silver `v{{SILVER_FINAL}}` gồm **{{SILVER_ROWS}} dòng** với phân bố quyết định chính xác:
  - **`inserted`: 100 khóa** (`R010001` đến `R010100`) — các khóa mới hoàn toàn thuộc ngày `2026-02-06`, làm tăng tổng số dòng từ `9,610` lên `9,710`.
  - **`updated_newer`: 20 khóa** (`R002001` đến `R002020`) — bản ghi đến có `ingest_time_utc` mới hơn bản ghi đang lưu trong Silver `v0` (ví dụ `2026-02-02T16:50:30Z` > `2026-02-02T16:41:30Z`), được phép cập nhật giá trị nhiệt độ và provenance.
  - **`unchanged_stale`: 10 khóa** (`R003001` đến `R003010`) — bản ghi đến có `ingest_time_utc` cũ hơn bản ghi đang lưu trong Silver `v0`, bị vị từ `precedes` chặn lại và giữ nguyên giá trị cũ.

- **Truy vết chi tiết 3 bản ghi đại diện trước và sau cập nhật (`evidence/row_trace.json`):**

{{REPRESENTATIVE_TRACE_TABLE}}

#### 3. Chứng minh an toàn phát lại (Replay Safety) ở Bronze và Silver (`M04`, `M05`)

- **Phát lại biên nhận Bronze (`M04`):** Tầng Bronze định danh mỗi biên nhận vật lý bằng bộ ba khóa tự nhiên `(source_object, source_row, source_sha256)`. Lần nạp đầu tiên ghi `9,610` dòng ở `v0` và hợp nhất thêm `140` dòng ở `v1` (tổng cộng **{{BRONZE_RECEIPTS}} biên nhận**). Khi phát lại toàn bộ batch lần thứ hai, câu lệnh `MERGE` chỉ có nhánh `whenNotMatchedInsertAll` nhận thấy cả 140 biên nhận vật lý đã tồn tại nên không chèn thêm dòng nào: số dòng giữ nguyên **{{BRONZE_RECEIPTS}}**, mã băm biên nhận giữ nguyên `{{BRONZE_SHA}}`, và version giữ ở `v{{BRONZE_REPLAY}}`.
- **Phát lại hợp nhất Silver (`M05`):** Khi áp dụng lại cùng lô `changes_batch02.csv` lên Silver `v{{SILVER_FINAL}}`, cả 120 khóa (`100` khóa vừa insert và `20` khóa vừa update) giờ đây có `s.ingest_time_utc = t.ingest_time_utc`, `s.source_object = t.source_object` và `s.source_row = t.source_row`. Do vị từ `precedes` dùng dấu bất đẳng thức nghiêm ngặt (`>` và `<`), một bản ghi trùng hệt định danh làm cho `precedes = False`. Đồng thời 10 khóa `unchanged_stale` vẫn tiếp tục có `s.ingest_time_utc < t.ingest_time_utc` nên `precedes = False`.
- Kết quả là ở lần chạy replay (`Silver v{{SILVER_REPLAY}}`), không có dòng nào được insert (`numTargetRowsInserted = 0`) và không có dòng nào bị update (`numTargetRowsMatchedUpdated = 0`). Mã băm chính quy (canonical logical SHA-256) của Silver `v{{SILVER_FINAL}}` và Silver `v{{SILVER_REPLAY}}` khớp tuyệt đối:
  - `logical_sha256_after (v1)` = `{{SILVER_AFTER_SHA}}`
  - `logical_sha256_replay (v2)` = `{{SILVER_REPLAY_SHA}}`

- **Đối chiếu lịch sử giao dịch và metric thực thi Delta Lake (`evidence/live_table_verification.json`):**

{{DELTA_HISTORY_TABLE}}

#### 4. Đọc lịch sử Time Travel (`M06`), Từ chối lô lỗi (`M07`) và Nhất quán Gold (`M08`)

- **Đọc lịch sử với `versionAsOf` (`M06`):** Sau khi bảng Silver đã tiến lên `v{{SILVER_FINAL}}` và `v{{SILVER_REPLAY}}`, truy vấn `spark.read.format('delta').option('versionAsOf', 0).load(silver_path)` thu được chính xác **{{BASELINE_ROWS}} dòng** với mã băm logic `{{HANDOFF_LOGICAL_SHA}}`, khớp 100% với bản xuất `silver_baseline.jsonl` ban đầu và tệp bàn giao thượng nguồn. Không chạy `VACUUM` hay xóa log lịch sử.
- **Từ chối toàn bộ lô đầu vào không hợp lệ trước khi ghi (`M07`):** Trước khi gọi `append_receipts` hoặc `merge_silver`, runner kiểm tra toàn bộ lô bằng `validate_rows`. Ba bài probe đột biến độc lập (`evidence/input_contract_tests.json`) xác nhận chính sách từ chối cấp độ lô (whole-batch rejection):

{{MUTATION_TABLE}}

- **Tính nhất quán của bảng Gold sau cập nhật (`M08`):** Bảng Gold `v{{GOLD_FINAL}}` được tái tạo từ snapshot ghim `Silver v{{SILVER_FINAL}}` (không nối thêm dòng trùng lặp vào bảng Gold cũ). Tổng số nhóm tăng từ `16` nhóm (4 site × 4 ngày ở `v0`) lên **{{GOLD_GROUPS}} nhóm** (4 site × 5 ngày ở `v1`, thêm ngày `2026-02-06`). Tổng số quan sát bảo toàn tuyệt đối `SUM(n_observations) = 9,710`, tổng nhiệt độ đạt `222,930.50 °C`, tổng số bản ghi trễ `n_late = 220`, và `n_above_25 = 2,818`. Phép tính độc lập bằng `Decimal` khớp 100% trên cả 20 nhóm × 11 trường với `gold_after_batch02.json` và `release_candidate/gold.json`.

---

### 1.3 Kết quả kiểm định các tiêu chí Task 3 (`M01`–`M08`)

| Mã kiểm tra | Nội dung kiểm định | Bằng chứng quan sát thực tế | Trạng thái |
|:---:|---|---|:---:|
| **M01** | Số lượng incoming winners | 140 dòng vật lý $\rightarrow$ 130 winners duy nhất + 10 dòng đồng hạng bị thay thế | **PASS** |
| **M02** | Quyết định Newer vs. Stale vs. Inserted | `inserted: 100`, `updated_newer: 20`, `unchanged_stale: 10`; truy vết `R010001`, `R003001`, `R002001` khớp nguồn gốc | **PASS** |
| **M03** | Phá hòa xác định khi đảo thứ tự | Đảo ngược và xáo trộn dòng vẫn giữ nguyên 130 winners và provenance; dict lấy dòng cuối sai cả 10 khóa | **PASS** |
| **M04** | Phát lại biên nhận Bronze | Số biên nhận giữ nguyên `9,750` dòng (`v1`); mã băm `80205e70...` không đổi sau replay | **PASS** |
| **M05** | Phát lại hợp nhất Silver | Replay tạo `v2` (`0` insert, `0` update); mã băm logic `d0e76f8b...` khớp tuyệt đối với `v1` (`9,710` dòng) | **PASS** |
| **M06** | Đọc lịch sử (`versionAsOf=0`) | Đọc lại `Silver v0` sau merge khớp 100% handoff (`9,610` dòng, SHA-256 `c50f8252...`) | **PASS** |
| **M07** | Từ chối 3 ca lô lỗi (Invalid batches) | `VALUE`, `REQUIRED_VALUE`, `SCHEMA_ALLOWLIST` đều bị chặn trước khi ghi (`writes_performed = 0`) | **PASS** |
| **M08** | Nhất quán bảng Gold sau merge | Tái tính từ `Silver v1` khớp tuyệt đối 20 nhóm × 11 cột (`SUM(n_obs) = 9,710`, `SUM(temp) = 222930.50`) | **PASS** |
| **C→D** | Kiểm tra chéo tương đương kết quả D | Cả 6 lần chạy A/B khớp nhau và khớp `100 × Gold(2026-02-06)`; Gold vận hành tách biệt khỏi replica | **PASS** |

**Các tệp bằng chứng kỹ thuật nộp kèm:**
- [`role_C_prediction.json`](../../evidence/role_C_prediction.json), [`role_C_check.json`](../../evidence/role_C_check.json) — Bản ghi dự đoán trước khi chạy và kết quả **{{N_CHECKS}}/{{N_CHECKS}} kiểm tra tự động** của Role C.
- [`incremental.json`](../../evidence/incremental.json), [`revision_decisions.json`](../../evidence/revision_decisions.json), [`row_trace.json`](../../evidence/row_trace.json) — Kết quả thực thi bước `incremental`, toàn bộ 130 quyết định hợp nhất và truy vết dòng đại diện.
- [`input_contract_tests.json`](../../evidence/input_contract_tests.json) — Bằng chứng 3 bài kiểm tra đột biến lô lỗi bị từ chối với 0 thao tác ghi Delta.
- [`silver_baseline.jsonl`](../../evidence/silver_baseline.jsonl), [`silver_after_batch02.jsonl`](../../evidence/silver_after_batch02.jsonl), [`silver_replay.jsonl`](../../evidence/silver_replay.jsonl), [`silver_time_travel.jsonl`](../../evidence/silver_time_travel.jsonl) — Bốn bản xuất chính quy của Silver (`v0`, `v1`, `v2`, và `versionAsOf=0`).
- [`gold_baseline.json`](../../evidence/gold_baseline.json), [`gold_after_batch02.json`](../../evidence/gold_after_batch02.json), [`live_table_verification.json`](../../evidence/live_table_verification.json) — Dữ liệu Gold trước/sau merge và lịch sử commit Delta thực tế.

---

## Phần 2: Kiểm tra chéo độc lập (10 điểm)

### Thao tác được chỉ định: Check Member D's result-equivalence evidence (`C_CHECK_D`)
*(Kiểm tra bằng chứng tương đương kết quả thí nghiệm bố trí vật lý Layout A và Layout B của Thành viên D)*

#### 2.1 Mục tiêu kiểm tra

Thành viên D (Đàm Quang Tiến — MSSV `24022463`) phụ trách cài đặt `benchmark_query` và thực hiện thí nghiệm so sánh hiệu năng giữa hai bố trí vật lý: **Layout A** (không phân vùng, `repartition(32)`) và **Layout B** (`repartition(5, event_date)` kết hợp `partitionBy(event_date)`). Trong kỹ thuật dữ liệu lớn, một phép tối ưu hóa vật lý chỉ có giá trị khi **tập kết quả trả về hoàn toàn tương đương** (result equivalence) với truy vấn gốc và không làm biến dạng dữ liệu vận hành.

Mục tiêu kiểm tra chéo của tôi (Role C) là xác minh độc lập rằng:
1. Kết quả truy vấn trên cả hai bảng Layout A và Layout B trong toàn bộ 6 lần đo (`A–B, B–A, A–B`) là đồng nhất tuyệt đối.
2. Kết quả tổng hợp của ngày `2026-02-06` trên tập dữ liệu mở rộng (`971,000` dòng = `9,710` dòng Silver `v1` × `100` bản sao `replica_id`) khớp chính xác với kết quả nhân hệ số `100` từ chính 100 bản ghi mới được Task 3 chèn vào Silver `v1` (và bảng Gold `v1`) trong ngày `2026-02-06`.
3. Kế hoạch thực thi vật lý (`plan_A.txt`, `plan_B.txt`) và metric quét file (`benchmark_scan_metrics.json`) phản ánh đúng cùng một biểu thức tính toán mà không làm rò rỉ dữ liệu nhân bản vào bảng Gold vận hành.

#### 2.2 Kịch bản và Dự đoán

Trước khi chạy công cụ kiểm định tự động (ghi nhận lúc `{{PREDICTED_AT}}` tại [`evidence/role_C_prediction.json`](../../evidence/role_C_prediction.json)), tôi lập dự đoán định lượng dựa trên kết quả của Task 3:
1. Trong Task 3, toàn bộ 100 khóa được `inserted` (`R010001`–`R010100`) đều thuộc ngày UTC `2026-02-06`, phân bổ đều cho 4 site (`zone-1`, `zone-2`, `zone-3`, `zone-4`), mỗi site đúng **25 quan sát** (trong đó mỗi site có 5 quan sát trễ `is_late = true`).
2. Khi nhân bản với `benchmark_copies = 100` (`replica_id` từ `0` đến `99`) và nhóm theo `site` (không nhóm theo `replica_id`), mỗi site trong ngày `2026-02-06` bắt buộc phải có:
   - `n_observations = 25 × 100 = 2,500` (tổng 4 site = `10,000` dòng workload).
   - `n_late = 5 × 100 = 500` cho mỗi site.
   - `sum_temperature_c` bằng đúng tổng nhiệt độ của site đó trong `gold_after_batch02.json` nhân `100`: `532.50 × 100 = 53250.00 °C` (cho `zone-1` và `zone-3`) và `540.00 × 100 = 54000.00 °C` (cho `zone-2` và `zone-4`).
3. Cả 6 lần đo xen kẽ `A–B, B–A, A–B` đều phải có `result_equal = true`. Layout A đọc toàn bộ **32/32 file** do chỉ có `PushedFilters`, trong khi Layout B chỉ đọc **1/5 file** nhờ `PartitionFilters` cắt tỉa theo `event_date = 2026-02-06`.

#### 2.3 Thực thi kiểm chứng độc lập

Thực thi kịch bản kiểm tra của Role C:

```powershell
python tools/check_role_c.py --config config/team.json
```

Đoạn mã Python dưới đây minh họa cách kiểm chứng trực tiếp dự đoán đối với một bản ghi cũ/đồng hạng và kiểm tra tương đương kết quả A/B của Role D (dùng cho cả kiểm tra tự động và phần trình bày 2 phút của Role C):

```python
import sys
from decimal import Decimal
from pathlib import Path
sys.path[:0] = ['src', 'templates', 'tools']
from common import load_csv, load_registry, logical_sha256, read_json, validate_rows
from check_role_c import apply_merge_reference, expected_benchmark_from_gold, select_incoming_winners

registry = load_registry(Path('data/handoff/sensors.csv'))
baseline = validate_rows(load_csv(Path('data/handoff/curated.csv')), registry, True)
incoming = validate_rows(load_csv(Path('data/changes/changes_batch02.csv')), registry, False)

# 1. Kiểm tra tie-break và stale revision (Role C Demo)
winners, superseded = select_incoming_winners(incoming)
merged, _, _, counts, traces = apply_merge_reference(baseline, incoming)
assert counts == {'updated_newer': 20, 'unchanged_stale': 10, 'inserted': 100}
assert traces['R010001']['selected']['source_row'] == 1       # Tie-break: row 1 (19.10 C) thắng row 131 (20.10 C)
assert traces['R003001']['selected']['source_row'] == 3001    # Stale: giữ observations_a.csv row 3001 (18.10 C)

# 2. Kiểm tra chéo C_CHECK_D: Tương đương kết quả A/B với 100x Gold ngày 2026-02-06
gold_v1 = read_json(Path('evidence/gold_after_batch02.json'))
bench = read_json(Path('evidence/benchmark_report.json'))
expected_ab = expected_benchmark_from_gold(gold_v1, bench['benchmark_copies'], '2026-02-06')
assert bench['query_result'] == expected_ab
assert all(m['result_equal'] for m in bench['measurements']) and len(bench['measurements']) == 6
print('PASS C_CHECK_D: All 6 A/B runs match 100x operational Gold on 2026-02-06:', expected_ab)
```

#### 2.4 Kết quả quan sát và Đối chiếu

**Bảng 1 — Đối chiếu đáp án truy vấn A/B của Role D với dữ liệu chuẩn từ Silver v1 / Gold v1 ngày `2026-02-06`:**

{{CROSS_D_RESULT_TABLE}}

**Bảng 2 — Kiểm tra 6 lần đo thực nghiệm và số file thực đọc (`A–B, B–A, A–B`):**

{{CROSS_D_TIMINGS_TABLE}}

Toàn bộ quá trình kiểm tra hoàn tất lúc `{{COMPLETED_AT}}`, vượt qua **{{N_CHECKS}}/{{N_CHECKS}} assertion** (và toàn bộ **{{N_TESTS}} unit/integration tests** trong `implementation_tests.json`):
- Cả 6 lần chạy (`3` lần Layout A, `3` lần Layout B) đều trả về cùng một danh sách 4 dòng tổng hợp khớp 100% với giá trị suy dẫn độc lập từ Silver `v1` và Gold `v1`.
- Trong [`plan_A.txt`](../../evidence/plan_A.txt) và [`plan_B.txt`](../../evidence/plan_B.txt), hai nhánh `HashAggregate` thực thi cùng một công thức: `count(1)`, `MakeDecimal(sum(UnscaledValue(temperature_c)),18,2)` và `sum(CASE WHEN is_late THEN 1 ELSE 0 END)` nhóm theo duy nhất khóa `site`.
- Bảng Gold vận hành (`gold_after_batch02.json`) vẫn giữ nguyên **9,710 quan sát** trên 20 nhóm site/ngày, xác nhận 971,000 dòng nhân bản chỉ nằm trong đường dẫn riêng `benchmarks/...` và không làm ô nhiễm dữ liệu phát hành.

#### 2.5 Kết luận kiểm tra chéo

Bằng chứng tương đương kết quả (`result-equivalence evidence`) của Thành viên D là hoàn toàn chính xác, trung thực và có thể tái lập. Sự chênh lệch thời gian chạy trung vị giữa Layout A (**{{MEDIAN_A}} giây**, đọc 32 file) và Layout B (**{{MEDIAN_B}} giây**, đọc 1 file, tỷ số speedup **{{MEDIAN_RATIO}}×**) xuất phát từ việc cắt tỉa phân vùng (`PartitionFilters`) và giảm lượng dữ liệu đọc trên cùng một câu truy vấn tương đương ngữ nghĩa, không phải do thay đổi logic tính toán hay làm sai lệch mẫu số.

---

## Phần 3: Trả lời câu hỏi phỏng vấn thoát (10 điểm)

### Câu 1: Tại sao một thao tác merge không thay đổi dữ liệu (no-op merge) vẫn có thể ghi thêm một phiên bản commit mới vào lịch sử Delta mà không vi phạm tính lũy đẳng (idempotence)?

**Trả lời:**  
Tính lũy đẳng (idempotence) của một phép biến đổi dữ liệu được định nghĩa trên **trạng thái nội dung logic** của tập dữ liệu ($f(f(S)) = f(S)$), không phải trên số thứ tự commit của nhật ký giao dịch vật lý (`_delta_log`). Trong Delta Lake, khi thực thi lệnh `MERGE INTO` lần thứ hai với cùng lô `changes_batch02.csv` lên bảng Silver `v{{SILVER_FINAL}}`, mặc dù vị từ `precedes` đánh giá thành `False` cho mọi dòng khớp (`numTargetRowsInserted = 0`, `numTargetRowsMatchedUpdated = 0`), cơ chế copy-on-write của Delta MERGE vẫn quét phân vùng đích, sao chép lại `9,710` dòng sang tệp Parquet mới (`numTargetRowsCopied = 9710`, `numTargetFilesAdded = 1`, `numTargetFilesRemoved = 1`) và ghi một tệp log giao dịch mới là `00000000000000000002.json` (`Silver v{{SILVER_REPLAY}}`). Do toàn bộ `9,710` bản ghi ở `v{{SILVER_REPLAY}}` có giá trị đo đạc và provenance trùng khớp tuyệt đối với `v{{SILVER_FINAL}}` (cùng mã băm chính quy `{{SILVER_AFTER_SHA}}`), trạng thái nghiệp vụ hoàn toàn bất biến và thỏa mãn tính lũy đẳng.

---

### Câu 2: Tại sao một kiểm tra miền giá trị hợp lệ (passing range check) không đồng nghĩa với việc xác lập tính chính xác của phép đo (measurement accuracy)?

**Trả lời:**  
Kiểm tra miền giá trị (ví dụ nhiệt độ nằm trong khoảng hợp lệ `[-50, 60] °C` và không bị `NULL`) chỉ xác nhận bản ghi tuân thủ **ràng buộc cú pháp và miền hợp đồng** (syntactic/domain validity) để không làm hỏng pipeline tính toán. Nó không thể phát hiện sai số hệ thống, độ trôi cảm biến (sensor drift), hay nhiễu vật lý nằm bên trong khoảng hợp lệ đó. Trong bài thực hành này, ngay cả các bản ghi hợp lệ vượt qua `validate_rows` vẫn tồn tại những điểm lệch nhất định khi đối chiếu với bộ mẫu kiểm định `qa_reference.csv`. Do đó, hợp đồng dữ liệu quy định chỉ dùng `qa_reference.csv` để đánh giá chất lượng và ghi nhận giới hạn trong metadata, tuyệt đối không tự ý lấy giá trị tham chiếu QA để sửa hay ghi đè giá trị quan sát thực tế trong Silver và Gold.

---

### Câu 3: Tại sao tập dữ liệu benchmark nhân bản tổng hợp (replica-expanded benchmark) bắt buộc phải nằm ngoài các bảng Gold vận hành (operational Gold)?

**Trả lời:**  
Tập dữ liệu benchmark (`971,000` dòng) được tạo ra bằng phép tích Đề-các (`crossJoin`) giữa `9,710` quan sát thực của Silver `v{{SILVER_FINAL}}` với `100` mã `replica_id` giả lập nhằm mục đích duy nhất là tạo tải I/O đủ lớn để quan sát hành vi cắt tỉa phân vùng (`PartitionFilters`) giữa Layout A và Layout B. Các dòng nhân bản này không phải là các phép đo vật lý độc lập mới. Nếu ghi hoặc trộn chúng vào bảng Gold vận hành, các chỉ số cộng tính như `n_observations` và `sum_temperature_c` sẽ bị phóng đại gấp 100 lần, làm sai lệch hoàn toàn ý nghĩa thống kê, số lượng mẫu và cam kết provenance của bản phát hành. Vì vậy, hai bảng A/B phải được ghi vào không gian đường dẫn tách biệt (`benchmarks/<uuid>/...`) và bảng Gold vận hành chỉ được tính từ đúng `9,710` khóa của Silver `v{{SILVER_FINAL}}`.

---

### Câu 4: Tại sao một manifest phát hành (release manifest) không phải là một giao dịch đa bảng/đa đối tượng (cross-table transaction)?

**Trả lời:**  
Mỗi bảng Delta (`bronze`, `silver`, `gold`) quản lý tính nguyên tử (ACID) riêng biệt trong phạm vi thư mục `_delta_log` của chính bảng đó; không có giao thức cam kết hai pha (two-phase commit) nguyên tử trải rộng đồng thời trên cả ba bảng và các tệp JSON xuất ra trên S3. Tệp `release_manifest.json` và `snapshot_binding.json` đóng vai trò là **giao thức phía người tiêu dùng** (consumer protocol): chúng ghim tường minh cặp phiên bản đã được kiểm định (`Silver v{{SILVER_FINAL}}` và `Gold v{{GOLD_FINAL}}` có `gold_source_silver_version = {{SILVER_FINAL}}`) và được ghi sau cùng (manifest-last) vào một prefix phát hành mới. Cơ chế này giúp analyst đọc đúng cặp snapshot nhất quán và phát hiện các bản ghi trộn phiên bản (như gate `R02_mixed_version`), nhưng bản thân tệp manifest không tự động khóa quyền truy cập IAM, không phải chữ ký mật mã, và không tự động rollback các bảng nếu một bước giữa chừng bị lỗi.

---

### Câu 5: Tại sao việc một hệ thống lưu trữ tương thích giao diện S3 API thô (raw S3 API) vẫn chưa đủ để khẳng định tính tương thích với Delta Lake?

**Trả lời:**  
Giao diện S3 API cơ bản (`GET`, `PUT`, `LIST`, `DELETE` object) chỉ đảm bảo khả năng lưu trữ đối tượng, nhưng Delta Lake đòi hỏi các cam kết ngữ nghĩa chặt chẽ hơn nhiều ở tầng nhật ký giao dịch (`_delta_log`): tính nguyên tử khi tạo tệp log mới (atomic `put-if-absent` hoặc cơ chế khóa/đồng thuận tương đương của `LogStore`), tính nhất quán đọc-sau-khi-ghi (read-after-write consistency) và liệt kê thư mục nhất quán để các reader/writer nhìn thấy đúng chuỗi phiên bản `000000...json`. Một backend tương thích S3 API nhưng thiếu kiểm soát ghi nguyên tử có thể làm hỏng trạng thái bảng nếu có nhiều driver ghi đồng thời. Do đó, trong cấu hình S3 chuẩn của bài lab, mọi bước ghi (`bootstrap`, `incremental`, `benchmark`, `release`) bắt buộc phải thực thi tuần tự theo mô hình **single writer** (một Spark driver ghi tại một thời điểm) và phải được kiểm nghiệm thực tế bằng các phép thử schema enforcement, `MERGE`, replay và `versionAsOf`.
