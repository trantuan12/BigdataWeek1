# BÁO CÁO CÁ NHÂN — INDIVIDUAL ASSESSMENT
**Học phần:** Cloud-Native Big Data Infrastructure & Governance

| Thông tin | |
|---|---|
| Họ và tên | Trần Anh Tuấn |
| MSSV | 24022484 |
| Vai trò | Role D — Performance Engineer |
| Nhiệm vụ | Task 4 — Measure the data path |
| Nhóm | bd-g10 |
| Commit | `7a6881740439fcafc32416bad25c1abab23ba8c1` (message: `task 4`) |

---

## Phần 1: Đóng góp kỹ thuật (10 điểm)

**Trước khi thực hiện:**
Namespace `bd-g10` lúc này đã có SeaweedFS và các fixture nhỏ (25 bytes) từ Task 1–2, nhưng chưa có số liệu nào về thông lượng hay độ trễ thực tế của đường truyền dữ liệu.

**Công việc đã làm:**

Chạy 6 trial đo kiểm theo thứ tự xen kẽ A-B-B-A-A-B để tránh ảnh hưởng warm-up:
- 3 trial tại concurrency `c=1`: `r1-c1`, `r2-c1`, `r3-c1`
- 3 trial tại concurrency `c=4`: `r1-c4`, `r2-c4`, `r3-c4`
- Mỗi trial: 32 objects × 4 MiB = 128 MiB, gồm 32 PUT và 32 GET có kiểm tra SHA-256

Các file tạo ra:
- [`parse_benchmark.py`](file:///d:/BigData/BigdataWeek1/parse_benchmark.py) — script đọc JSONL, tính goodput và p95
- [`evidence/r1-c1.jsonl`](file:///d:/BigData/BigdataWeek1/evidence/r1-c1.jsonl) đến `r3-c4.jsonl` — log thô từng request
- [`evidence/resource-samples.txt`](file:///d:/BigData/BigdataWeek1/evidence/resource-samples.txt) — mẫu CPU/RAM mỗi 5 giây trong khi chạy tải
- [`benchmark-summary.csv`](file:///d:/BigData/BigdataWeek1/benchmark-summary.csv) — bảng tổng hợp kết quả

**Kết quả:**

192/192 PUT thành công, 192/192 GET xác thực hash SHA-256 không có lỗi. Số liệu tổng hợp:

| Phase | Metric | c=1 (median) | c=4 (median) | Tỷ lệ |
|---|---|---|---|---|
| PUT | Goodput (MiB/s) | 43.33 | 41.44 | 0.96× |
| GET | Goodput (MiB/s) | 206.04 | 184.65 | 0.90× |
| PUT | p95 latency (ms) | ~110 | ~686 | 6.2× tệ hơn |

Concurrency 4 không làm tăng thông lượng mà còn làm xấu đáng kể độ trễ đuôi.

---

## Phần 2: Kiểm tra thực hành độc lập

**Trial được chọn:** `bench/r1-c1` — file [`evidence/r1-c1.jsonl`](file:///d:/BigData/BigdataWeek1/evidence/r1-c1.jsonl)

### Tính lại Goodput (MiB/s)

Công thức: `Goodput = tổng_bytes / (1 048 576 × wall_s)`

**PUT:**
- 32 object × 4 194 304 bytes = 134 217 728 bytes = 128 MiB
- wall_s = 3.0335 s (lấy từ dòng `kind: summary` trong JSONL)
- 128 / 3.0335 = **42.20 MiB/s** ← khớp CSV

**GET:**
- 128 MiB, wall_s = 0.6212 s
- 128 / 0.6212 = **206.04 MiB/s** ← khớp CSV

### Tính lại p95 Nearest-rank (ms)

`k = ceil(0.95 × 32) = ceil(30.4) = 31` → lấy giá trị thứ 31 trong mảng ms sắp xếp tăng dần.

**PUT:** phần tử thứ 31 = **109.34 ms** *(khớp `p95_success_ms: 109.33575...` trong JSONL)*

**GET:** phần tử thứ 31 = **23.81 ms** *(khớp `p95_success_ms: 23.81283...`)*

### Kiểm tra chéo với Role C

Nhiệm vụ: *re-run two of C's expected denials*

| # | Client | Thao tác | Kết quả |
|---|---|---|---|
| S04 | `ingestor` | PUT `research-release/auth-probe.txt` | HTTP 403 AccessDenied ✓ |
| S10 | `analyst` | GET `research-raw/fixture.txt` | HTTP 403 AccessDenied ✓ |

---

## Phần 3: Câu hỏi vấn đáp

### Câu 1
**Why are Kubernetes observer permissions separate from S3 reader permissions, and why is a namespace operator outside the claimed isolation boundary?**

Kubernetes observer và S3 reader thuộc hai mặt phẳng hoàn toàn khác nhau:

- **Control plane (K8s RBAC):** `observer` ServiceAccount được cấp quyền `get/list/watch` pods, logs, events qua Kubernetes API — chỉ đọc trạng thái cụm.
- **Data plane (SeaweedFS S3 API):** truy cập qua AccessKey/SecretKey riêng, xác thực bởi chính tiến trình SeaweedFS trên cổng 8333. Hai hệ thống không liên thông — có quyền xem pods không đồng nghĩa đọc được object S3.

Namespace operator nằm ngoài biên giới cách ly vì:
1. Operator có thể tạo Pod bất kỳ và mount Secret `s3-config` hoặc `s3-owner` để lấy credentials — không có cơ chế nào ngăn điều này ở tầng namespace.
2. NetworkPolicy dùng label `access: s3` làm selector, không phải danh tính mã hóa. Operator có thể gắn label này lên Pod bất kỳ để bypass network rule.

---

### Câu 2
**Why might concurrency four be slower or have higher latency than concurrency one? Connect your answer to evidence or a plausible bottleneck.**

Dựa trên số liệu 6 trial và `resource-samples.txt`:

1. **CPU bị giới hạn cứng:** `store.yaml` đặt `limits: {cpu: "1"}`. SeaweedFS chỉ có 1 core. Khi 4 thread cùng gửi file 4 MiB, core đó liên tục chuyển ngữ cảnh giữa các kết nối HTTP — chi phí context switch ăn vào thời gian xử lý thực.

2. **Nghẽn I/O đĩa:** Tất cả dữ liệu ghi vào một PVC (`local-path`). Bốn thread cùng ghi song song dẫn đến tranh chấp lock filesystem và serialization tại `fsync`. Đây là nguyên nhân trực tiếp khiến p95 PUT tăng từ ~110 ms lên ~686 ms (gấp hơn 6 lần).

3. **Không có RTT để che giấu:** Trên WAN, tăng concurrency giúp pipeline hóa request trong thời gian chờ gói tin. Ở đây client và storage cùng node, RTT < 1 ms. Một thread đơn đã gần đạt băng thông tối đa; thêm thread chỉ tạo thêm hàng đợi tranh chấp.

---

### Câu 3
**Why does Pod recovery not establish backup, high availability or enforced retention?**

1. **Không phải HA:** Deployment dùng 1 replica, `strategy: Recreate`. Khi Pod bị xóa có downtime (~10 giây canary ghi nhận) trước khi Pod mới sẵn sàng. HA thực sự cần nhiều replica với failover tự động và zero downtime.

2. **Không phải backup:** Thực nghiệm chỉ chứng minh dữ liệu tồn tại qua vòng đời container nhờ PVC được giữ lại (UID `9484778c...`). Nếu ổ đĩa vật lý hỏng hoặc dữ liệu bị xóa nhầm thì không có bản sao nào. Backup đòi hỏi bản sao độc lập ở vị trí vật lý khác và khả năng khôi phục theo thời điểm.

3. **Không phải enforced retention:** Trường `retention` trong `governance.json` là metadata chính sách, không có cơ chế kỹ thuật nào ngăn xóa object. Không có S3 Object Lock hay WORM — người dùng có quyền ghi vẫn xóa được bất cứ lúc nào.

---


**Trần Anh Tuấn — MSSV: 24022484 — Role D**
