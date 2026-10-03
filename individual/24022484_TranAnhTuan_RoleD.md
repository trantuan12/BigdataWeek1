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

Hai loại quyền này về bản chất nằm trên hai lớp hoàn toàn không liên thông nhau. Quyền `observer` hoạt động ở mặt phẳng quản trị Kubernetes — cụ thể là ServiceAccount `observer` được cấp qua RBAC với các verb `get/list/watch` trên pods, logs, events. Nó chỉ đọc được trạng thái cụm qua API Server, không chạm được vào dữ liệu bên trong ứng dụng. Còn quyền S3 reader lại nằm hoàn toàn ở tầng ứng dụng: SeaweedFS tự xác thực AccessKey/SecretKey qua cổng 8333, không phụ thuộc gì vào RBAC của Kubernetes. Nên dù có thể `kubectl get pod` được thì cũng không rút ra được quyền đọc object S3, và ngược lại.

Về namespace operator, lý do nó nằm ngoài biên giới cách ly là vì toàn bộ cơ chế bảo vệ đều dựa trên tài nguyên trong namespace — mà operator lại có quyền tạo và quản lý chính những tài nguyên đó. Operator hoàn toàn có thể spin up một Pod rồi mount Secret `s3-config` hoặc `s3-owner` vào để đọc credentials, không có gì chặn được điều này ở tầng namespace. Ngoài ra, NetworkPolicy dùng label `access: s3` làm selector — nhưng label thì không phải danh tính mã hóa, operator có thể tự gán label này cho bất kỳ Pod nào muốn. Tóm lại, kẻ tấn công không cần break crypto; họ chỉ cần là namespace operator là đủ để vượt qua mọi rào cản.

---

### Câu 2
**Why might concurrency four be slower or have higher latency than concurrency one? Connect your answer to evidence or a plausible bottleneck.**

Kết quả thực nghiệm khá rõ: ở c=4, median goodput PUT chỉ còn 41.44 MiB/s so với 43.33 MiB/s ở c=1 — giảm nhẹ khoảng 4%. GET thì tệ hơn, giảm 10%. Nhưng con số đáng chú ý nhất là p95 latency PUT tăng từ ~110 ms lên ~686 ms, tức gấp hơn 6 lần. Điều đó cho thấy throughput không cải thiện mà tail latency lại tệ hẳn — dấu hiệu kinh điển của bottleneck do tranh chấp tài nguyên, không phải do mạng.

Nguyên nhân chính có thể đến từ hai chỗ. Thứ nhất là CPU: trong `store.yaml`, container bị giới hạn cứng `limits: {cpu: "1"}`, tức SeaweedFS chỉ được phép dùng tối đa 1 core. Khi 4 connection đồng thời gửi file 4 MiB, 1 core đó phải liên tục chuyển ngữ cảnh giữa các goroutine xử lý HTTP — chi phí context switching này ăn thẳng vào thời gian xử lý thực. Thứ hai là I/O đĩa: toàn bộ dữ liệu đổ vào một PVC duy nhất dạng `local-path`. Bốn luồng ghi song song cùng một filesystem sẽ cạnh tranh lock ở mức kernel, và `fsync` buộc phải serialize các thao tác ghi — đây nhiều khả năng là nguyên nhân trực tiếp đẩy p95 lên cao vậy.

Một điểm nữa là concurrency cao thực ra chỉ phát huy tác dụng khi cần che giấu RTT lớn (môi trường WAN). Ở đây client và storage Pod chạy cùng một node, RTT nội bộ dưới 1 ms — một thread đơn đã gần tận dụng hết băng thông rồi, thêm thread chỉ tạo thêm hàng đợi chứ không giải quyết được gì.

---

### Câu 3
**Why does Pod recovery not establish backup, high availability or enforced retention?**

Ba khái niệm này thường bị nhầm lẫn với nhau, nhưng thực ra chúng giải quyết những vấn đề khác nhau hoàn toàn.

**High Availability** đòi hỏi dịch vụ không bị gián đoạn kể cả khi một instance bị lỗi. Deployment ở đây dùng 1 replica với `strategy: Recreate` — khi Pod cũ bị xóa, hệ thống phải chờ Pod mới khởi động và vượt qua readiness probe mới nhận traffic trở lại. Canary đo được downtime khoảng 10 giây. HA thực sự cần ít nhất 2 replica với cơ chế failover tự động để không có thời điểm nào dịch vụ bị trống.

**Backup** là về việc có bản sao độc lập để khôi phục khi mất dữ liệu. Thực nghiệm pod recovery chỉ chứng minh rằng PVC tồn tại sau khi container bị xóa và tái tạo — PVC UID `9484778c...` không thay đổi, nên dữ liệu vẫn còn đó. Nhưng nếu ổ đĩa vật lý của node bị hỏng, hoặc ai đó xóa nhầm object thì cũng không có gì để restore. Backup thật sự cần bản sao ở vị trí vật lý khác và hỗ trợ point-in-time recovery.

**Enforced retention** là về việc ngăn xóa hoặc sửa dữ liệu trong một khoảng thời gian nhất định. Trường `retention` trong `governance.json` chỉ là metadata ghi chú chính sách — không có cơ chế kỹ thuật nào enforce nó. Không có S3 Object Lock, không có WORM. Người dùng có quyền ghi như `owner` vẫn DELETE được object bất cứ lúc nào, và SeaweedFS sẽ thực hiện ngay mà không hỏi lại.

---

**Trần Anh Tuấn — MSSV: 24022484 — Role D**
