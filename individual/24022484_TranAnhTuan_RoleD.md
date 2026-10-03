# BÁO CÁO ĐÁNH GIÁ NĂNG LỰC CÁ NHÂN (INDIVIDUAL ASSESSMENT)
### Học phần: Cloud-Native Big Data Infrastructure & Governance

* **Họ và tên:** Trần Anh Tuấn
* **Mã số sinh viên (MSSV):** 24022484
* **Vai trò kỹ thuật:** Role D — Performance Engineer
* **Nhiệm vụ chính:** Task 4 — Measure the data path (Đo lường & Phân tích hiệu năng đường truyền)
* **Nhóm:** bd-g10 / Group 10
* **Commit cá nhân:** `7a68817` (Commit message: `"task 4"`)

---

## PHẦN 1: ĐÓNG GÓP KỸ THUẬT (TECHNICAL CONTRIBUTION) — 10 ĐIỂM

### 1. Hiện trạng trước khi thực hiện (Before Behaviour)
* Namespace `bd-g10` sau khi triển khai Task 1 và Task 2 chỉ có dịch vụ lưu trữ SeaweedFS cơ bản và các file fixture kiểm thử nhỏ (25 bytes).
* Chưa hề có pipeline đo kiểm tải, chưa có dữ liệu đánh giá thông lượng (goodput) hay độ trễ đuôi (tail latency / p95) trong điều kiện truyền tải dữ liệu lớn thực tế.

### 2. Đóng góp kỹ thuật đã thực hiện (Attributable Change)
* Thiết kế và triển khai quy trình đo kiểm hiệu năng 6 lượt có kiểm soát theo mô hình xen kẽ **A - B - B - A - A - B**:
  * 3 lượt tại concurrency $c=1$ (`r1-c1`, `r2-c1`, `r3-c1`).
  * 3 lượt tại concurrency $c=4$ (`r1-c4`, `r2-c4`, `r3-c4`).
  * Mỗi lượt truyền 32 objects $\times$ 4 MiB = 128 MiB (tổng cộng 192 requests PUT và 192 requests GET).
* Viết script phân tích, trích xuất số liệu thống kê: [parse_benchmark.py](file:///d:/BigData/BigdataWeek1/parse_benchmark.py).
* Vận hành tiến trình nền lấy mẫu tài nguyên CPU/RAM (`kubectl top pod`) định kỳ 5 giây/lần trong suốt quá trình tải: [evidence/resource-samples.txt](file:///d:/BigData/BigdataWeek1/evidence/resource-samples.txt).
* Tổng hợp bảng kết quả chuẩn hóa: [benchmark-summary.csv](file:///d:/BigData/BigdataWeek1/benchmark-summary.csv).

### 3. Hiện trạng sau khi thực hiện (After Behaviour)
* 100% các request đạt tỷ lệ thành công tuyệt đối: **192/192 PUT thành công** và **192/192 GET được xác thực toàn vẹn mã băm SHA-256** (0 lỗi hash).
* Cung cấp bằng chứng số liệu máy đọc được (`.jsonl`, `.csv`, `.txt`), bác bỏ giả thuyết ban đầu một cách khoa học: Concurrency 4 không làm tăng Goodput (tỷ lệ trung vị $c4/c1$ đạt $0.96\times$ với PUT và $0.90\times$ với GET), đồng thời làm tăng mạnh độ trễ đuôi p95 từ $\sim 110\text{ ms}$ lên $\sim 686\text{ ms}$.

### 4. Artifacts định danh
* **Mã commit:** `7a6881740439fcafc32416bad25c1abab23ba8c1`
* **File kết quả:**
  * [benchmark-summary.csv](file:///d:/BigData/BigdataWeek1/benchmark-summary.csv)
  * [parse_benchmark.py](file:///d:/BigData/BigdataWeek1/parse_benchmark.py)
  * [evidence/r1-c1.jsonl](file:///d:/BigData/BigdataWeek1/evidence/r1-c1.jsonl), `r1-c4.jsonl`, `r2-c1.jsonl`, `r2-c4.jsonl`, `r3-c1.jsonl`, `r3-c4.jsonl`
  * [evidence/resource-samples.txt](file:///d:/BigData/BigdataWeek1/evidence/resource-samples.txt)

---

## PHẦN 2: BÀI KIỂM TRA THỰC HÀNH ĐỘC LẬP (INDEPENDENT PRACTICAL CHECK) — 10 ĐIỂM

* **Nhiệm vụ:** Tự tay tính toán lại Goodput và Nearest-rank p95 từ log thô của 1 trial.
* **Trial tính toán:** Trial 1 (Prefix: `bench/r1-c1`, Concurrency: 1)
* **File dữ liệu thô:** [evidence/r1-c1.jsonl](file:///d:/BigData/BigdataWeek1/evidence/r1-c1.jsonl)

### 1. Tính toán lại Application Goodput (MiB/s)
Công thức chuẩn:
$$\text{Goodput} = \frac{\text{Tổng dung lượng bytes hợp lệ}}{2^{20} \times \text{wall\_s}}$$
*(Trong đó: $2^{20} = 1,048,576\text{ bytes} = 1\text{ MiB}$ theo chuẩn nhị phân IEC).*

* **Pha PUT (Ghi dữ liệu):**
  * Số object thành công: $32 / 32$ objects (mỗi object $4,194,304\text{ bytes}$).
  * Tổng bytes = $32 \times 4,194,304 = 134,217,728\text{ bytes} = 128.00\text{ MiB}$.
  * Thời gian đo (`wall_s`): $3.0335\text{ giây}$.
  * **Tính toán:** $\text{Goodput}_{\text{PUT}} = \frac{128}{3.0335} = \mathbf{42.20\text{ MiB/s}}$.
  * *(Khớp chính xác với dòng summary trong `r1-c1.jsonl` và `benchmark-summary.csv`)*.

* **Pha GET (Đọc và xác thực hash):**
  * Số object đọc và kiểm tra hash thành công: $32 / 32$ objects.
  * Tổng bytes = $128.00\text{ MiB}$.
  * Thời gian đo (`wall_s`): $0.6212\text{ giây}$.
  * **Tính toán:** $\text{Goodput}_{\text{GET}} = \frac{128}{0.6212} = \mathbf{206.04\text{ MiB/s}}$.

---

### 2. Tính toán lại Nearest-Rank p95 Latency (ms)
* **Phương pháp:** Nearest-Rank Percentile (không nội suy).
* **Chỉ số rank:**
  $$k = \lceil 0.95 \times n \rceil = \lceil 0.95 \times 32 \rceil = \lceil 30.4 \rceil = 31$$
  *(Lấy giá trị phần tử thứ 31 trong mảng 32 giá trị độ trễ ms đã sắp xếp tăng dần).*

* **Pha PUT:**
  * Sắp xếp 32 giá trị ms tăng dần: phần tử thứ 31 là **`109.34 ms`**.
  * $\Rightarrow \mathbf{p95_{\text{PUT}} = 109.34\text{ ms}}$.
* **Pha GET:**
  * Sắp xếp 32 giá trị ms tăng dần: phần tử thứ 31 là **`23.81 ms`**.
  * $\Rightarrow \mathbf{p95_{\text{GET}} = 23.81\text{ ms}}$.

---

### 3. Kết quả kiểm tra chéo (Cross-Check với Role C)
* **Nhiệm vụ:** *"Re-run two of C’s expected denials"*.
* Thực thi trực tiếp 2 lệnh cấm từ client `ingestor` và `analyst`:
  1. **Lệnh 1 (S04):** `ingestor` PUT `research-release/auth-probe.txt`
     * Kết quả: **HTTP 403 Forbidden, Code: AccessDenied** (Đạt).
  2. **Lệnh 2 (S10):** `analyst` GET `research-raw/fixture.txt`
     * Kết quả: **HTTP 403 Forbidden, Code: AccessDenied** (Đạt).
* Xác nhận: Quyền bucket-scoped của Role C được thực thi chính xác.

---

## PHẦN 3: TRẢ LỜI CÂU HỎI VẤN ĐÁP (EXIT QUESTIONS) — 10 ĐIỂM (4 + 3 + 3)

### Câu 1 (4 điểm):
**Why are Kubernetes observer permissions separate from S3 reader permissions, and why is a namespace operator outside the claimed isolation boundary?**

* **Trả lời:**
  1. *Tách biệt quyền:* Kubernetes observer và S3 reader thuộc hai mặt phẳng độc lập:
     * Observer thuộc **Mặt phẳng quản trị (Control Plane)**: Sử dụng Kubernetes RBAC để truy vấn API Server (`get`, `list`, `watch` pods, logs, events) nhằm giám sát hạ tầng.
     * S3 reader thuộc **Mặt phẳng dữ liệu (Data Plane)**: Sử dụng S3 AccessKey/SecretKey gửi qua cổng TCP 8333 do tiến trình SeaweedFS xác thực và ủy quyền trực tiếp. Có quyền quan sát Kubernetes API không hàm ý quyền đọc các object trong bucket S3 và ngược lại.
  2. *Namespace operator nằm ngoài biên giới cách ly:* Người có quyền quản trị namespace (namespace operator) có thể tạo Pod tùy ý và mount bất kỳ Secret nào trong namespace (kể cả `s3-config` hay `s3-owner`). Hơn nữa, NetworkPolicy chỉ dùng nhãn pod (`access: s3`) làm selector chứ không phải danh tính mã hóa (cryptographic identity), nên namespace operator có thể dễ dàng gắn nhãn này cho bất kỳ Pod nào để vượt rào mạng. Vì vậy, hệ thống không thể tự cô lập khỏi chính người quản trị namespace.

---

### Câu 2 (3 điểm - Trọng tâm của Role D):
**Why might concurrency four be slower or have higher latency than concurrency one? Connect your answer to evidence or a plausible bottleneck.**

* **Trả lời:**
  Dựa trên bằng chứng từ 6 trials và file `resource-samples.txt`:
  1. **Nghẽn tài nguyên CPU (CPU Throttling & Context Switching):** Trong `store.yaml`, container SeaweedFS bị giới hạn cứng `limits: {cpu: "1", memory: 1Gi}`. Server chỉ có 1 core CPU. Khi 4 luồng client đồng thời đẩy các file 4 MiB, 1 core CPU này phải liên tục chia nhỏ thời gian xử lý phân luồng và giải mã HTTP socket. Chi phí chuyển đổi ngữ cảnh (context switching) làm giảm hiệu suất thực tế của CPU.
  2. **Tranh chấp khóa và nghẽn I/O đĩa cục bộ (Disk Lock Contention):** Toàn bộ dữ liệu ghi vào một volume PVC duy nhất (`object-data`, StorageClass `local-path`). Khi 4 luồng cùng ghi đồng thời 4 khối 4 MiB vào cùng một filesystem, cơ chế khóa hệ thống file và hoạt động `fsync` buộc các tiến trình ghi phải xếp hàng (serialization), khiến request phải chờ đợi nhau, đẩy độ trễ p95 tăng vọt từ $\sim 110\text{ ms}$ lên $\sim 686\text{ ms}$.
  3. **Môi trường mạng nội bộ (Zero RTT benefit):** Client và Storage Pod chạy trên cùng một node máy ảo, độ trễ mạng cực nhỏ ($< 1\text{ ms}$). Concurrency cao chỉ phát huy tác dụng trên mạng WAN để che giấu độ trễ truyền gói tin (hide RTT). Trong môi trường local, 1 luồng đã tận dụng tối đa băng thông, việc tăng lên 4 luồng chỉ tạo thêm hàng đợi tranh chấp.

---

### Câu 3 (3 điểm):
**Why does Pod recovery not establish backup, high availability or enforced retention?**

* **Trả lời:**
  1. *Không phải High Availability (HA):* Deployment dùng 1 replica với chiến lược `Recreate`. Khi xóa Pod, hệ thống bị gián đoạn hoàn toàn ($T_{observed} = 10.05\text{s}$) trước khi Pod mới Ready. Hệ thống HA thực thụ yêu cầu nhiều replica phân tán (multi-replica active-active/active-passive) với thời gian downtime bằng 0.
  2. *Không phải Sao lưu (Backup):* Thực nghiệm chỉ chứng minh dữ liệu sống sót qua vòng đời của container trên CÙNG MỘT persistent volume (PVC UID giữ nguyên). Nếu hỏng ổ đĩa vật lý của máy chủ, toàn bộ dữ liệu sẽ mất. Backup đòi hỏi bản sao độc lập, đặt ở vị trí địa lý khác (off-site) và hỗ trợ khôi phục theo thời điểm (point-in-time recovery).
  3. *Không phải Enforced Retention:* Cam kết lưu trữ trong `governance.json` chỉ là siêu dữ liệu (metadata), không có cơ chế kỹ thuật WORM (Write Once, Read Many) hay S3 Object Lock ngăn chặn xóa. Người dùng có quyền ghi (như `owner`) vẫn có thể xóa hoặc sửa đổi dữ liệu bất cứ lúc nào.

---

**XÁC NHẬN:**  
**Trần Anh Tuấn — MSSV: 24022484 — Role D (Performance Engineer)**
