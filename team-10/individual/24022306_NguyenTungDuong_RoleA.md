# BÁO CÁO CÁ NHÂN — INDIVIDUAL ASSESSMENT

**Học phần:** Cloud-Native Big Data Infrastructure & Governance

| Thông tin | Nội dung |
|---|---|
| Họ và tên | Nguyễn Tùng Dương |
| MSSV | 24022306 |
| Vai trò | Role A — Platform Engineer |
| Nhiệm vụ | Task 1 — Establish a bounded tenant (Tenant Guardrails & Resource Budget) |
| Namespace kiểm thử | `bd-g10` |
| Context kiểm thử | `docker-desktop` / `minikube` |


---

## Phần 1: Đóng góp kỹ thuật (10 điểm)

### Trước khi thực hiện

Sau khi cụm Kubernetes khởi tạo, nhóm được bàn giao một namespace trống (`bd-g10`). Tại thời điểm này, cụm hoàn toàn chưa có bất kỳ cơ chế kiểm soát biên tài nguyên (ResourceQuota) hay rào chắn lưu lượng mạng (NetworkPolicy). 

Nếu các tiến trình lưu trữ dữ liệu lớn hoặc client tải dữ liệu tiêu thụ tài nguyên vượt mức (hiện tượng Noisy Neighbor), hệ thống có thể làm cạn kiệt CPU, RAM của các Node vật lý, gây mất ổn định cho toàn bộ nền tảng nghiên cứu. Ngoài ra, việc thiếu kiểm soát quyền hạn có thể khiến các tiến trình tùy ý gọi API máy chủ Kubernetes hoặc lắng nghe trên các cổng không được phép.

Vai trò của tôi là thiết lập Tenant Guardrails, tính toán chi tiết ngân sách tài nguyên nhằm đảm bảo toàn bộ hệ thống lưu trữ và các client hoạt động an toàn trong hạn mức, đồng thời kiểm chứng cơ chế Admission Control của Kubernetes API Server bằng thực nghiệm cụ thể.

### Công việc đã làm

1. **Thiết lập rào chắn bảo vệ qua [`manifests/guardrails.yaml`](../manifests/guardrails.yaml):**
   - **`ResourceQuota` (`team-budget`):** Đặt giới hạn cứng (Hard Limits) cho namespace:
     - `requests.cpu: "2"`, `limits.cpu: "4"`
     - `requests.memory: 2Gi`, `limits.memory: 4Gi`
     - `requests.storage: 8Gi`, `persistentvolumeclaims: "2"`
     - `pods: "10"`
   - **`NetworkPolicy` (`private-object-store`):** Cô lập tầng mạng cho Pod lưu trữ (`app: objects`), chỉ cho phép các Pod mang nhãn `access: s3` kết nối vào cổng TCP 8333. Toàn bộ kết nối khác đều bị drop tại tầng CNI.
   - **`ServiceAccount` & `Role` / `RoleBinding` (`observer`):** Thiết lập tài khoản quan sát viên theo nguyên tắc đặc quyền tối thiểu (Least Privilege). Tắt tự động mount token (`automountServiceAccountToken: false`), Role chỉ cấp quyền `get, list, watch` trên 3 tài nguyên: `pods`, `pods/log`, `events`.

2. **Tính toán và giải thích ngân sách tài nguyên ([`evidence/resource-budget.txt`](../evidence/resource-budget.txt)):**
   - Lập bảng tính chứng minh tổng tài nguyên của **1 Pod lưu trữ SeaweedFS** (Task 2) và **4 Pod Client** (`owner`, `ingestor`, `analyst`, `blocked`) khi hoạt động đồng thời vẫn nằm nghiêm ngặt dưới hạn mức Quota:
     - **Pod Storage (SeaweedFS):** Requests: 500m CPU, 512Mi RAM; Limits: 1 CPU, 1Gi RAM; 1 PVC 4Gi.
     - **4 Pod Client:** Mỗi Pod request 100m CPU, 128Mi RAM; limit 500m CPU, 512Mi RAM $\rightarrow$ Tổng 4 Pod requests: 400m CPU, 512Mi RAM; limits: 2 CPU, 2Gi RAM.
     - **Tổng thực tế toàn hệ thống:**
       - Tổng Requests: $500\text{m} + 400\text{m} = 900\text{m}$ CPU ($\le 2$ CPU Quota) và $512\text{Mi} + 512\text{Mi} = 1024\text{Mi} = 1\text{Gi}$ RAM ($\le 2\text{Gi}$ Quota).
       - Tổng Limits: $1 + 2 = 3$ CPU ($\le 4$ CPU Quota) và $1\text{Gi} + 2\text{Gi} = 3\text{Gi}$ RAM ($\le 4\text{Gi}$ Quota).
       - Số lượng Pod: 5 Pods ($\le 10$ Pods Quota).
       - Số lượng PVC: 1 PVC 4Gi ($\le 2$ PVCs / 8Gi Quota).
   - Giải thích ngắn gọn bằng đoạn văn bản (50–80 từ).

3. **Thực nghiệm Admission Control (Positive & Negative Controls):**
   - **Thử nghiệm tích cực ([`manifests/positive.yaml`](../manifests/positive.yaml)):** Triển khai Pod nhỏ hợp lệ (`quota-positive`) với request 100m CPU / 128Mi RAM và securityContext tăng cường (`runAsUser: 1000`, `runAsNonRoot: true`, drop ALL capabilities, seccomp `RuntimeDefault`). Kubernetes API chấp nhận (Admitted), Pod chạy hoàn tất (`Completed`), sau đó được xóa dọn dẹp an toàn.
   - **Thử nghiệm tiêu cực ([`manifests/negative.yaml`](../manifests/negative.yaml)):** Triển khai Pod yêu cầu vượt hạn mức (`quota-negative`: CPU request = 3, CPU limit = 3). Kubernetes API Server chặn đứng ngay tại giai đoạn Admission Control và ghi nhận lỗi vào [`evidence/quota-reject.txt`](../evidence/quota-reject.txt). Lệnh `kubectl get pod quota-negative` trả về `NotFound`, chứng minh Pod vi phạm không bao giờ được tạo trong cụm.

### Kết quả và bằng chứng

- Bằng chứng trạng thái Quota ban đầu: [`evidence/quota-before.yaml`](../evidence/quota-before.yaml).
- Bằng chứng từ chối Quota: [`evidence/quota-reject.txt`](../evidence/quota-reject.txt) ghi nhận:
  ```text
  Error from server (Forbidden): error when creating "negative.yaml": pods "quota-negative" is forbidden: 
  exceeded quota: team-budget, requested: requests.cpu=3, used: requests.cpu=0, limited: requests.cpu=2
  ```
- File ngân sách tài nguyên: [`evidence/resource-budget.txt`](../evidence/resource-budget.txt).
- Bảng tổng hợp [contribution.csv](../contribution.csv) ghi nhận tôi là Operator của Task 1 và Reviewer của Task 4.

---

## Phần 2: Kiểm tra thực hành độc lập (10 điểm)

### Thao tác được chỉ định: Dự đoán và thử nghiệm Admission Control cho Pod mới

**Yêu cầu:** *Dự đoán khả năng được chấp nhận (Admission) cho một yêu cầu Pod dùng một lần (disposable Pod) do giảng viên cung cấp dựa trên trạng thái Quota hiện hành, sau đó chạy lệnh kiểm chứng.*

#### 1. Trạng thái Quota hiện hành của Namespace `bd-g10`
Tại thời điểm kết thúc bài lab, hệ thống đang duy trì 1 Pod storage và 4 Pod client.
- `used.requests.cpu`: 900m (Hạn mức: 2000m)
- `used.requests.memory`: 1024Mi (Hạn mức: 2048Mi)
- `used.pods`: 5 (Hạn mức: 10)

#### 2. Kịch bản kiểm thử và Dự đoán

* **Kịch bản A (Pod vượt ngưỡng CPU):**
  - Cấu hình Pod `test-fail-quota`: `requests: {cpu: 1500m, memory: 256Mi}`.
  - **Dự đoán:** Tổng request CPU mới sẽ là $900\text{m} + 1500\text{m} = 2400\text{m} = 2.4\text{ CPU} > 2.0\text{ CPU}$ (hạn mức). Yêu cầu sẽ bị Kubernetes **REJECT (Từ chối)** với mã lỗi HTTP 403 Forbidden (`exceeded quota`).
  - **Thực thi kiểm chứng:**
    ```powershell
    kubectl -n bd-g10 run test-fail-quota --image=s3client:v1 --restart=Never \
      --requests='cpu=1500m,memory=256Mi' --dry-run=server
    ```
  - **Kết quả thực tế:** 
    ```text
    Error from server (Forbidden): pods "test-fail-quota" is forbidden: 
    exceeded quota: team-budget, requested: requests.cpu=1500m, used: requests.cpu=900m, limited: requests.cpu=2
    ```
    $\rightarrow$ **Kết quả khớp 100% với dự đoán.**

* **Kịch bản B (Pod hợp lệ):**
  - Cấu hình Pod `test-pass-quota`: `requests: {cpu: 200m, memory: 256Mi}`.
  - **Dự đoán:** Tổng CPU request mới $= 900\text{m} + 200\text{m} = 1100\text{m} \le 2000\text{m}$; Tổng RAM mới $= 1024\text{Mi} + 256\text{Mi} = 1280\text{Mi} \le 2048\text{Mi}$; Số Pod mới $= 5 + 1 = 6 \le 10$. Yêu cầu sẽ được Kubernetes **ADMIT (Chấp nhận)**.
  - **Thực thi kiểm chứng:**
    ```powershell
    kubectl -n bd-g10 run test-pass-quota --image=s3client:v1 --restart=Never \
      --requests='cpu=200m,memory=256Mi' --dry-run=server
    ```
  - **Kết quả thực tế:**
    ```text
    pod/test-pass-quota created (server dry run)
    ```
    $\rightarrow$ **Kết quả khớp 100% với dự đoán.**

---

### Kiểm tra chéo độc lập với Role D (Trần Anh Tuấn — Performance Engineer)

Theo phân công bàn giao kiểm tra chéo (Section 3: *Check D’s resource measurements and units*), tôi đã kiểm tra độc lập các số liệu đo lường của Role D trong file [`benchmark-summary.csv`](../benchmark-summary.csv) và script [`parse_benchmark.py`](../parse_benchmark.py):

1. **Kiểm tra chuẩn đơn vị đo lường:**
   - Xác nhận script sử dụng đúng định nghĩa nhị phân chuẩn IEC: $1\text{ MiB} = 1,048,576\text{ bytes}$ (`1024 * 1024`), không bị nhầm lẫn với đơn vị thập phân SI ($10^6\text{ bytes}$).
   - Tốc độ đường truyền được tính theo đơn vị `MiB_s` = $\frac{\sum bytes}{1024^2 \times wall\_s}$ cho cả hai pha PUT và GET riêng biệt.
   - Thời gian đáp ứng (`p95_ms`) được ghi nhận bằng mili-giây (`ms`).

2. **Kiểm tra tính hợp lệ của mẫu tài nguyên:**
   - Kiểm tra file [`evidence/resource-samples.txt`](../evidence/resource-samples.txt): Các mẫu CPU ghi nhận chính xác theo đơn vị millicores (`m`) và RAM theo mebibytes (`Mi`).
   - Đảm bảo trong suốt quá trình chạy 6 trial, tài nguyên tiêu thụ thực tế của Pod storage không vượt quá limit 1 CPU / 1Gi RAM đã khai báo.

$\rightarrow$ **Kết luận kiểm tra chéo:** Toàn bộ đơn vị đo lường và tính toán của Role D đều chính xác, minh bạch và nhất quán về mặt kỹ thuật.

---

## Phần 3: Câu hỏi vấn đáp (10 điểm)

### Câu 1 (4 điểm)
**Why are Kubernetes observer permissions separate from S3 reader permissions, and why is a namespace operator outside the claimed isolation boundary?**

*Trả lời:*

1. **Sự tách biệt giữa Kubernetes observer và S3 reader:**
   - Hai quyền này thuộc về hai mặt phẳng điều khiển và giao thức hoàn toàn độc lập trong mô hình kiến trúc:
     - **Kubernetes RBAC (Control Plane):** Quyền `observer` tương tác qua cổng HTTPS 6443 của Kubernetes API Server. Quyền này chỉ cho phép truy vấn trạng thái cụm (`get/list/watch` trên `pods`, `events`, `pods/log`). Nó hoàn toàn không hiểu và không quản lý cấu trúc bucket hay object.
     - **S3 API Authorization (Data Plane):** Quyền đọc đối tượng S3 tương tác qua cổng TCP 8333 của dịch vụ SeaweedFS, được xác thực bằng cặp khóa `AccessKey`/`SecretKey` do engine S3 quản lý độc lập.
   - Do đó, việc sở hữu token của `observer` không cung cấp bất kỳ quyền hạn nào để đọc dữ liệu S3 nếu không có khóa truy cập S3 hợp lệ. Điều này đảm bảo nguyên tắc phân tách trách nhiệm (Separation of Concerns).

2. **Lý do Namespace Operator nằm ngoài biên giới cách ly:**
   - Trong mô hình bảo mật của Kubernetes, người quản trị namespace (Namespace Operator) có toàn quyền tạo Pod và đọc các tài nguyên Secret trong namespace đó.
   - Operator có thể dễ dàng gắn (mount) Secret `s3-config` hoặc `s3-owner` vào một Pod tùy ý để chiếm đoạt khóa truy cập cấp cao nhất của S3.
   - Hơn nữa, cơ chế tường lửa `NetworkPolicy` chỉ dựa trên bộ chọn nhãn (`matchLabels: {access: s3}`). Nhãn này là chuỗi metadata thông thường, không phải danh tính mã hóa (cryptographic identity), nên Operator hoàn toàn có thể tự gắn nhãn này lên bất kỳ Pod nào để vượt qua rào chắn mạng. Vì vậy, hệ thống chỉ đảm bảo cô lập giữa các vai trò người dùng thông thường, không thể cô lập trước một namespace operator có quyền quản trị.

---

### Câu 2 (3 điểm)
**Why might concurrency four be slower or have higher latency than concurrency one? Connect your answer to evidence or a plausible bottleneck.**

*Trả lời:*

Dựa trên số liệu thực nghiệm từ 6 lượt chạy và mẫu tài nguyên thu thập được:

1. **Nghẽn trần CPU và chi phí chuyển ngữ cảnh (Context Switching):**
   - Trong file cấu hình [`manifests/store.yaml`](../manifests/store.yaml), Pod lưu trữ bị áp giới hạn cứng `limits: {cpu: "1"}`. SeaweedFS ở chế độ `weed mini` chỉ có tối đa 1 lõi xử lý.
   - Khi tăng số luồng đồng thời từ 1 lên 4 ($c=4$), tiến trình phải liên tục chia sẻ lõi CPU duy nhất này để xử lý giải mã gói tin HTTP, tính toán hàm băm và luân chuyển giữa 4 kết nối song song. Chi phí chuyển đổi ngữ cảnh liên tục làm giảm hiệu suất xử lý thực tế, khiến thông lượng PUT giảm nhẹ từ $43.33\text{ MiB/s}$ ($c=1$) xuống $41.44\text{ MiB/s}$ ($c=4$).

2. **Hiện tượng nghẽn hàng đợi I/O đĩa (I/O Contention & Serialization):**
   - Tất cả 4 luồng đồng thời ghi các khối dữ liệu 4 MiB vào cùng một volume cục bộ (`local-path` PVC).
   - Tầng lưu trữ bên dưới phải thực hiện tuần tự hóa (serialization) các thao tác ghi và gọi `fsync` để đảm bảo dữ liệu ghi an toàn xuống đĩa vật lý. Sự tranh chấp hàng đợi I/O này là nguyên nhân trực tiếp khiến độ trễ phân vị đuôi p95 của pha PUT tăng vọt từ $109.34\text{ ms}$ ($c=1$) lên tới $686.02\text{ ms}$ ($c=4$) — tức độ trễ xấu đi gấp hơn 6.2 lần.

3. **Thiếu vắng lợi thế che giấu độ trễ mạng (RTT):**
   - Trong các hệ thống phân tán qua mạng diện rộng (WAN), tăng concurrency giúp che giấu độ trễ mạng (RTT hàng chục ms) bằng cách gửi gối đầu request. Tuy nhiên, trong bài lab này, client và storage cùng chạy trên một node cục bộ với RTT $< 1\text{ ms}$. Đường truyền đơn luồng vốn đã khai thác tối đa băng thông cục bộ, việc mở thêm luồng chỉ tạo thêm hàng đợi tranh chấp mà không đem lại lợi ích che giấu độ trễ.

---

### Câu 3 (3 điểm)
**Why does Pod recovery not establish backup, high availability or enforced retention?**

*Trả lời:*

Thực nghiệm ở Task 5 chỉ chứng minh khả năng tái gắn kết ổ đĩa khi Pod bị thay thế, hoàn toàn không chứng minh 3 thuộc tính sau:

1. **Không phải High Availability (HA):**
   - Deployment chỉ duy trì đúng **1 bản sao duy nhất (single replica)** với chiến lược `Recreate`.
   - Trong suốt thời gian Pod cũ bị xóa và Pod mới đang khởi động, dịch vụ S3 hoàn toàn ngưng trệ. Tiến trình canary ghi nhận thời gian gián đoạn thực tế $T_{observed} = 10.05\text{ giây}$. Tính sẵn sàng cao (HA) đòi hỏi nhiều bản sao chạy song song (multi-replica) phân tán trên nhiều node với cơ chế chuyển đổi dự phòng tức thì (zero downtime), điều mà mô hình đơn node này không có.

2. **Không phải Sao lưu (Backup) hay Chống mất Node (Node-loss Durability):**
   - Dữ liệu được lưu trữ trên một Persistent Volume sử dụng StorageClass `local-path`. Ổ đĩa này liên kết chặt chẽ với hệ thống tệp cục bộ của đúng node máy chủ đó.
   - Nếu ổ đĩa vật lý bị hỏng hoặc node máy chủ bị sự cố phần cứng, toàn bộ dữ liệu sẽ mất vĩnh viễn. Cơ chế Backup thực thụ đòi hỏi dữ liệu phải được nhân bản độc lập ra các vùng lưu trữ tách biệt về mặt địa lý (off-site backup) và có khả năng khôi phục về các mốc thời gian cụ thể (Point-in-time Recovery).

3. **Không phải Lưu trữ bắt buộc bất biến (Enforced Retention):**
   - Trường thời hạn lưu trữ `retention: "assessment completion + 7 days"` trong file `governance.json` chỉ là một cam kết chính sách bằng văn bản giữa các thành viên.
   - Về mặt kỹ thuật, hệ thống SeaweedFS không bật tính năng WORM (Write Once, Read Many) hay S3 Object Lock. Bất kỳ người dùng nào có quyền `Write` (như vai trò `owner` hoặc `ingestor` trên `research-raw`) vẫn có thể ghi đè hoặc xóa vĩnh viễn các file bất cứ lúc nào trước thời hạn 7 ngày. Do đó, việc lưu trữ không được hệ thống cưỡng chế tự động.

---

**Người thực hiện: Nguyễn Tùng Dương — MSSV: 24022306 — Role A**
