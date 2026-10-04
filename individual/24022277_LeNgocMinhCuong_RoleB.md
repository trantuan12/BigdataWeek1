# BÁO CÁO CÁ NHÂN — INDIVIDUAL ASSESSMENT

**Học phần:** Cloud-Native Big Data Infrastructure & Governance

| Thông tin | Nội dung |
|---|---|
| Họ và tên | Lê Ngọc Minh Cường |
| MSSV | 24022277 |
| Vai trò | Role B — Storage Engineer |
| Nhiệm vụ | Task 2 — Deploy persistent object storage |
| Namespace kiểm thử trong bằng chứng | `bd-g01` |
| Context kiểm thử trong bảng kết quả | `kind-bd-g01` |
| Commit đóng góp | `79a6c7c` (message: `Complete Task B: deploy s3 storage, clients and seed data`) |
---

## Phần 1: Đóng góp kỹ thuật (10 điểm)

### 1.1 Trước khi thực hiện (Before State)

Tại thời điểm kết thúc Task 1, namespace `bd-g01` đã được thiết lập các rào chắn bảo vệ (guardrails) bao gồm `ResourceQuota` (để giới hạn tiêu thụ tài nguyên tổng thể) và `NetworkPolicy` (để kiểm soát luồng traffic Ingress vào cụm). Tuy nhiên, môi trường hiện tại hoàn toàn trống rỗng về mặt lưu trữ:
- Chưa có khối lượng lưu trữ bền vững (Persistent Volume/PVC) nào được yêu cầu hay cấp phát.
- Không có ứng dụng cơ sở dữ liệu hay lưu trữ đối tượng (Object Storage) nào đang chạy.
- Chưa có bất kỳ cấu hình định danh (identities), Secret, hay phân quyền nào cho S3.
- Một Client Pod nếu cố gắng gửi request tới cổng 8333 ở thời điểm này sẽ nhận lỗi `Connection Refused` hoặc `DNS Resolution Failure`.

Nhiệm vụ của Role B (Storage Engineer) là xây dựng nên lõi dịch vụ lưu trữ (Storage Core) — một hệ thống S3-compatible cục bộ sử dụng SeaweedFS (teaching mode), đảm bảo tính an toàn dữ liệu, tính liên tục của Volume và cấp quyền truy cập theo từng vai trò cụ thể.

### 1.2 Công việc đã làm (Technical Implementation)

Tôi đã thiết kế và triển khai toàn bộ hệ thống lưu trữ có trạng thái thông qua các hạng mục chính sau:

**A. Thiết lập Định danh và Bảo mật S3 (S3 Identity Generation)**
Tôi đã thực thi kịch bản `make_identities.py` để khởi tạo ba bộ thông tin xác thực (Credentials) độc lập. Script sử dụng các hàm mã hóa ngẫu nhiên an toàn (`secrets.token_hex` và `secrets.token_urlsafe`) để sinh ra `accessKey` và `secretKey`. Điểm mấu chốt ở đây là việc ánh xạ quyền hạn (Action mapping) theo nguyên tắc đặc quyền tối thiểu (Least Privilege):
- **`owner`**: Nhận toàn quyền quản trị (`Admin`, `Read`, `Write`, `List`) để có thể tạo bucket và seed dữ liệu mẫu ban đầu.
- **`ingestor`**: Chỉ được cấp quyền (`Read`, `Write`, `List`) giới hạn nghiêm ngặt trong không gian của bucket `research-raw`.
- **`analyst`**: Chỉ có quyền đọc (`Read`, `List`) đối với bucket `research-release`, không có đặc quyền ghi hoặc xóa để đảm bảo an toàn cho dữ liệu đã phát hành.
Sau khi sinh khóa, tôi đóng gói tất cả cấu hình này vào Kubernetes Secrets. Server cấu hình tổng hợp được lưu vào Secret `s3-config`, trong khi mỗi vai trò được nhận một Secret riêng lẻ (vd: `s3-analyst`). Pod `blocked` không được mount bất kỳ Secret nào, phục vụ cho việc kiểm thử từ chối truy cập (Denial testing).

**B. Triển khai Persistent Storage và Deployment**
Tôi đã áp dụng (apply) tệp khai báo `store.yaml` với cấu trúc chặt chẽ:
- **PersistentVolumeClaim (`object-data`)**: Yêu cầu 4Gi dung lượng với chế độ truy cập `ReadWriteOnce` (chỉ cho phép mount vào 1 node duy nhất), sử dụng `StorageClass` mặc định của cụm. Trong bằng chứng `topology.txt`, PVC này báo trạng thái `Bound` với UID duy nhất `9484778c-7330-45f9-82f0-cd8e14972a01`.
- **Deployment (`objects`)**: Chạy SeaweedFS bằng image đã được phê duyệt (digest pinned). Tôi chủ ý thiết lập `strategy: {type: Recreate}` thay vì `RollingUpdate` để tránh tình trạng hai Pod cùng lúc tranh chấp mount volume `ReadWriteOnce` (điều có thể gây lỗi treo I/O). Pod sử dụng SecurityContext nghiêm ngặt (`runAsNonRoot: true`, drop ALL capabilities) và mount cấu hình auth dưới dạng ReadOnly. Cờ `-s3.config=/auth/s3.json` chặn mọi truy cập nặc danh (anonymous access).
- **Service (`objects`)**: Tạo một ClusterIP nội bộ phơi bày cổng 8333, ẩn hoàn toàn service này khỏi mạng bên ngoài cụm.

**C. Triển khai Client, Seed Data và Kiểm chứng**
Tôi cấu hình 4 client Pods (trong `clients.yaml`) vô hiệu hóa tính năng tự động nạp Kubernetes API Token (`automountServiceAccountToken: false`), chứng minh rằng các client này chỉ cần S3 Endpoint và S3 Credential là đủ để giao tiếp với storage, không cần quyền tương tác với Kubernetes API.
Tiếp theo, tôi cho `owner` nạp dữ liệu mồi (seed) tạo ra 4 object. Kết quả từ `evidence/seed.jsonl` ghi nhận cả 4 lệnh PUT thành công với HTTP 200, độ trễ dao động từ 22ms đến 41ms.

### 1.3 Sau khi thực hiện (After State & Verifications)

Hệ thống lưu trữ đã chuyển từ trạng thái không tồn tại sang trạng thái hoàn toàn khả dụng và bảo mật.
- Bằng chứng `evidence/topology.txt` xác nhận Pod SeaweedFS ở trạng thái `Running (1/1)` và PVC ở trạng thái `Bound`.
- Quan trọng nhất, tôi đã kiểm chứng tính đúng đắn của cơ chế phân quyền bằng dữ liệu thực. Lệnh GET từ `ingestor` (vào `research-raw/fixture.txt`) và `analyst` (vào `research-release/fixture.txt`) đều thành công trả về HTTP 200 (xem log S02 và S06).
- Mã băm SHA-256 trả về từ cả hai file là `9ce4c8bb96c85122c3b386653fbe9150cb2f4df03f25c83408fe11c29b87d6f8`, hoàn toàn trùng khớp với hằng số chuỗi `b"research-landing-zone-v1\n"`. Việc xác thực mã hash này là bằng chứng cao nhất cho thấy toàn bộ Pipeline: Client S3 -> Service -> SeaweedFS Pod -> Filesystem PVC hoạt động hoàn hảo và dữ liệu không hề bị hỏng.

---

## Phần 2: Kiểm tra thực hành độc lập (10 điểm)

**Nhiệm vụ của Role B:** Đóng vai trò là người đánh giá chéo, tôi phải truy vết (trace) độc lập một S3 object đã được xác minh đi qua chuỗi Service, Pod, PVC, nhằm xác nhận tính nguyên vẹn của dữ liệu và UID thực tế sau khi Role E thực hiện thử nghiệm mô phỏng sự cố (xóa Pod).

### 2.1 Truy vết tính liên tục của Volume (Volume Persistence Check)

Mục đích của việc kiểm tra này là đảm bảo Pod mới (sau sự cố) thực sự gắn kết (mount) lại cùng một khối lượng đĩa cứng vật lý chứ không phải tạo ra một volume trống mới.
Tôi đối chiếu hai tệp snapshot trạng thái trước và sau khi xóa Pod (do Role E cung cấp trong `evidence/task5/run-20261003T030554169124Z/`):
- Từ `pvc-before.json` (trước khi Pod bị xóa): UID của `object-data` là `9484778c-7330-45f9-82f0-cd8e14972a01`.
- Từ `pvc-after.json` (sau khi Pod mới khởi động): UID vẫn giữ nguyên là `9484778c-7330-45f9-82f0-cd8e14972a01`, trạng thái là `Bound`.
**Giải thích:** Điều này là bằng chứng không thể chối cãi rằng Storage Class đã giữ lại Volume cũ nhờ vào tính chất ràng buộc vòng đời của PersistentVolumeClaim, bất chấp việc thực thể tiêu thụ (Pod) bị xóa bỏ.

### 2.2 Truy vết sự thay thế của Compute (Pod Identity Check)

Dù Volume giữ nguyên, tôi cần kiểm chứng thực thể thực thi ứng dụng (Pod) đã thực sự được thay mới bởi Deployment Controller.
- Từ `pod-before.json`: Tên Pod cũ là `objects-5dd6544c55-zqzfd`, với UID `0313af21-f773-4ec6-a9b0-afceb47f1ce1`.
- Từ `pod-after.json`: Tên Pod mới là `objects-5dd6544c55-jrdfv`, với UID `589b3bdd-e667-4d9a-94b6-762d7626d267`.
**Giải thích:** UID khác biệt chứng minh một Container hoàn toàn mới đã được dựng lên để tiếp quản, mô phỏng đúng kịch bản phục hồi sau khi bị crash.

### 2.3 Truy vết luồng dữ liệu (Data Path Tracing)

Khi một Client S3 gửi một yêu cầu GET để đọc lại `fixture.txt` (đã lưu), đường đi của Request được xác định như sau:
1. **Client Identity**: Pod `analyst` nạp biến môi trường chứa cặp khóa AWS S3 giả lập.
2. **Network Routing**: SDK S3 sử dụng Endpoint `http://objects:8333`. Cụm CoreDNS phân giải tên `objects.bd-g01.svc.cluster.local` ra địa chỉ ClusterIP (ví dụ 10.96.94.254). Kube-proxy định tuyến gói tin qua EndpointSlice (cổng 8333) đến địa chỉ IP nội bộ của chính xác cái Pod mang UID `589b3bdd...` mới được tạo.
3. **Application Layer**: Tiến trình `weed mini` bên trong Pod lắng nghe trên cổng 8333, xác thực cặp khóa bằng tệp `s3.json` được mount ở `/auth`.
4. **Storage Layer**: Xác thực thành công, SeaweedFS tìm kiếm meta-data, định vị file trên thư mục `/data` (vốn là điểm mount của PVC mang UID `9484778c...`) và trả payload về.

### 2.4 Xác minh toàn vẹn dữ liệu (Integrity Hash Check)

Để hoàn tất truy vết, tôi xem xét kết quả đầu ra của thao tác GET độc lập thực hiện trên môi trường phục hồi, cụ thể tại tệp `crosscheck-B-analyst.jsonl`:
```json
{
  "bucket": "research-release", "key": "fixture.txt",
  "op": "get", "principal": "analyst",
  "http": 200, "ok": true, "bytes": 25,
  "sha256": "9ce4c8bb96c85122c3b386653fbe9150cb2f4df03f25c83408fe11c29b87d6f8",
  "utc": "2026-10-03T03:05:57.238348+00:00"
}
```
**Giải thích kết luận:** File báo mã HTTP 200 và SHA-256 trả về tuyệt đối trùng khớp với digest gốc (`9ce4c8bb...d6f8`). Object 32/32 trong tập benchmark (`verify-after.jsonl`) cũng bảo toàn nguyên vẹn mã băm. Qua quá trình truy vết (Volume không đổi -> Pod tạo mới -> Định tuyến API trơn tru -> Hash trùng khớp), tôi chính thức độc lập xác nhận rằng cơ chế gắn kết bộ nhớ liên tục (Persistent Storage Binding) hoạt động đúng thiết kế và dữ liệu đã được bảo toàn sau thảm họa ở tầng Compute.

---

## Phần 3: Câu hỏi vấn đáp (10 điểm)

### Câu 1 (4 điểm)
**Why are Kubernetes observer permissions separate from S3 reader permissions, and why is a namespace operator outside the claimed isolation boundary?**

Việc tách biệt này xuất phát từ bản chất kiến trúc của hệ thống phân quyền kép.
- **Quyền Kubernetes Observer:** Chịu trách nhiệm kiểm soát Control Plane (mặt phẳng điều khiển). Xác thực được thực hiện qua ServiceAccount Token (JWT) được cấp bởi API Server. Một Role tên là `observer` chỉ được ánh xạ quyền Read-only (`get`, `list`, `watch`) trên các resource metadata như Pods, Events, và Logs. Observer không thể thao tác với Object data. Bằng chứng là khi thử đọc Secret `s3-config` (K04), API trả về mã lỗi HTTP 403 Forbidden với principal rõ ràng: `system:serviceaccount:bd-g01:observer`.
- **Quyền S3 Reader:** Chịu trách nhiệm kiểm soát Data Plane (mặt phẳng dữ liệu). Nó không liên quan đến API Server. S3 SDK ký Request bằng AWS Signature V4 và truyền trực tiếp đến SeaweedFS qua Service (TCP 8333). SeaweedFS đối chiếu chữ ký với tệp cấu hình riêng của nó (`/auth/s3.json`). Quyền hạn ở đây có tính hướng đối tượng (ví dụ: `analyst` có quyền đọc nhưng không có quyền PUT).

Về mặt ranh giới cách ly (Isolation boundary), một **Namespace Operator** (Quản trị viên của Namespace) nằm ngoài vùng an toàn mà lab này có thể kiểm soát. Lý do là Operator có quyền khởi tạo Pod và Mount Secret. Nếu Operator có ý đồ xấu, họ hoàn toàn có thể tự tạo ra một client Pod, đính kèm nhãn `access: s3` (để vượt qua NetworkPolicy) và mount vào các Secret `s3-owner` hoặc `s3-ingestor`. Khi đã có trong tay secret keys, Operator có thể mạo danh bất kỳ role nào để can thiệp dữ liệu. Lab này chỉ bảo vệ dữ liệu chống lại sự rò rỉ quyền hạn giữa các Application Role (như Analyst không sửa được dữ liệu Raw), chứ không thiết kế để chống lại một Admin nội bộ bị tổn thương.

### Câu 2 (3 điểm)
**Why might concurrency four be slower or have higher latency than concurrency one? Connect your answer to evidence or a plausible bottleneck.**

Theo kết quả trích xuất từ báo cáo hiệu năng `benchmark-summary.csv` của Role D:
- Ở cấu hình thử nghiệm `concurrency = 1` (Tuần tự): Median Goodput cho quá trình PUT đạt khoảng `43.33 MiB/s` với p95 Latency khá thấp (chỉ khoảng `110.97 ms`).
- Ở cấu hình thử nghiệm `concurrency = 4` (Đồng thời 4 luồng): Mặc dù kỳ vọng hệ thống sẽ xử lý lượng lớn dữ liệu nhanh hơn, nhưng thực tế Median Goodput của PUT bị kéo lùi nhẹ xuống `41.44 MiB/s` (tỷ lệ khoảng 0.96 so với c=1), và quan trọng nhất, p95 Latency tăng vọt lên tới `686.02 ms` (tăng gấp hơn 6 lần). Độ trễ của quá trình GET cũng chịu tình trạng tương tự.

Có một số điểm nghẽn (bottleneck) kìm hãm lợi ích của đa luồng trong kiến trúc bài lab này:
1. **CPU Throttling tại Storage Server:** Pod lưu trữ SeaweedFS đã bị giới hạn phần cứng cứng ngắc tại `limits.cpu: "1"`. Việc đổ 4 yêu cầu I/O cường độ cao đồng thời vào một CPU core duy nhất sẽ gây ra chi phí chuyển đổi ngữ cảnh (Context Switching) đáng kể. Các tiến trình xử lý request phải tranh giành thời gian CPU, xếp hàng đợi dài hơn, từ đó tăng độ trễ tổng thể thay vì giải quyết công việc song song.
2. **I/O Serialization tại Volume:** Khối lượng lưu trữ sử dụng là `ReadWriteOnce` nằm cục bộ trên một ổ đĩa Node (`local-path`). Khác với các hệ thống phân tán, hệ thống tệp cục bộ có thể xảy ra tình trạng "file locking" (khóa tệp) hoặc I/O Queue khi có nhiều luồng cố gắng flush (ghi) các đoạn buffer 4 MiB xuống đĩa cùng một lúc.
3. **Overhead Kết Nối:** Tại tầng Network, việc duy trì 4 kết nối TCP đồng thời sinh ra các chi phí overhead xử lý gói tin và phân bổ bộ nhớ mà với chỉ một giới hạn CPU=1, server phải gồng gánh xử lý.

Tóm lại, hiện tượng c=4 chậm hơn hoặc có độ trễ cao hơn c=1 trong môi trường này chứng minh rằng việc tăng cường luồng (Concurrency) chỉ đem lại lợi ích hiệu năng (Throughput) khi hệ thống Storage Backend (bao gồm CPU, Network và I/O đĩa) chưa đạt điểm bão hòa (Saturation Point).

### Câu 3 (3 điểm)
**Why does Pod recovery not establish backup, high availability or enforced retention?**

Mặc dù thử nghiệm mô phỏng sự cố (Task 5) cho thấy Pod có thể tự phục hồi và dữ liệu vẫn nguyên vẹn sau thời gian đứt quãng khoảng ~10.05 giây, kết quả này hoàn toàn không đồng nghĩa với ba đặc tính cấp độ doanh nghiệp (Enterprise-grade) sau:

1. **High Availability (Sẵn sàng cao):** Kiến trúc HA đòi hỏi hệ thống phải duy trì khả năng phục vụ liên tục ngay cả khi có thành phần bị lỗi (vd: luôn có 2-3 replicas chạy song song qua Load Balancer). Trong mô hình lab, chúng ta chỉ triển khai `replicas: 1` cùng chiến lược `Recreate`. Trong 10 giây thời gian Pod bị hủy và Pod mới đang khởi động, dịch vụ bị rớt hoàn toàn (downtime). Canary watch đã ghi nhận 10 lỗi GET liên tiếp. Một hệ thống gián đoạn dịch vụ rõ ràng như vậy không thể gọi là High Availability.
2. **Backup (Sao lưu dự phòng):** Sự phục hồi trong lab chỉ là việc thay thế lớp Compute (Pod) và cắm nó lại vào lớp Storage vật lý CŨ (`PVC object-data`). Dữ liệu tồn tại được là do đĩa ảo trên host vẫn còn. Nếu thảm họa xảy ra ở cấp độ phần cứng (Node sập, ổ cứng vật lý hỏng cháy) hoặc có ai đó lỡ tay xóa mất Persistent Volume, toàn bộ dữ liệu sẽ bốc hơi vĩnh viễn. Một hệ thống Backup đúng nghĩa đòi hỏi phải có các bản sao (Snapshot/Replication) được sao chép đến một khu vực lưu trữ hoàn toàn độc lập khác.
3. **Enforced Retention (Cưỡng chế lưu giữ vòng đời):** Tập tin quản trị `governance.json` có ghi một dòng `retention: assessment completion + 7 days`. Tuy nhiên đây chỉ là văn bản cam kết bằng lời (Documentation). Trên thực tế, cờ `"retention_enforced": false` đã được ghi nhận. Không có một cơ chế tự động nào (vd: AWS S3 Object Lock, hay cronjob tự động cleanup) chạy trong nền để bảo vệ tập tin khỏi việc bị thao tác xóa bằng tay trước hạn hay tự dọn dẹp sau hạn. Sự phục hồi của một Pod không chứng minh được tính minh bạch và tính thực thi của vòng đời tài liệu.

---

**Lê Ngọc Minh Cường — MSSV: 24022277 — Role B — Storage Engineer**

**Ký nhận chéo (Cross-check Review):**  
Tôi xác nhận đã đóng vai trò Reviewer độc lập cho bài test phục hồi sự cố (Recovery) của Đàm Quang Tiến (Role E - MSSV 24022463). Việc xác minh tính toàn vẹn thông qua các logs `crosscheck-B-*.jsonl` và `pvc/pod after/before` đã hoàn tất chính xác theo mô tả tại Phần 2.
