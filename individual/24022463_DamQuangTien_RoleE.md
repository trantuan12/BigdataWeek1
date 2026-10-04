# BÁO CÁO CÁ NHÂN — INDIVIDUAL ASSESSMENT

**Học phần:** Cloud-Native Big Data Infrastructure & Governance

| Thông tin | Nội dung |
|---|---|
| Họ và tên | Đàm Quang Tiến |
| MSSV | 24022463 |
| Vai trò | Role E — Reliability/Governance Engineer |
| Nhiệm vụ chính | Task 5 — Phục hồi, kiểm tra toàn vẹn và bàn giao bằng chứng |
| Nhóm theo báo cáo chung | Group 10 / `bd-g10` |
| Namespace của lần chạy được phân tích | `bd-g01`, cluster alias `local-lab` |
| Lần chạy | `run-20261003T030554169124Z`, ngày 03/10/2026 |
| Commit đóng góp Task E | `bec58600a66d2ab6c708c33cf92a8a691fde6887` — `task e` |
| Người kiểm tra chéo theo contribution.csv | Lê Ngọc Minh Cường — 24022277 |

Báo cáo bám theo Task 5 và mục 11 của [notebook hướng dẫn](../Lab_1_Cau_hinh_Cluster_VI.ipynb). Số đo được tính lại từ log gốc đã có trong thư mục. Bundle ghi `automation_assisted_by: Codex`, `human_review: PENDING`; phần tính lại dưới đây là kiểm chứng offline có hỗ trợ tự động. Việc sinh viên tự thực hành và reviewer xác nhận chưa được chứng thực bởi log này.

## Phần 1: Đóng góp kỹ thuật (10 điểm)

### Trạng thái trước và sau

**Trước:** hệ thống có Deployment `objects`, Service S3, PVC và các fixture. Tuy nhiên, khả năng đọc khi hệ thống đang chạy chưa đủ chứng minh dữ liệu vẫn tồn tại sau khi thay storage Pod. Trong lần chạy E, kiểm tra prefix `bench/r1-c1/` trả danh sách rỗng, nên cần chuẩn bị 32 object tổng hợp trước khi thử recovery.

**Thay đổi thuộc Role E:** thực hiện thí nghiệm thay storage Pod có kiểm soát, lưu canary và snapshot trước/sau, xác minh toàn vẹn, rồi chạy lại các quyền S3. Commit `bec5860` lưu các artifact này trong `evidence/task5/run-20261003T030554169124Z/`.

**Sau:** có bằng chứng Pod UID đổi, PVC/PV vẫn được sử dụng, 32/32 object đọc đúng trước và sau, thời gian gián đoạn quan sát được khoảng 10,05 giây và ba policy retest khớp kỳ vọng. Kết quả kiểm chứng offline được lưu trong [recovery-check-E.json](recovery-check-E.json) để đối chiếu với raw record.

### Trình tự thí nghiệm và bàn giao

1. Kiểm tra các client `owner`, `ingestor`, `analyst`, `blocked` không còn tiến trình benchmark đang chạy. Phạm vi kiểm tra chỉ bao gồm các client của lab; tải bên ngoài vẫn có thể tồn tại.
2. Đọc chéo fixture của Role B bằng `ingestor` và `analyst`. Cả hai request trả HTTP 200 và cùng SHA-256 mong đợi.
3. Chuẩn bị 32 object, mỗi object 4 MiB, dưới `research-raw/bench/r1-c1/`; verify toàn bộ trước recovery. File `E-data-preparation.jsonl` là bước chuẩn bị dữ liệu của E, không thay thế kết quả benchmark Task D trên máy khác.
4. Lưu snapshot Pod, PVC, Deployment và Service; chạy canary GET `research-raw/fixture.txt` trong 180 giây.
5. Chỉ xóa storage Pod `objects-5dd6544c55-zqzfd` bằng normal termination, `--wait=false`, với grace period mặc định 30 giây. Sau đó chờ Pod cũ bị xóa và kiểm tra rollout.
6. Lưu snapshot mới; GET và kiểm tra SHA-256 của đủ 32 object; chạy lại S06, S08, S10.
7. Tổ chức evidence index dưới đây để người kiểm tra đối chiếu số đo, danh tính tài nguyên, hash và quyền truy cập.

Vai trò governance của E được ghi ở trường `steward: "24022463"` cho cả hai bucket trong [governance.json](../governance.json). Record nêu owner, mục đích, classification, approver, revision, thời hạn lưu và người cleanup. `retention_enforced: false` thể hiện đây là cam kết quản trị được ghi lại, chưa phải cơ chế kỹ thuật bắt buộc lưu giữ hoặc tự động xóa.

### Kết quả và artifact

| Hạng mục | Trước | Sau / kết quả |
|---|---|---|
| Storage Pod | `objects-5dd6544c55-zqzfd` | `objects-5dd6544c55-jrdfv` |
| Pod UID | `0313af21-f773-4ec6-a9b0-afceb47f1ce1` | `589b3bdd-e667-4d9a-94b6-762d7626d267` |
| PVC `object-data` UID | `9484778c-7330-45f9-82f0-cd8e14972a01` | Giữ nguyên |
| PVC status / capacity | `Bound` / 4 GiB | `Bound` / 4 GiB |
| StorageClass của lần chạy E | `standard` | `standard` |
| Node alias trong snapshot | `node-1` | `node-1` |
| Dữ liệu tại `bench/r1-c1` | 32/32 GET đúng SHA-256 | 32/32 GET đúng SHA-256; hash từng key không đổi |
| Canary | 179 mẫu | 10 đọc lỗi, 169 đọc đúng |
| Gián đoạn quan sát được | Tính từ raw canary | **10,049331106999944 giây**, đạt mục tiêu ≤120 giây |
| Chờ xác nhận recovery ở máy chạy lệnh | Summary ghi nhận | **31,281 giây**, đạt mục tiêu ≤120 giây |

Service UID và cấu hình port 8333 không đổi trong snapshot. Địa chỉ ClusterIP đã được che thành `<REDACTED_ADDRESS>`, nên không dùng việc hai chuỗi che giống nhau để tuyên bố đã kiểm chứng độc lập IP gốc.

### Evidence index

Tất cả artifact Task 5 bên dưới nằm trong `evidence/task5/run-20261003T030554169124Z/`:

| Bằng chứng | Nội dung kiểm chứng |
|---|---|
| [commands.jsonl](../evidence/task5/run-20261003T030554169124Z/commands.jsonl) | Lệnh thực tế, target Pod, thời điểm và exit code |
| [prefix-preflight.jsonl](../evidence/task5/run-20261003T030554169124Z/prefix-preflight.jsonl), [E-data-preparation.jsonl](../evidence/task5/run-20261003T030554169124Z/E-data-preparation.jsonl) | Prefix ban đầu rỗng và dữ liệu E chuẩn bị |
| [canary.jsonl](../evidence/task5/run-20261003T030554169124Z/canary.jsonl) | 179 mẫu GET với clock, kết quả và hash |
| [verify-before.jsonl](../evidence/task5/run-20261003T030554169124Z/verify-before.jsonl), [verify-after.jsonl](../evidence/task5/run-20261003T030554169124Z/verify-after.jsonl) | 32 object trước/sau, bytes, HTTP status và SHA-256 |
| [pod-before.json](../evidence/task5/run-20261003T030554169124Z/pod-before.json), [pod-after.json](../evidence/task5/run-20261003T030554169124Z/pod-after.json) | Pod UID, readiness, node và mount `/data` |
| [pvc-before.json](../evidence/task5/run-20261003T030554169124Z/pvc-before.json), [pvc-after.json](../evidence/task5/run-20261003T030554169124Z/pvc-after.json) | PVC UID, PV bind, StorageClass và capacity |
| [service-before.json](../evidence/task5/run-20261003T030554169124Z/service-before.json), [service-after.json](../evidence/task5/run-20261003T030554169124Z/service-after.json) | Service UID và port; địa chỉ đã che |
| [storage-data-layout.json](../evidence/task5/run-20261003T030554169124Z/storage-data-layout.json) | Thư mục metadata `/data/filerldb2`, `/data/m9333` có trên data mount |
| [policy-retests.json](../evidence/task5/run-20261003T030554169124Z/policy-retests.json) và các `policy-S06/S08/S10.jsonl` | Kết quả allowed control và hai denial sau recovery |
| [recovery-summary.json](../evidence/task5/run-20261003T030554169124Z/recovery-summary.json), [independent-check.json](../evidence/task5/run-20261003T030554169124Z/independent-check.json) | Tổng hợp ban đầu để đối chiếu với phép tính lại |

## Phần 2: Practical check độc lập (10 điểm)

**Yêu cầu của E:** tính lại recovery time và giải thích giới hạn của bằng chứng “PVC không đổi”.

### Tính lại từ raw canary

Không lấy sẵn `observed_interruption_s` làm đầu vào phép tính. Đọc từng dòng `canary.jsonl`, kiểm tra clock tăng đúng thứ tự và coi mẫu thành công khi GET trả HTTP 200, `ok = true`, `hash_ok = true`, SHA-256 đúng với fixture.

Theo notebook:

```text
t_failure = start_s của lần đọc thất bại đầu tiên
t_stable  = end_s của lần đọc ĐẦU trong chuỗi 5 lần đọc đúng liên tiếp
T_observed = t_stable - t_failure
```

Các dòng quyết định trong log, đánh số từ 1:

| Dòng | start_s | end_s | Kết quả |
|---|---:|---:|---|
| 5 | 4,059671938 | 4,066438725 | HTTP 200, hash đúng trước lỗi |
| 6 | **5,066702851** | 5,069459587 | `EndpointConnectionError`, lỗi đầu tiên |
| 15 | 14,101761601 | 14,105975237 | Lỗi cuối của đợt này |
| 16 | 15,106350894 | **15,116033958** | Đọc đúng thứ 1 trong chuỗi ổn định |
| 17 | 16,116350582 | 16,122021982 | Đọc đúng thứ 2 |
| 18 | 17,122375251 | 17,128444704 | Đọc đúng thứ 3 |
| 19 | 18,128748276 | 18,133773732 | Đọc đúng thứ 4 |
| 20 | 19,134026958 | 19,139210880 | Đọc đúng thứ 5, xác nhận chuỗi |

Từ giá trị đầy đủ trong raw record:

```text
t_failure = 5.066702851000173 s
t_stable  = 15.116033958000116 s
T_observed = 15.116033958000116 - 5.066702851000173
           = 10.049331106999944 s
           ≈ 10.05 s
```

Dòng 20 dùng để xác nhận có đủ năm lần đọc đúng, nhưng `t_stable` vẫn lấy ở dòng 16. Nếu lấy thời điểm dòng 20 sẽ cộng thêm khoảng 4,02 giây và sai định nghĩa của lab. Tổng số lỗi đếm trực tiếp là **10**, ở dòng 6–15. Phép tính khớp summary và independent check đã lưu.

### Kết quả tính lại và output thực tế

Phép tính ở trên sử dụng trực tiếp các mốc trong raw canary. Việc kiểm chứng offline còn đối chiếu hash từng object với payload xác định trước của workload. Kết quả đã được lưu trong [recovery-check-E.json](recovery-check-E.json), gồm raw samples tại ranh giới lỗi/phục hồi và các kiểm tra:

```json
{
  "status": "PASS",
  "samples": 179,
  "failed_reads": 10,
  "first_failure_line": 6,
  "first_failure_start_s": 5.066702851000173,
  "stable_sequence_first_line": 16,
  "stable_read_end_s": 15.116033958000116,
  "observed_interruption_s": 10.049331106999944,
  "objects_verified_before": 32,
  "objects_verified_after": 32,
  "object_hashes_unchanged": true
}
```

Đây là trích đoạn output. File JSON đầy đủ còn có UID, policy retest và đối chiếu summary. SHA-256 của log khớp hash đã ghi sau khi chuẩn hóa CRLF của Windows thành LF để so sánh; việc đối chiếu không thay đổi file log gốc.

### Diễn giải thời gian và giới hạn lấy mẫu

Canary dùng cùng một monotonic clock cho `start_s` và `end_s`, tránh trộn clock của máy chạy lệnh với clock trong client. `watch` nghỉ một giây **sau** mỗi request; khoảng cách giữa các sample còn bao gồm thời gian xử lý request. Median khoảng cách start-to-start là **1,005575 giây**, lớn nhất **1,041255 giây** trong log này. Do đó 10,05 giây là gián đoạn quan sát được ở độ phân giải lấy mẫu này, chưa phải downtime chính xác liên tục.

Giá trị **31,281 giây** trong summary là thời gian chờ xác nhận recovery ở máy điều khiển, có bước chờ Pod cũ bị xóa. Snapshot Pod mới ghi `Ready=True` từ `03:06:22Z` (10:06:22 giờ Việt Nam), còn lệnh rollout được ghi lúc `03:06:44.235356Z`. Vì vậy không đồng nhất 31,281 giây với downtime S3 hoặc thời điểm chuyển Ready đầu tiên. Không thay thế số đo canary bằng hiệu timestamp giữa hai hệ thống có clock và độ chính xác khác nhau.

### “PVC không đổi” chứng minh được gì?

Hai snapshot giữ nguyên UID `9484778c-7330-45f9-82f0-cd8e14972a01`, cùng `volumeName` và trạng thái `Bound`; Pod trước/sau đều mount claim `object-data` vào `/data`. Điều này chứng minh Pod mới sử dụng lại claim và volume đã bind trong lần thử này. PV có vòng đời độc lập với Pod, nên lưu trữ có thể tồn tại qua lần thay Pod. [Kubernetes — Persistent Volumes](https://kubernetes.io/docs/concepts/storage/persistent-volumes/)

**UID giữ nguyên tự nó chưa chứng minh nội dung dữ liệu không đổi.** Kết luận toàn vẹn cần thêm 32 GET trước/sau, đủ tập key `000.bin`–`031.bin`, kích thước 4 MiB và SHA-256 đúng với payload xác định trước. Kiểm tra lại các log này cho kết quả 32/32 ở cả hai thời điểm, hash của từng key giữ nguyên.

Phạm vi chứng minh là thay Pod bình thường trên cùng volume và cùng node alias. Chưa kiểm thử mất node/ổ đĩa, xóa nhầm object, backup restore, crash consistency, replication hoặc lịch sử audit bất biến. StorageClass của run E là `standard`; không lấy thông tin `local-path` và PVC UID khác ở `evidence/provenance.json` của lần chạy khác để mô tả backend của run E.

### Kiểm tra chéo Role B và retest Role C

Hai fixture read trong [crosscheck-B-ingestor.jsonl](../evidence/task5/run-20261003T030554169124Z/crosscheck-B-ingestor.jsonl) và [crosscheck-B-analyst.jsonl](../evidence/task5/run-20261003T030554169124Z/crosscheck-B-analyst.jsonl) đều trả HTTP 200, 25 bytes và SHA-256:

```text
9ce4c8bb96c85122c3b386653fbe9150cb2f4df03f25c83408fe11c29b87d6f8
```

Sau recovery, ba request analyst cho kết quả:

| ID | Request | Kỳ vọng | Kết quả raw |
|---|---|---|---|
| S06 | GET `research-release/fixture.txt` | Allow | HTTP 200, SHA-256 đúng |
| S08 | PUT `research-release/auth-probe.txt` | Deny | HTTP 403 `AccessDenied` |
| S10 | GET `research-raw/fixture.txt` | Deny | HTTP 403 `AccessDenied` |

S06 là allowed control cho thấy dịch vụ và credential vẫn hoạt động. S08/S10 là denial ở tầng S3 vì đúng request trả `403 AccessDenied`; lỗi kết nối của canary được dùng để đo interruption và không tính là authorization denial.

## Phần 3: Ba exit questions (10 điểm)

### Câu 1: Vì sao quyền Kubernetes observer tách biệt với quyền S3 reader, và namespace operator nằm ngoài isolation boundary?

`observer` được Kubernetes RBAC cấp `get/list/watch` cho `pods`, `pods/log`, `events` qua Kubernetes API. S3 reader dùng AccessKey/SecretKey, được SeaweedFS xác thực và kiểm tra quyền theo bucket trên cổng 8333. Hai cơ chế bảo vệ hai API khác nhau; đọc trạng thái Pod không tự cấp quyền GET object S3. Manifest không cấp observer quyền đọc Secret, tạo Pod hay xóa Pod.

Namespace operator có quyền tạo workload có thể tạo Pod sử dụng Secret chứa credential S3. Vì vậy, cấm `get secrets` trực tiếp chưa đủ tạo biên bảo mật trước một operator có quyền mount Secret trong Pod. Đây là rủi ro Kubernetes nêu ở quyền tạo workload. [Kubernetes — RBAC Good Practices](https://kubernetes.io/docs/concepts/security/rbac-good-practices/#workload-creation)

Ngoài ra, NetworkPolicy của lab chọn client theo label `access: s3`. Operator có thể tạo Pod mang label đó; label chỉ là điều kiện chọn Pod, không phải danh tính mật mã. Policy kiểm soát kết nối mạng, còn quyền thao tác S3 vẫn do S3 credential quyết định. Biên đã kiểm thử áp dụng cho các workload/client được cấu hình trong lab, chưa bao gồm namespace administrator độc hại. [Kubernetes — Network Policies](https://kubernetes.io/docs/concepts/services-networking/network-policies/)

### Câu 2: Vì sao concurrency 4 có thể chậm hơn hoặc latency cao hơn concurrency 1?

Tăng số request đồng thời có thể làm tăng thời gian xếp hàng khi CPU, storage backend hoặc client đã là nút thắt. Server trong `store.yaml` chỉ có một replica, CPU limit 1 core và một PVC; bốn request không làm tăng tài nguyên của server.

Số liệu từ [benchmark-summary.csv](../benchmark-summary.csv), dùng median của ba trial cho mỗi concurrency:

| Metric | c=1 | c=4 | So sánh |
|---|---:|---:|---|
| PUT goodput | 43,33 MiB/s | 41,44 MiB/s | c=4/c=1 ≈ 0,96× |
| GET goodput | 206,04 MiB/s | 184,65 MiB/s | c=4/c=1 ≈ 0,90× |
| PUT p95 latency | 110,97 ms | 686,02 ms | Tăng khoảng 6,18× |
| GET p95 latency | 31,28 ms | 102,36 ms | Tăng khoảng 3,27× |

Đây là bằng chứng concurrency 4 không cải thiện goodput trong thí nghiệm đã lưu, và làm tăng latency đuôi. Các nguyên nhân có thể gồm contention/hàng đợi ở backend, I/O, CPU/client overhead và ảnh hưởng cache. CPU sample của storage trong `resource-samples.txt` nằm từ 9m đến 217m; log lấy mẫu thưa này không chứng minh CPU throttling hay lock tại `fsync`. Vì vậy các cơ chế đó là giả thuyết cần đo thêm, không khẳng định là nguyên nhân đã xác định. Kết quả Task D cũng không tự suy ra hiệu năng của cluster trong run E.

### Câu 3: Vì sao Pod recovery không chứng minh backup, HA hoặc enforced retention?

- **Backup:** dữ liệu được đọc lại trên volume đang dùng, chưa có bản sao độc lập hoặc phép restore. PVC tồn tại qua vòng đời Pod không bảo vệ khỏi hỏng backend, mất node hoặc xóa nhầm dữ liệu.
- **High availability:** cấu hình có một storage replica và đã quan sát 10 lần đọc lỗi trong khoảng 10,05 giây. Thí nghiệm chứng minh phục hồi sau thay Pod; chưa chứng minh dịch vụ tiếp tục đáp ứng qua các lỗi theo một mục tiêu availability.
- **Enforced retention:** `governance.json` ghi thời hạn lưu, người cleanup và `retention_enforced: false`. Chưa có evidence về Object Lock/WORM, cơ chế ngăn xóa/ghi đè trong thời hạn hoặc tác vụ cleanup tự động. Object Lock là ví dụ cơ chế lưu giữ có thực thi; việc backend tương thích S3 không tự chứng minh tính năng đó đã được bật. [AWS — Object Lock](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html)

**Kết luận thực nghiệm:** với lần chạy local được lưu, hệ thống đọc lại đúng toàn bộ 32 object qua Service sau normal Pod replacement và giữ các quyền S3 đã retest. Kết luận chỉ áp dụng cho điều kiện và dữ liệu đã kiểm tra, với các giới hạn nêu trên.

**Đàm Quang Tiến — MSSV: 24022463 — Role E**
