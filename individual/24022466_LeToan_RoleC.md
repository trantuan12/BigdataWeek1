# BÁO CÁO CÁ NHÂN — INDIVIDUAL ASSESSMENT

**Học phần:** Cloud-Native Big Data Infrastructure & Governance

| Thông tin | Nội dung |
|---|---|
| Họ và tên | Lê Toàn |
| MSSV | 24022466 |
| Vai trò | Role C — Access Engineer |
| Nhiệm vụ | Task 3 — Prove access governance |
| Namespace kiểm thử trong bằng chứng | `bd-g01` |
| Context kiểm thử trong bảng kết quả | `minikube` |

---

## Phần 1: Đóng góp kỹ thuật (10 điểm)

### Trước khi thực hiện

Sau Task 1 và Task 2, nhóm có các cấu hình quota, NetworkPolicy, observer Role và dịch vụ lưu trữ SeaweedFS với các bucket, client và fixture phục vụ kiểm thử. Tuy nhiên, cấu hình khai báo hoặc trạng thái Pod đang chạy chưa đủ chứng minh quyền truy cập được thực thi đúng. Cần gửi request thực tế và đối chiếu với ma trận Allow/Deny của đề bài.

Vai trò của tôi là kiểm chứng ba lớp kiểm soát: quyền S3 đối với object, Kubernetes RBAC đối với API quản trị và NetworkPolicy đối với kết nối mạng. Tôi giữ kết quả dự kiến, danh tính thực hiện và đầu ra thực tế để phân biệt truy cập bị từ chối đúng chính sách với lỗi môi trường.

### Công việc đã làm

Đóng góp của tôi được thể hiện qua các log kiểm thử Task 3, [bảng kết quả](../security-results.csv) và [hồ sơ governance](../governance.json) trong bộ bài nộp. Bảng kết quả ghi người vận hành bằng MSSV `24022466` và dẫn đường đến log của từng test để giảng viên đối chiếu trực tiếp sau khi giải nén.

- **12 test S3:** kiểm tra PUT, GET, LIST và DELETE của `ingestor`, `analyst`, cùng request anonymous. Các test sử dụng fixture đã tồn tại và object dùng riêng cho thử nghiệm.
- **6 test Kubernetes RBAC:** dùng kubeconfig token riêng của `observer` để gửi request thật: đọc Pods, events và storage Pod logs; thử đọc Secret, tạo Pod và xóa storage Pod. Hai request thay đổi Pod dùng `--dry-run=server`.
- **4 test mạng:** kiểm tra client được gắn label, client không được gắn label, cổng quản trị của storage và nguồn outsider. Lưu DNS checks, permitted controls và kết quả kết nối TCP.
- **Governance:** ghi trách nhiệm, mục đích sử dụng, phân loại, retention, ngày cleanup dự kiến và chính sách truy cập cho hai bucket. Không coi retention metadata là cơ chế tự động được thực thi.

Các artifact chính:

- [Bằng chứng request S3 và RBAC](../evidence/run-20261002T162540Z/) — stdout, stderr và exit codes được lưu theo ID.
- [Bằng chứng network và outsider](../evidence/task3/) — DNS, routing control, kết quả N04 và snapshot policy cuối cùng.
- [security-results.csv](../security-results.csv) — ma trận 22 test cùng principal, expected, observed, operator, reviewer và đường dẫn bằng chứng.
- [governance.json](../governance.json) — record của `research-raw` và `research-release`.
- [policies-redacted.json](../policies-redacted.json) — các action được cấp, không chứa access key hoặc secret key.
- [Chỉ dẫn bằng chứng Task 3](../evidence/task3/README.md) — giải thích phạm vi kết luận và phần còn thiếu.

### Kết quả và giới hạn

Trạng thái dưới đây phản ánh hồ sơ hiện tại sau khi rà soát bằng chứng, không phải tuyên bố tất cả test đã PASS.

| Nhóm test | Kết quả hiện tại | Bằng chứng và cách hiểu |
|---|---|---|
| S01–S12 | 12 PASS | Request được phép thành công; request bị cấm trả HTTP 403 với `AccessDenied` |
| K01–K06 | 6 PASS | Request đọc được cho phép; request bị cấm trả `Forbidden` đúng principal observer |
| N01–N02 | 2 PASS | Owner kết nối được; blocked timeout với DNS và permitted controls |
| N03 | INCONCLUSIVE | Có remote timeout nhưng chưa có bằng chứng rõ ràng rằng local port 8888 đang LISTEN |
| N04 | PENDING-INSTRUCTOR | Local routing control thành công nhưng chưa có xác nhận outsider fixture được giảng viên chỉ định/chấp nhận |
| Tổng | 20 PASS và 2 mục chưa chốt | Cần hoàn thiện điều kiện bằng chứng trước khi tuyên bố đủ 22 PASS |

N03 chưa thể kết luận chỉ từ timeout, vì một cổng không lắng nghe cũng khiến kết nối thất bại. N04 đã có bằng chứng outsider kết nối được khi thêm ingress allowance tạm thời, rồi bị chặn sau khi gỡ allowance; phần còn thiếu là xác nhận fixture và môi trường theo yêu cầu đề bài. Lần chạy ban đầu và trạng thái sau rà soát phải được giữ để người đánh giá theo dõi được thay đổi.

---

## Phần 2: Kiểm tra thực hành độc lập (10 điểm)

**Nhiệm vụ của Role C:** thực hiện một request analyst được phép và một request analyst bị cấm, rồi giải thích vì sao kết quả khác nhau.

Phần dưới phân tích log S06 và S08 đã lưu trong Task 3. Những log này chứng minh kết quả request, nhưng chưa phải bộ log riêng xác nhận một phiên kiểm tra cá nhân độc lập trong 15 phút cuối. Khi thực hiện phiên kiểm tra đó, tôi cần lưu stdout, stderr, thời điểm, người thao tác và giải thích vào thư mục mới; không dùng log nhóm để mặc định xác nhận đã hoàn thành yêu cầu độc lập.

### Request được phép là S06

```powershell
$env:NS = 'bd-g01'
kubectl -n $env:NS exec analyst -- python /opt/s3lab.py `
  probe get research-release fixture.txt
```

Lệnh chạy helper trong Pod `analyst`, dùng S3 credentials của analyst để đọc object `fixture.txt` trong bucket `research-release`. Quyền khai báo của analyst là `Read:research-release` và `List:research-release`, nên GET này phải được cho phép.

Kết quả quan sát trong [S06.stdout](../evidence/run-20261002T162540Z/S06.stdout):

| Trường | Giá trị |
|---|---|
| Principal | `analyst` |
| Operation | `get` |
| Bucket và key | `research-release/fixture.txt` |
| HTTP | `200` |
| Error | `null` |
| ok | `true` |
| Bytes | `25` |
| SHA-256 | `9ce4c8bb96c85122c3b386653fbe9150cb2f4df03f25c83408fe11c29b87d6f8` |

Digest trùng với fixture chuẩn trong helper. Điều này xác nhận analyst không chỉ kết nối được đến storage mà còn thực hiện một request S3 được cho phép và nhận đúng nội dung fixture.

### Request bị cấm là S08

```powershell
kubectl -n $env:NS exec analyst -- python /opt/s3lab.py `
  probe put research-release auth-probe.txt
```

Lệnh thử ghi một object tổng hợp vào cùng bucket release. Analyst chỉ có Read/List, không có Write, nên PUT này phải bị từ chối.

Kết quả quan sát trong [S08.stdout](../evidence/run-20261002T162540Z/S08.stdout):

| Trường | Giá trị |
|---|---|
| Principal | `analyst` |
| Operation | `put` |
| Bucket và key | `research-release/auth-probe.txt` |
| HTTP | `403` |
| Error | `AccessDenied` |
| ok | `false` |

`403/AccessDenied` là bằng chứng authorization denial đúng yêu cầu. Mã thoát `2` của helper ở request bị từ chối là hành vi dự kiến; bản thân exit code không quyết định test PASS. Timeout, lỗi DNS, `404` hoặc lỗi chữ ký phải được ghi là chưa đủ kết luận cho mục tiêu phân quyền.

### Giải thích sự khác biệt và permitted control

Hai request dùng cùng principal và cùng bucket, nhưng action khác nhau: GET tương ứng quyền Read được cấp, còn PUT yêu cầu Write không được cấp. Vì vậy, sự khác biệt nằm ở S3 action policy. NetworkPolicy chỉ quyết định đường kết nối có đến được storage; nó không phân biệt GET với PUT.

Khi chạy kiểm tra độc lập, tôi lặp lại S06 sau S08 để xác nhận credentials và đường kết nối của analyst vẫn hoạt động. Có thể lưu phiên mới như sau trong PowerShell:

```powershell
$checkDir = 'evidence/task3/individual-C-' + `
  (Get-Date -Format 'yyyyMMdd-HHmmss')
New-Item -ItemType Directory -Path $checkDir -Force | Out-Null

kubectl -n $env:NS exec analyst -- python /opt/s3lab.py `
  probe get research-release fixture.txt `
  1> "$checkDir/allow.stdout" 2> "$checkDir/allow.stderr"
$LASTEXITCODE | Set-Content "$checkDir/allow.exit"

kubectl -n $env:NS exec analyst -- python /opt/s3lab.py `
  probe put research-release auth-probe.txt `
  1> "$checkDir/deny.stdout" 2> "$checkDir/deny.stderr"
$LASTEXITCODE | Set-Content "$checkDir/deny.exit"

kubectl -n $env:NS exec analyst -- python /opt/s3lab.py `
  probe get research-release fixture.txt `
  1> "$checkDir/control.stdout" 2> "$checkDir/control.stderr"
$LASTEXITCODE | Set-Content "$checkDir/control.exit"
```

Sau khi chạy, kiểm tra các JSON, đối chiếu SHA-256 và ghi giải thích cùng MSSV `24022466`, thời điểm UTC, context và namespace. Không đưa credentials hoặc kubeconfig vào bằng chứng.

### Kiểm tra chéo theo trách nhiệm Role C

Role C cần kiểm tra việc từ chối Pod oversized của Role A có thực sự do quota. Trong [quota-reject.txt](../evidence/quota-reject.txt), thông báo ghi `exceeded quota: team-budget`, `requested: requests.cpu=3`, `used: requests.cpu=0`, `limited: requests.cpu=2`.

Request CPU 3 vượt quota requests.cpu 2 ngay cả khi chưa có CPU requests đang sử dụng. Đây là admission rejection do quota, khác với Pod Pending, image pull lỗi hoặc YAML không hợp lệ. File này hỗ trợ đối chiếu kết quả của A; nó không tự chứng minh tôi đã thực hiện một lần kiểm tra chéo riêng. Phiên kiểm tra chéo thực tế cần ghi người chạy, reviewer và đầu ra riêng.

---

## Phần 3: Câu hỏi vấn đáp (10 điểm)

### Câu 1

**Why are Kubernetes observer permissions separate from S3 reader permissions, and why is a namespace operator outside the claimed isolation boundary?**

Kubernetes observer và S3 reader được xác thực và phân quyền bởi hai hệ thống riêng.

- **Kubernetes API:** ServiceAccount `observer` dùng token Kubernetes. RBAC cho phép đọc Pods, logs và events để quan sát hạ tầng. Trong [K04.stderr](../evidence/run-20261002T162540Z/K04.stderr), API server từ chối đọc `s3-config` và ghi đúng principal `system:serviceaccount:bd-g01:observer`.
- **S3 API:** analyst dùng S3 credentials để gửi request đến Service `objects` trên TCP 8333. SeaweedFS áp dụng bucket-scoped action policy. S06 được phép GET release, trong khi S08 bị cấm PUT vào cùng bucket.

Object payload đi từ client qua Service đến storage Pod, không đi qua Kubernetes API server. Có quyền xem Pod không đồng nghĩa có quyền đọc object; có S3 credentials cũng không tự tạo quyền quản trị Kubernetes.

Namespace operator nằm ngoài ranh giới cách ly được tuyên bố vì operator có thể tạo Pod và mount Secrets trong namespace, qua đó tiếp cận S3 credentials. Operator cũng có thể gắn label `access: s3` lên Pod. Label chỉ là selector, không phải danh tính mật mã. Vì vậy, bài lab chứng minh ranh giới đối với các workload và observer bị giới hạn quyền, chưa chứng minh khả năng chống lại namespace administrator độc hại.

### Câu 2

**Why might concurrency four be slower or have higher latency than concurrency one? Connect your answer to evidence or a plausible bottleneck.**

Theo [benchmark-summary.csv](../benchmark-summary.csv), median goodput PUT giảm từ `43.33 MiB/s` ở c=1 xuống `41.44 MiB/s` ở c=4, tỷ lệ khoảng `0.96`. Median goodput GET giảm từ `206.04` xuống `184.65 MiB/s`, tỷ lệ khoảng `0.90`. Median p95 PUT của ba trial tăng từ khoảng `110.97 ms` lên `686.02 ms`.

Tăng concurrency làm nhiều request cạnh tranh tài nguyên cùng lúc, trong khi storage vẫn có một replica và CPU limit `1`. Những bottleneck có thể gồm giới hạn CPU phía server hoặc client, hàng đợi xử lý, tranh chấp I/O và overhead của nhiều luồng/kết nối. Khi tài nguyên đã gần bão hòa, thêm request đồng thời có thể tăng thời gian chờ thay vì tăng bytes thành công mỗi giây.

Đây là các giải thích có thể phù hợp với số liệu, chưa phải kết luận đã đo được CPU throttling hoặc filesystem lock contention. Bài lab chưa có counters throttling, I/O profiling hoặc phép đo RTT để xác định một nguyên nhân duy nhất. Cache, mạng, backend và tải đồng thời của cluster cũng có thể ảnh hưởng kết quả. Thực nghiệm cho phép kết luận c=4 không cải thiện goodput trong sáu trial này; không suy rộng thành quy luật cho mọi hệ thống S3.

### Câu 3

**Why does Pod recovery not establish backup, high availability or enforced retention?**

**Pod recovery** trong bài lab xác nhận object vẫn đọc được khi thay storage Pod và giữ nguyên PVC. [recovery-summary.json](../recovery-summary.json) ghi Pod UID thay đổi, PVC UID không đổi, 32 object được xác minh trước/sau và observed canary interruption khoảng `10.05 giây`.

Kết quả này chưa chứng minh ba khả năng sau:

1. **High availability:** hệ thống có một storage replica và vẫn có khoảng gián đoạn khi thay Pod. Bài lab chưa thử kiến trúc dự phòng hoặc failover bảo đảm mục tiêu availability khi có sự cố. HA là đáp ứng mục tiêu sẵn sàng; không nên định nghĩa mọi hệ thống HA đều phải đạt tuyệt đối zero downtime.
2. **Backup:** dữ liệu còn trên cùng volume không phải phục hồi từ một bản sao độc lập. Bài lab chưa thử mất volume, mất node hoặc restore từ backup. Để chứng minh backup cần có bản sao phù hợp và kiểm thử khôi phục, không chỉ một PVC giữ nguyên UID.
3. **Enforced retention:** `governance.json` ghi `retention_enforced: false`. Retention và cleanup date là cam kết quản trị, chưa có bằng chứng cơ chế tự xóa đúng hạn hoặc bảo vệ object không bị xóa trước hạn. Pod replacement không kiểm chứng những cơ chế đó.

Graceful Pod replacement cũng chưa chứng minh crash consistency, audit immutability hoặc encryption. Báo cáo phải giữ phạm vi kết luận đúng với thí nghiệm đã thực hiện.

---

**Lê Toàn — MSSV: 24022466 — Role C**

**Trạng thái hoàn thiện:** nội dung đóng góp, phân tích bằng chứng và câu trả lời vấn đáp đã được soạn theo hồ sơ hiện có. Log của phiên thực hành cá nhân độc lập và xác nhận của người thực hiện cần được bổ sung sau khi thực sự chạy; N03 và N04 giữ nguyên trạng thái chưa chốt của Task 3.
