# Minh chứng Task 3

Kết quả tổng hợp: [security-results.csv](../../security-results.csv). Quản trị bucket: [governance.json](../../governance.json).

## Cấu trúc thư mục

| Thư mục/file | Nội dung |
|---|---|
| `expected.json` | Kết quả mong đợi của 22 ca kiểm thử |
| [run-20261002T162540Z/](run-20261002T162540Z/) | Lần chạy ban đầu: S01–S12, K01–K06, N01–N03 và các controls |
| [n04-controls/](n04-controls/) | N04, outsider DNS/egress, routing control, allowance tạm thời và policy cuối cùng |
| [run-20261004T150608Z-N03/](run-20261004T150608Z-N03/) | Retest N03 thành công: summary, lệnh đã chạy, stdout/stderr và exit codes |

Mỗi lần chạy giữ riêng minh chứng để theo dõi lịch sử. Các đường dẫn cũ xuất hiện trong nội dung log/lệnh là đường dẫn tại thời điểm thực thi.

## Kết quả hiện tại

| Nhóm test | Minh chứng | Kết quả |
|---|---|---|
| S01–S12 | Các file S01–S12 và S12-control trong lần chạy ban đầu | 12 PASS; request bị cấm trả HTTP 403 / AccessDenied |
| K01–K06 | Các file K01–K06 trong lần chạy ban đầu | 6 PASS; request bị cấm ghi đúng principal observer |
| N01–N02 | N01, N02, N01-after, dns-owner và dns-blocked trong lần chạy ban đầu | 2 PASS; kết nối được phép và DNS controls thành công |
| N03 | [Summary retest](run-20261004T150608Z-N03/summary.json) và logs cùng thư mục | PASS; listener 8888, local TCP trước/sau thành công; owner 8333 kết nối được, 8888 timeout |
| N04 | [Controls N04](n04-controls/) | PENDING-INSTRUCTOR; local routing control thành công, chưa có xác nhận fixture outsider |

Tổng: **21 PASS**, **1 PENDING-INSTRUCTOR**.

Retest N03 ngày 04/10/2026 (22:06 giờ Việt Nam), do Codex chạy trên `minikube/bd-g01`, Pod `objects-74bd94c8cb-gvntk`, IP `10.244.120.85`. NetworkPolicy trước/sau giống nhau; không thay workload. Retest chưa được review độc lập. `commands.jsonl` lưu các lệnh và stdin để tái lập; script retest đã được bỏ.

N04 kết nối thành công đến đúng Service khi thêm allowance tạm thời và timeout sau khi gỡ allowance. Đề yêu cầu outsider được chỉ định và staff xác nhận routing/egress, nên chưa chuyển sang PASS chỉ dựa trên thử nghiệm local.

RBAC kiểm soát Kubernetes API, S3 policy kiểm soát thao tác object và NetworkPolicy kiểm soát kết nối mạng. Namespace operator có thể mount Secrets và gắn Pod labels nên nằm ngoài phạm vi cách ly đã tuyên bố. Retention được ghi nhận trong governance, chưa được thực thi tự động.

`archive/` giữ lần thử N03 chưa hoàn tất do PowerShell xử lý thông báo stderr của `nc`; không dùng để kết luận PASS.
