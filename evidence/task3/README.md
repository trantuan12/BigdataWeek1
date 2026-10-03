# Minh chứng Task 3

Kết quả tổng hợp được lưu tại [security-results.csv](../../security-results.csv). Record của hai bucket nằm tại [governance.json](../../governance.json).

| Nhóm test | Minh chứng | Kết quả |
|---|---|---|
| S01–S12 | [Log S3](../run-20261002T162540Z/), các file S01 đến S12 và S12-control | 12 PASS; các request bị cấm trả HTTP 403 / AccessDenied |
| K01–K06 | Cùng thư mục, các file K01 đến K06 | 6 PASS; các request bị cấm ghi đúng principal observer |
| N01–N02 | N01, N02, N01-after, dns-owner và dns-blocked | 2 PASS; kết nối được phép và DNS controls thành công |
| N03 | N03 và N03-listener | INCONCLUSIVE; chưa có output chứng minh local port 8888 đang LISTEN |
| N04 | N04, N04-routing-control, outsider-dns, outsider-egress-policies, temporary-positive-policy, N04-positive-remove, N01-before-N04, N01-after-N04 và final-policies | PENDING-INSTRUCTOR; local routing control thành công, chưa có xác nhận fixture outsider |

N04 đã có kết nối thành công đến đúng Service khi thêm allowance tạm thời và timeout sau khi gỡ allowance. Đề yêu cầu outsider được chỉ định và staff xác nhận routing/egress, nên chưa thể chuyển sang PASS chỉ dựa trên thử nghiệm local.

RBAC kiểm soát Kubernetes API, S3 policy kiểm soát thao tác object và NetworkPolicy kiểm soát kết nối mạng. Namespace operator có thể mount Secrets và gắn Pod labels nên nằm ngoài phạm vi cách ly đã tuyên bố. Retention được ghi nhận trong governance, chưa được thực thi tự động.
