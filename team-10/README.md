# Báo cáo nhóm Lab 1 Cluster Configuration

## Phương pháp và các bước thực hiện

Nhóm triển khai SeaweedFS một replica để xây dựng vùng lưu trữ dữ liệu tổng hợp có kiểm soát. Client truyền object qua Service ClusterIP ở cổng 8333; dữ liệu và metadata được lưu trên PVC 4Gi. Nhóm kiểm chứng bằng request thực tế, log và SHA-256, thay vì chỉ dựa vào trạng thái Pod.

**Task 1:** áp dụng ResourceQuota và NetworkPolicy, tạo Pod nhỏ hợp lệ rồi thử Pod yêu cầu 3 CPU. Log ghi `exceeded quota`, giới hạn requests.cpu là 2. Storage cùng bốn client yêu cầu tổng 900m CPU, 1024Mi RAM; tổng limits là 3 CPU, 3Gi RAM.

**Task 2:** cấu hình ba S3 identities, Secret riêng cho từng vai trò, một Deployment và PVC. Owner seed bốn fixture trong hai bucket. Ingestor đọc raw, analyst đọc release; SHA-256 khớp fixture chuẩn. Topology và image/PVC provenance được lưu kèm.

**Task 3:** thực hiện 12 S3, 6 RBAC và 4 network tests. Hồ sơ hiện có 20 PASS; N03 INCONCLUSIVE vì thiếu output local listener 8888; N04 PENDING-INSTRUCTOR vì chưa xác nhận outsider fixture. Request S3 bị cấm trả 403/AccessDenied; observer bị từ chối đọc Secret và thay đổi Pod. Hai request thay đổi Pod dùng server dry-run.

**Task 4:** chạy sáu trial xen kẽ concurrency 1 và 4, mỗi trial 32 object × 4MiB với PUT và GET xác minh hash. Kết quả có 192 PUT thành công, 192 GET nguyên vẹn. Median goodput c4/c1 đạt 0,96 với PUT và 0,90 với GET. CPU, I/O, cache và tải cluster là các yếu tố có thể ảnh hưởng; chưa đo được nguyên nhân duy nhất.

**Task 5:** dừng tải, kiểm tra object trước/sau, thay storage Pod bằng normal deletion và theo dõi canary. Pod UID đổi, PVC UID giữ nguyên; 32 object có hash không đổi. Pod Ready sau 31,28 giây; gián đoạn quan sát là 10,05 giây. Dữ liệu recovery được E chuẩn bị riêng, không phải lần chạy benchmark của D.

## Đóng góp thành viên

[contribution.csv](contribution.csv) ghi năm thành viên, MSSV, vai trò, artifact, commit và reviewer. A phụ trách guardrails; B lưu trữ; C phân quyền; D hiệu năng; E recovery. Báo cáo cá nhân hiện có C và D trong `individual/`.

## Kết quả và minh chứng

`manifests/` chứa cấu hình triển khai. Các bảng kết quả là `security-results.csv`, `benchmark-summary.csv` và `recovery-summary.json`. `governance.json` và `policies-redacted.json` mô tả quản trị, phân quyền. [evidence/README.md](evidence/README.md) chỉ dẫn log và output của năm task. Các helper phục vụ tái lập thí nghiệm được giữ tại thư mục gốc.

## Giới hạn và phần còn thiếu

Các lần chạy dùng môi trường khác nhau; topology/provenance của từng lần phải được đọc cùng log tương ứng. Một replica không chứng minh HA, backup hoặc node-loss durability. HTTP chưa có TLS; retention chưa thực thi tự động. N03/N04 còn chưa chốt; xác nhận thực hành độc lập còn thiếu. Chưa có ảnh chụp minh họa riêng và logs/events sau recovery được xuất riêng. Chưa đóng ZIP.
