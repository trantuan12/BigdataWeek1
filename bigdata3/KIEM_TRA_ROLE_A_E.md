# Kiểm tra kỹ thuật role A và E

## Role A

Bản hiện có đã có bằng chứng preflight, nhưng báo cáo cũ dùng Java 23 ngoài baseline Java 17/21 và chứa cấu hình đường dẫn không dùng chung với executor. Các tệp đầu vào hiện tại cũng bị đổi xuống dòng làm sai hash. Đã giữ bản trước, khôi phục 11 tệp đúng byte từ ZIP gốc, giữ nguyên manifest tin cậy và chạy lại toàn bộ kiểm tra.

Lượt chạy mới: 16/16 hash đạt; fallback được khai báo đúng, digest logic khớp; Delta từ chối cột lạ và giữ tám dòng; runtime đúng Spark/Delta 4.0.1, Java 17; task chạy trên hai worker container, bảng dùng S3A chung. Container không xác nhận nhiều máy vật lý. Hồ sơ `individual/24022484_TranAnhTuan_RoleA.md` đã được cập nhật bằng kiểm tra trực tiếp Silver v1 và Gold v1. Ba mẫu truy được về đúng object/ordinal/hash; R010001 chọn row 1 ở 19,10°C, không dùng dòng cuối của dictionary. Bằng chứng bổ sung nằm trong `role_A_check.json`, `role_A_source_trace.json`, `runtime_role_A_check.json`, `role_A_worker_probe.json`.

## Role E

Trong thư mục ban đầu, `validate_release_binding` vẫn là TODO và chưa có state/candidate để kiểm tra. Đã tích hợp hàm có sẵn từ bộ bài tập khác và kiểm tra lại bằng kết quả của lượt chạy mới. Năm gate bắt buộc đạt đúng kỳ vọng: valid nhận; mixed_version, self_review, missing_steward, wrong_code từ chối. Bổ sung thử thiếu từng khóa bắt buộc; tất cả từ chối, không lỗi thực thi.

Gold được tái tính từ snapshot Silver ghim và so toàn bộ trường, hash candidate khớp manifest, code đã thực thi được lưu theo hash; catalog và lineage có trong `evidence/release_candidate/` và `lineage.jsonl`. `evidence/role_A_E_review.json` chứa kết quả. Trạng thái là candidate, chưa owner-published; phần review/bảo vệ cá nhân vẫn do thành viên trình bày.

Đã bổ sung hồ sơ `individual/24022466_LeToan_RoleE.md` cùng bố cục A/D và công cụ `tools/check_role_e.py`. Review riêng đọc lại bảng Delta đã ghim, tính độc lập toàn bộ Gold, kiểm catalog/lineage và tự đối chiếu 16 input cùng baseline của A. Kết quả có trong `role_E_check.json`, `role_E_gate_tests.json`, `role_E_edge_cases.json`, `release_review.json`, `role_E_catalog_check.json`, `role_E_lineage_check.json` và `role_E_review_lineage.jsonl`.
