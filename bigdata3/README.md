# Lab 3 — Hồ sơ role A, B, C, D và E

Gói gồm năm hồ sơ cá nhân cùng bố cục: A — Trần Anh Tuấn (24022484), B — Lê Ngọc Minh Cường (24022277), C — Nguyễn Tùng Dương (24022306), D — Đàm Quang Tiến (24022463), E — Lê Toàn (24022466). Mở `experiment_report.md` và `individual/` để xem nội dung, kiểm tra chéo và hướng dẫn bảo vệ. Phần B xác minh mô hình Silver/Gold `B01`–`B04` (bảo toàn thành phần cộng tính, công thức 11 cột của `build_gold`, trung bình ngày có trọng số so với trung bình các trung bình site) và kiểm tra chéo quyết định bản ghi cũ/đồng hạng của C (`B_CHECK_C`). Phần C xác minh đầy đủ `M01`–`M08` và kiểm tra chéo tính tương đương kết quả A/B của D (`C_CHECK_D`). Phần D thực hiện truy vấn Q1–Q3, biểu đồ có phiên bản, thí nghiệm layout A/B và kiểm tra chéo trung bình có trọng số/mẫu số của B (`D_CHECK_B`). Phần E có năm gate bắt buộc, các ca thiếu khóa, review Gold độc lập từ bảng Delta thật, catalog, lineage và biên bản `release_review.json`. `evidence/` chứa các đầu ra thật; `portable_delta/` giữ log và dữ liệu của các bảng Delta vận hành, A/B benchmark và schema probe. Bảng benchmark tách khỏi Gold.

## Tái lập từ đầu

Giữ ZIP nộp làm bản đóng băng. Giải nén một bản làm việc mới, đặt nguyên thư mục `data/` từ ZIP starter gốc vào dự án, đặt manifest tin cậy do giảng viên cấp ở thư mục cha. Các dữ liệu gốc được phép không đính kèm gói nộp. Mở Docker Desktop rồi chạy:

```powershell
powershell -ExecutionPolicy Bypass -File .\Setup-Lab3.ps1 -Build
```

Đổi cả `run_label` và `evidence_dir` trong `config/team.json` sang tên mới, sau đó chạy:

```powershell
powershell -ExecutionPolicy Bypass -File .\Run-Lab3.ps1 -Step all
```

Chỉ dùng một driver ghi bảng tại một thời điểm. Không sửa hash, không dùng lại prefix bootstrap đã có, không VACUUM.

## Đọc lại đúng các snapshot đã nộp

Trên object store mới, giữ config và evidence bản nộp, khởi động dịch vụ bằng `Run-Lab3.ps1 -Step start`. Khôi phục dữ liệu nguồn `data/` đúng byte và cung cấp trusted manifest như trên. Sau đó:

```powershell
docker compose exec -T driver python tools/portable_delta.py verify
docker compose exec -T driver python tools/portable_delta.py restore
docker compose exec -T driver python tools/verify_live_tables.py --config config/team.json
docker compose exec -T driver python tools/check_role_b.py --config config/team.json
docker compose exec -T driver python tools/check_role_c.py --config config/team.json
docker compose exec -T driver python tools/check_role_d.py --config config/team.json
docker compose exec -T driver python tools/check_role_a.py --config config/team.json --trusted-manifest /instructor/trusted_lab3_inputs.json
docker compose exec -T driver python tools/check_role_e.py --config config/team.json --trusted-manifest /instructor/trusted_lab3_inputs.json
```

Restore chỉ nhận các prefix bảng trống. File timings giữ nguyên kết quả đã đo; chạy lại benchmark sẽ tạo các thời gian mới. File scan metrics đã xuất vẫn gắn với SQL execution ID của lần nộp; Spark History Server của môi trường mới chỉ có event log từ các lần chạy mới.

`.env` được setup tạo riêng và bị loại khỏi gói nộp. Không cần cài Python/Java/Spark trực tiếp trên Windows; image dựng môi trường Linux và ghim thư viện/JAR. Bản nộp là technical release candidate; công bố owner và trình bày cá nhân là các hoạt động riêng.
