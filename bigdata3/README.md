# Lab 3 — Hồ sơ role A, D và E

Gói gồm ba hồ sơ cá nhân cùng bố cục: A — Trần Anh Tuấn (24022484), D — Đàm Quang Tiến (24022463), E — Lê Toàn (24022466). Mở `experiment_report.md` và `individual/` để xem nội dung, kiểm tra chéo và hướng dẫn bảo vệ. Nhóm không phân công thành viên B; việc kiểm tra trung bình/mẫu số được thực hiện trực tiếp trên build_gold. Phần E có năm gate bắt buộc, các ca thiếu khóa, review Gold độc lập từ bảng Delta thật, catalog, lineage và biên bản `release_review.json`. Các hàm mô hình/merge và bằng chứng pipeline được giữ để tái lập; gói gồm hồ sơ A/D/E, không tạo hồ sơ cho một thành viên B. `evidence/` chứa các đầu ra thật; `portable_delta/` giữ log và dữ liệu của các bảng Delta vận hành, A/B benchmark và schema probe. Bảng benchmark tách khỏi Gold.

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
docker compose exec -T driver python tools/check_role_e.py --config config/team.json --trusted-manifest /instructor/trusted_lab3_inputs.json
```

Restore chỉ nhận các prefix bảng trống. File timings giữ nguyên kết quả đã đo; chạy lại benchmark sẽ tạo các thời gian mới. File scan metrics đã xuất vẫn gắn với SQL execution ID của lần nộp; Spark History Server của môi trường mới chỉ có event log từ các lần chạy mới.

`.env` được setup tạo riêng và bị loại khỏi gói nộp. Không cần cài Python/Java/Spark trực tiếp trên Windows; image dựng môi trường Linux và ghim thư viện/JAR. Bản nộp là technical release candidate; công bố owner và trình bày cá nhân là các hoạt động riêng.
