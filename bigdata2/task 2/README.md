# Task 2 — Profile and measure quality

Thư mục này chứa mã nguồn, input, contract, thư viện, báo cáo cá nhân và kết quả Task 2. Có thể sao chép nguyên thư mục để chạy trên máy khác; chương trình không phụ thuộc thư mục của nhiệm vụ khác.

## Cấu trúc

```text
task 2/
├── 24022466_LeToan_RoleB.md
├── contract.json
├── trusted_manifest.json
├── requirements.txt
├── latest-run.json
├── code/
│   ├── prepare_inputs.py
│   ├── typed.sql
│   ├── quality.sql
│   ├── run_tasks.py
│   └── test_quality.py
├── input/
│   ├── observations_a.csv
│   ├── observations_b.jsonl
│   ├── sensors.csv
│   ├── qa_reference.csv
│   └── source_inventory.json
├── schemas/
├── runs/
├── reference/
└── archive/
```

`input/` giữ đúng bốn object đã được lấy qua S3, kèm inventory của nguồn cung cấp. Khi chạy Quality, `prepare_inputs.py` xác minh lại byte length, SHA-256 và record counts với manifest độc lập, tạo snapshot và envelopes, giữ malformed JSON cùng physical provenance. Đây là bước nạp input phục vụ profiling; chương trình không tạo tài nguyên Kubernetes, upload S3 hoặc thực hiện access probes.

`reference/` giữ nguyên đề bài và bộ dataset do giảng viên cung cấp. `schemas/` chứa schema dùng để kiểm tra contract. `archive/` giữ bằng chứng lịch sử của các lần chạy trước khi tách thư mục; dùng `latest-run.json` để tìm kết quả hiện hành.

## Chạy lại

Với môi trường ảo đã có ở thư mục cha, chạy từ thư mục gốc của workspace:

```powershell
.\.venv\Scripts\python.exe 'task 2/code/test_quality.py'
.\.venv\Scripts\python.exe 'task 2/code/run_tasks.py'
```

Để chạy độc lập trên máy khác, mở terminal ngay trong thư mục `task 2`:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe code/test_quality.py
.\.venv\Scripts\python.exe code/run_tasks.py
```

Mỗi lần chạy tạo `runs/<run_id>/` mới và cập nhật `latest-run.json` trong chính thư mục này. Driver xác định đường dẫn theo vị trí mã nguồn nên không phụ thuộc thư mục làm việc của terminal. Có thể thay input bằng `--input-dir` và manifest bằng `--manifest` khi nhận một bộ dữ liệu mới được cung cấp độc lập.

## Kết quả và định nghĩa

Mở `REPORT.md` trong thư mục được ghi ở `latest-run.json`. Kết quả gồm before-quality, after chẩn đoán, metric records, intake profile, defect investigation, biểu đồ, log kiểm thử, runtime versions, code hashes, run record và evidence index. Mỗi lần chạy có snapshot và `restricted/envelopes.csv`, `restricted/work.duckdb` để tái lập truy vấn.

Population W là các deterministic winners trước lọc validity. Uniqueness before dùng key-eligible intake; timeliness chỉ dùng các hàng chronology hợp lệ. NULL là thất bại với quy tắc bắt buộc, mẫu số 0 là `NOT_EVALUATED`. QA coverage và agreement được đo riêng; reference không dùng để sửa hoặc chọn measurement.

Kết quả đã quan sát: raw 10.205; parse failures 5; pre-key rejections 40; key-eligible 10.160; duplicate excess 200; W 9.960. Projection chẩn đoán giữ 9.610 hàng, trong đó có 200 hàng trễ. Năm nhóm counterexample tests đều đạt.

After metrics là projection chẩn đoán; cần tính lại trên candidate thực tế khi Task 3 được bàn giao. Snapshot, envelopes và database có restricted attributes, chỉ dùng trong bài nộp hạn chế. Môi trường hiện chạy Python 3.14.7 và DuckDB 1.4.4; môi trường chuẩn của đề là Python 3.11.

Báo cáo cá nhân: [24022466_LeToan_RoleB.md](24022466_LeToan_RoleB.md).
