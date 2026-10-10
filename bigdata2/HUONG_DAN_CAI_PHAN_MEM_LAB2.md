# Hướng dẫn cài phần mềm để chạy Lab 2

Hướng dẫn dành cho Windows x64 và PowerShell. Bộ bài có thể đặt trong thư mục bất kỳ trên máy. Khi chạy các lệnh liên quan đến bộ bài, mở PowerShell tại thư mục chứa hai file hướng dẫn và `environment-setup/`; đường dẫn bắt đầu bằng `.\` được tính từ thư mục đó. Sau khi chuẩn bị xong, tiếp tục với [Hướng dẫn chạy Lab 2](HUONG_DAN_CHAY_LAB2.md).

## 1. Những thành phần cần có

| Thành phần | Mục đích | Yêu cầu của bộ mã hiện tại |
|---|---|---|
| Python 3.11 trên Windows | Chạy các script điều phối | Lệnh `py -3.11` hoạt động |
| WSL 2 | Backend cho Docker Desktop | Được bật và cập nhật |
| Docker Desktop | Xây dựng image Linux | Docker Engine đang chạy |
| Minikube | Vận hành Kubernetes trên máy | Profile `bd-g10-lab` |
| kubectl | Điều khiển các Pod | Kết nối đúng cụm Kubernetes |
| DuckDB, boto3, jsonschema trong image | SQL, Parquet, S3 và kiểm tra JSON Schema | Cài bằng `install-runtime.ps1` |
| Tenant và object store của Lab 1 | Kho dữ liệu và phân quyền | Namespace `bd-g10` và các tài nguyên ở mục 6 |

Git chỉ cần khi đưa mã lên GitHub. Bộ này không yêu cầu cài Spark, Kafka, Helm, DataHub hay một chương trình DuckDB CLI riêng.

## 2. Cài Python 3.11

Kiểm tra trước:

```powershell
py -0p
py -3.11 --version
```

Nếu đã có Python 3.11 thì bỏ qua phần cài đặt. Nếu chưa có, mở [trang Python 3.11.9 chính thức](https://www.python.org/downloads/release/python-3119/), tải **Windows installer (64-bit)** và cài Python cùng **pip** và **Python Launcher**. Bản này dùng cho script Windows; Python bên trong container được Dockerfile chuẩn bị riêng.

Mở lại PowerShell sau khi cài và kiểm tra `py -3.11 --version`. Dùng `py -3.11` trong hướng dẫn vì lệnh `python` trên máy có thể đang trỏ tới phiên bản khác.

Các script điều phối chính dùng thư viện chuẩn của Python. Để có thêm các thư viện phục vụ kiểm tra kết quả trên Windows, chạy:

```powershell
py -3.11 -m pip install "duckdb==1.4.5" "boto3==1.34.131" "jsonschema==4.26.0"
py -3.11 -c "import duckdb,boto3,jsonschema; print('Thu vien Windows: OK')"
```

**Cài thư viện trên Windows không thay thế việc cài chúng trong image Kubernetes ở mục 7.**

## 3. Chuẩn bị WSL 2

Nếu máy chưa có WSL, mở PowerShell bằng **Run as administrator** và chạy:

```powershell
wsl --install
```

Khởi động lại Windows khi được yêu cầu. Sau đó kiểm tra và cập nhật:

```powershell
wsl --update
wsl --version
```

Nếu Windows báo chưa bật virtualization, bật chức năng ảo hóa CPU trong BIOS/UEFI rồi kiểm tra lại. Chi tiết cài đặt có trong [tài liệu WSL của Microsoft](https://learn.microsoft.com/en-us/windows/wsl/install).

## 4. Cài và mở Docker Desktop

Tải từ [trang cài Docker Desktop cho Windows](https://docs.docker.com/desktop/setup/install/windows-install/). Cài bản x64, chọn backend **WSL 2**, rồi mở Docker Desktop. Image Lab 2 sử dụng **Linux containers**. Đợi Docker Engine hoạt động trước khi kiểm tra:

```powershell
docker version
docker info --format '{{.OSType}}'
```

`docker version` phải có thông tin **Client** và **Server**; lệnh thứ hai phải in `linux`. Nếu chỉ có Client hoặc lỗi pipe `dockerDesktopLinuxEngine`, Docker Engine chưa hoạt động.

## 5. Cài Minikube và kubectl nếu máy chưa có

Kiểm tra:

```powershell
Get-Command minikube,kubectl -ErrorAction SilentlyContinue
minikube version
kubectl version --client
```

Nếu cả hai lệnh đã hoạt động, không cần tải lại. Nếu thiếu, dùng thư mục công cụ dưới tài khoản hiện tại:

```powershell
$labTools = Join-Path $env:LOCALAPPDATA 'Programs\BigdataLab\bin'
New-Item -ItemType Directory -Path $labTools -Force | Out-Null
```

Chỉ chạy lệnh sau nếu thiếu Minikube:

```powershell
Invoke-WebRequest -UseBasicParsing -Uri 'https://github.com/kubernetes/minikube/releases/latest/download/minikube-windows-amd64.exe' -OutFile (Join-Path $labTools 'minikube.exe')
```

Theo [tài liệu Kubernetes](https://kubernetes.io/docs/tasks/tools/install-kubectl-windows/), kubectl nên chênh tối đa một phiên bản minor so với control plane. Cụm Lab 1 dùng để chạy bài này có Kubernetes **1.34.3**; nếu cần cài client tương ứng, chạy:

```powershell
$kubectlVersion = 'v1.34.3'
$kubectlDownload = "https://dl.k8s.io/release/$kubectlVersion/bin/windows/amd64/kubectl.exe"
Invoke-WebRequest -UseBasicParsing -Uri $kubectlDownload -OutFile (Join-Path $labTools 'kubectl.exe')
Invoke-WebRequest -UseBasicParsing -Uri "$kubectlDownload.sha256" -OutFile (Join-Path $labTools 'kubectl.exe.sha256')
$actualHash = (Get-FileHash -Algorithm SHA256 -LiteralPath (Join-Path $labTools 'kubectl.exe')).Hash
$expectedHash = (Get-Content -LiteralPath (Join-Path $labTools 'kubectl.exe.sha256') -Raw).Trim()
if ($actualHash -ne $expectedHash) { throw 'Checksum kubectl khong khop.' }
```

Với tenant khác, chọn kubectl tương thích với phiên bản control plane của tenant đó. Thêm thư mục công cụ vào PATH người dùng và phiên PowerShell hiện tại:

```powershell
$userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
if (($userPath -split ';') -notcontains $labTools) {
    [Environment]::SetEnvironmentVariable('Path', "$labTools;$userPath", 'User')
}
$env:Path = "$labTools;$env:Path"
Get-Command minikube,kubectl
```

Nguồn hướng dẫn Minikube: [cài đặt chính thức trên Windows](https://minikube.sigs.k8s.io/docs/start/).

## 6. Khởi động lại cụm Lab 1 và kiểm tra tenant

Với profile đã có trên máy:

```powershell
minikube start -p bd-g10-lab
kubectl config use-context bd-g10-lab
minikube status -p bd-g10-lab
kubectl get nodes
kubectl -n bd-g10 get deployment objects
kubectl -n bd-g10 get svc objects
kubectl -n bd-g10 get pvc object-data
kubectl -n bd-g10 get secrets s3-ingestor s3-owner s3-analyst
kubectl -n bd-g10 get resourcequota team-budget
kubectl -n bd-g10 get networkpolicy private-object-store
```

Tenant phải có sẵn các thành phần sau:

- Namespace `bd-g10`; Deployment `objects` hoạt động; Service `objects` mở cổng **8333**.
- PVC `object-data` đã Bound và giữ dữ liệu của Lab 1.
- Hai bucket `research-raw`, `research-release` đã tồn tại trong object store.
- Ba Secret với thông tin truy cập S3 riêng: `s3-ingestor`, `s3-owner`, `s3-analyst`.
- Quyền ingestor đọc/ghi raw và bị cấm release; owner có quyền phát hành; analyst chỉ đọc release và bị cấm raw/ghi dữ liệu.
- Quota và NetworkPolicy của Lab 1 vẫn được giữ.

Các lệnh `get secrets` ở trên chỉ liệt kê tên/trạng thái, không hiển thị giá trị Secret. Bucket và quyền S3 được kiểm tra bằng những lần truy cập thực tế khi chạy pipeline.

**Nếu chạy trên máy khác chưa có Lab 1:** cài phần mềm hoặc tạo một Minikube trống chưa đủ để chạy Lab 2. Cần triển khai lại bộ Lab 1 được cấp, hoặc dùng tenant tương đương do giảng viên chuẩn bị. Bộ mã này không tự tạo kho dữ liệu, bucket hay tài khoản S3. Các tên namespace/profile đang được cấu hình cho nhóm `bd-g10`; tenant mang tên khác cần điều chỉnh cấu hình tương ứng trước khi chạy.

## 7. Cài runtime bên trong container

Mở PowerShell tại thư mục chứa `environment-setup` và hai file hướng dẫn. Có thể mở thư mục đó trong File Explorer, nhập `powershell` vào thanh địa chỉ rồi nhấn Enter. Chạy:

```powershell
if (-not (Test-Path -LiteralPath '.\environment-setup\install-runtime.ps1')) {
    throw 'Mo PowerShell tai thu muc chua environment-setup va hai file huong dan.'
}
powershell -NoProfile -ExecutionPolicy Bypass -File '.\environment-setup\install-runtime.ps1'
```

Script thực hiện ba việc: xây image `bd-g10-lab2-runtime:1.0`, kiểm tra import thư viện trong container, rồi đưa image vào Minikube `bd-g10-lab`. `ExecutionPolicy Bypass` chỉ áp dụng cho tiến trình PowerShell chạy script này.

Dockerfile cài **DuckDB 1.4.5**, **boto3 1.34.131**, **jsonschema 4.26.0** trên nền **Python 3.11**. Cần Internet để tải base image và package trong lần build đầu.

Khi thành công, dòng cuối là:

```text
Lab 2 runtime installed and loaded successfully.
```

Kiểm tra thêm nếu cần:

```powershell
docker run --rm bd-g10-lab2-runtime:1.0 python -c "import sys,importlib.metadata as m; print(sys.version); print('\n'.join(p+' '+m.version(p) for p in ['duckdb','boto3','jsonschema']))"
minikube image ls -p bd-g10-lab
```

Danh sách image trong Minikube phải có `bd-g10-lab2-runtime:1.0`. Sau bước này, thực hiện [Hướng dẫn chạy Lab 2](HUONG_DAN_CHAY_LAB2.md).

## 8. Lỗi cài đặt thường gặp

| Hiện tượng | Cách xử lý |
|---|---|
| `py -3.11` không tìm thấy Python | Cài Python 3.11 và Python Launcher, rồi mở PowerShell mới |
| `docker`/`minikube`/`kubectl` không được nhận diện | Kiểm tra PATH và mở lại PowerShell |
| Docker chỉ hiện Client, không có Server | Mở Docker Desktop và đợi Engine chạy |
| Cụm không kết nối được | Kiểm tra Docker, chạy `minikube start -p bd-g10-lab`, chọn lại context |
| Không có namespace, Secret, PVC hoặc object store | Chuẩn bị tenant Lab 1 hoặc tenant tương đương được cấp |
| Image build lỗi khi tải package | Kiểm tra Internet/proxy và thông báo lỗi, sau đó chạy lại script runtime |
