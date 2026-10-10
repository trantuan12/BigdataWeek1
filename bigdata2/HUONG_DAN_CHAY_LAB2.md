# Hướng dẫn chạy Lab 2 sau khi cài xong phần mềm

Hướng dẫn sử dụng PowerShell và bộ mã đầy đủ của nhóm `bd-g10`. Bộ bài có thể đặt tại thư mục bất kỳ; các lệnh lấy vị trí từ thư mục PowerShell hiện tại và không yêu cầu tên tài khoản hay ổ đĩa cụ thể. Trước khi bắt đầu, hoàn thành [Hướng dẫn cài phần mềm](HUONG_DAN_CAI_PHAN_MEM_LAB2.md). Các lệnh bên dưới tạo **một thư mục chạy mới**, giữ nguyên bài nộp và năm thư mục theo vai trò đã có.

## 1. Bộ thư mục cần dùng

Cấu trúc cần có tại thư mục chứa bộ bài; tên thư mục ngoài cùng có thể thay đổi:

```text
thu-muc-bo-bai\
├── HUONG_DAN_CAI_PHAN_MEM_LAB2.md
├── HUONG_DAN_CHAY_LAB2.md
├── environment-setup\
│   ├── Dockerfile
│   ├── install-runtime.ps1
│   ├── prepare_pods.py
│   ├── orchestrate.py
│   └── audit_release.py
├── Lab2-datasets\
│   └── input\
│       ├── observations_a.csv
│       ├── observations_b.jsonl
│       ├── sensors.csv
│       └── qa_reference.csv
├── trusted_manifest.json
└── Lab 2 - Data Curation, Quality & Metadata Governance\
    ├── code\
    ├── schemas\
    ├── environment\
    │   └── trusted-manifest-configmap.yaml
    ├── contract.json
    ├── team.json
    └── trusted_manifest.json
```

**Dùng folder Lab 2 đầy đủ, chưa tách, hoặc bản ghép từ đủ năm folder cá nhân.** Năm phần chứa đủ mọi file của bản đầy đủ; tài liệu/schema dùng chung được sao chép giống hệt nhau. Mỗi phần riêng lẻ không phải một pipeline đầy đủ.

### Ghép năm phần sau khi tải từ GitHub

Mỗi folder cá nhân chỉ chứa folder bài tập cùng tên. Upload phần bài tập được phân công; dùng repository riêng tư vì một số phần chứa snapshot và dữ liệu hạn chế của học phần. Không có thứ tự upload bắt buộc.

Để ghép, tạo một folder `Lab 2 - Data Curation, Quality & Metadata Governance`, rồi sao chép nội dung bên trong folder bài tập của cả năm người vào đó, giữ đúng các đường dẫn con như `code/`, `candidate/`, `individual/`, `restricted/`. Hợp nhất các thư mục con, không thay toàn bộ thư mục bằng phần của một người. Những file trùng đường dẫn trong bộ đã chia có nội dung giống hệt nhau. Cần đủ năm phần cùng phiên bản để khôi phục bản đầy đủ.

Nếu dùng Git trên Windows, đặt `git config core.autocrlf false` trong repository trước khi thêm file để giữ nguyên byte. Khi dùng chung một repository, mỗi người chỉ thêm file thuộc phần mình và giữ phần đã có của các bạn khác.

Đề yêu cầu nhóm làm song song và tích hợp ở phút 30, 60, 95, 120. Thứ tự phụ thuộc khi chạy là xác minh nguồn → đo chất lượng/curation → metadata và kiểm tra → phát hành → consumer. Thứ tự upload không thay thế các kiểm tra và bàn giao này.

Nếu chỉ có ZIP bài nộp đầy đủ, ZIP đó chưa chứa thư mục điều phối `environment-setup` ở ngoài bài tập. Cần lấy thêm thư mục này từ bộ gốc. Các file đầu vào có thể lấy từ `Lab2-datasets/input/`, hoặc từ snapshot nguyên byte `restricted/snapshot/` trong bản nộp đầy đủ, và đặt đúng cấu trúc trên. Manifest tin cậy vẫn phải là bản được cấp độc lập; không dùng convenience manifest để thay thế.

## 2. Kiểm tra Docker và Kubernetes

Mở Docker Desktop, đợi Engine chạy. Mở thư mục chứa hai file hướng dẫn và `environment-setup` trong File Explorer, nhập `powershell` vào thanh địa chỉ rồi nhấn Enter. Trong cửa sổ PowerShell đó, chạy:

```powershell
$sourceWorkspace = (Get-Location).Path
if (-not (Test-Path -LiteralPath (Join-Path $sourceWorkspace 'environment-setup\orchestrate.py'))) {
    throw 'Mo PowerShell tai thu muc chua environment-setup va hai file huong dan.'
}
$env:PYTHONUTF8 = '1'
py -3.11 --version
docker version
minikube start -p bd-g10-lab
kubectl config use-context bd-g10-lab
kubectl -n bd-g10 rollout status deployment/objects --timeout=60s
kubectl -n bd-g10 get secrets s3-ingestor s3-owner s3-analyst
minikube image ls -p bd-g10-lab
```

Phải có object store sẵn sàng, đủ ba Secret và image `bd-g10-lab2-runtime:1.0` trong Minikube. Nếu thiếu image, chạy script cài runtime theo file hướng dẫn cài phần mềm. Biến `PYTHONUTF8` giúp Python đọc/ghi tên tiếng Việt ổn định trên Windows.

Không chạy pipeline đồng thời với một phiên khác sử dụng cùng ba Pod `curate`, `publisher`, `analyst`.

## 3. Tạo thư mục chạy mới

Chạy khối lệnh sau trong cùng cửa sổ PowerShell đã dùng ở mục 2. Biến `$sourceWorkspace` lưu vị trí bộ bài trước khi chuyển sang thư mục chạy mới, nên vẫn dùng được khi chạy lại mục 3 trong phiên này:

```powershell
if (-not $sourceWorkspace) {
    throw 'Chay muc 2 truoc de xac dinh thu muc bo bai.'
}
$labName = 'Lab 2 - Data Curation, Quality & Metadata Governance'
$sourceLab = Join-Path $sourceWorkspace $labName

$requiredPaths = @(
    (Join-Path $sourceWorkspace 'environment-setup\orchestrate.py'),
    (Join-Path $sourceWorkspace 'Lab2-datasets\input\observations_a.csv'),
    (Join-Path $sourceWorkspace 'Lab2-datasets\input\observations_b.jsonl'),
    (Join-Path $sourceWorkspace 'Lab2-datasets\input\sensors.csv'),
    (Join-Path $sourceWorkspace 'Lab2-datasets\input\qa_reference.csv'),
    (Join-Path $sourceWorkspace 'trusted_manifest.json'),
    (Join-Path $sourceLab 'code\driver.py'),
    (Join-Path $sourceLab 'environment\trusted-manifest-configmap.yaml')
)
foreach ($requiredPath in $requiredPaths) {
    if (-not (Test-Path -LiteralPath $requiredPath)) {
        throw "Thieu file: $requiredPath"
    }
}

$runWorkspace = Join-Path $sourceWorkspace ('runs\run-' + (Get-Date -Format 'yyyyMMdd-HHmmss-fff'))
if (Test-Path -LiteralPath $runWorkspace) { throw 'Thu muc chay da ton tai.' }
$runLab = Join-Path $runWorkspace $labName
New-Item -ItemType Directory -Path $runLab -Force | Out-Null

Copy-Item -LiteralPath (Join-Path $sourceWorkspace 'environment-setup') -Destination (Join-Path $runWorkspace 'environment-setup') -Recurse
Copy-Item -LiteralPath (Join-Path $sourceWorkspace 'Lab2-datasets') -Destination (Join-Path $runWorkspace 'Lab2-datasets') -Recurse
Copy-Item -LiteralPath (Join-Path $sourceWorkspace 'trusted_manifest.json') -Destination (Join-Path $runWorkspace 'trusted_manifest.json')

foreach ($part in @('code','schemas','environment')) {
    Copy-Item -LiteralPath (Join-Path $sourceLab $part) -Destination (Join-Path $runLab $part) -Recurse
}
foreach ($file in @('contract.json','team.json','trusted_manifest.json')) {
    Copy-Item -LiteralPath (Join-Path $sourceLab $file) -Destination (Join-Path $runLab $file)
}
New-Item -ItemType Directory -Path (Join-Path $runLab 'evidence') -Force | Out-Null

Set-Location -LiteralPath $runWorkspace
Write-Host "Thu muc chay: $runWorkspace"
```

Khối lệnh chỉ sao chép đầu vào, mã, schema và cấu hình cần thiết. Nó không đưa `lineage.jsonl`, candidate, probe hay bằng chứng của lần chạy cũ vào workspace mới. Điều này cần thiết vì driver yêu cầu một scratch mới cho mỗi run.

## 4. Tạo lại ba Pod với scratch sạch

Trước khi thay Pod, kết quả của phiên trước cần được lưu trên máy; scratch `/work` là `emptyDir`, sẽ mất khi Pod bị xóa. Bản nộp đã hoàn thành hiện nằm trên Windows và được giữ nguyên.

Trong thư mục chạy mới, thực hiện:

```powershell
kubectl -n bd-g10 delete pod curate publisher analyst ingestor owner blocked --ignore-not-found=true --wait=true --timeout=60s
if ($LASTEXITCODE -ne 0) { throw 'Chua thay duoc cac Pod khach.' }

py -3.11 .\environment-setup\prepare_pods.py
if ($LASTEXITCODE -ne 0) { throw 'Chuan bi Pod that bai.' }

kubectl -n bd-g10 get pods -o wide
```

Lệnh xóa chỉ áp dụng cho các Pod khách/role/probe được nêu tên. Deployment `objects`, PVC, namespace, bucket, quota và NetworkPolicy được giữ nguyên.

`prepare_pods.py` tạo ConfigMap manifest tin cậy, ghi cấu hình image digest hiện tại và tạo ba Pod:

| Pod | Secret được mount | Vai trò |
|---|---|---|
| `curate` | `s3-ingestor` | Thu nhận, SQL curation và staging |
| `publisher` | `s3-owner` | Kiểm chứng lại và phát hành |
| `analyst` | `s3-analyst` | Đọc, kiểm tra bản phát hành |

Khi sẵn sàng, script in `READY: three separate credential contexts; original object store retained`. Các Pod phải ở trạng thái **Running**, **READY 1/1**.

## 5. Chạy thu nhận và curation

```powershell
py -3.11 .\environment-setup\orchestrate.py curate
if ($LASTEXITCODE -ne 0) { throw 'Curation that bai; xem log truoc khi tiep tuc.' }
```

Giai đoạn này kiểm tra đầu vào local theo trusted manifest, đưa bốn input lên raw nếu chưa có, tải chúng qua S3 trong curator context và kiểm tra lại số byte/SHA-256. Sau đó pipeline chuẩn hóa, chọn winner, cách ly lỗi, ghi duplicate ledger, tính chất lượng, thử replay/counterexample, tạo metadata/lineage và tải staging lên raw.

Với fixture đã cấp, kết quả thành công có các giá trị:

```text
status: COMPLETE
raw_envelopes: 10205
curated: 9610
quarantine: 395
duplicates: 200
```

Run UUID và staging prefix thay đổi mỗi lần. Script tải kết quả từ Pod về `$runLab`. Chờ đến khi lệnh kết thúc; không chạy publish khi curate còn đang chạy hoặc đã trả lỗi.

## 6. Chạy gate và phát hành

```powershell
py -3.11 .\environment-setup\orchestrate.py publish
if ($LASTEXITCODE -ne 0) { throw 'Gate/publish that bai; khong coi la da phat hanh.' }
```

Publisher tải và xác minh staging, kiểm tra lại bốn nguồn, tính lại G01–G09, chạy tám ca mutation và các kiểm tra chéo. Kỳ vọng:

| Ca thử | Kết quả |
|---|---|
| P01 — candidate gốc | PASS |
| P02 — nhiệt độ 100 °C | REJECT |
| P03 — nhiệt độ NULL | REJECT |
| P04 — thêm duplicate | REJECT |
| P05 — bỏ metadata steward | REJECT |
| P06 — đổi byte bản sao nguồn | REJECT |
| P07 — thiếu output lineage | REJECT |
| P08 — bỏ QA ID bất đồng | REJECT |

Khi tất cả kiểm chứng cần thiết đạt, script tạo release prefix mới, xác minh artifact tải lên rồi ghi `release_manifest.json` cuối cùng. Dòng cuối có `status: COMPLETE`, `release_id`, `manifest_key`, `manifest_sha256`.

Review trong implementation hiện tại là **automated owner-context technical review**; record ghi rõ `human_signature_attested: false`. Kết quả này không phải chữ ký duyệt độc lập hay xác nhận demo trực tiếp của một thành viên.

## 7. Kiểm tra bằng tài khoản analyst và audit các ca bị từ chối

```powershell
py -3.11 .\environment-setup\orchestrate.py consume
if ($LASTEXITCODE -ne 0) { throw 'Consumer check that bai.' }

py -3.11 .\environment-setup\audit_release.py
if ($LASTEXITCODE -ne 0) { throw 'Release audit that bai.' }
```

Consumer tải final manifest cùng năm artifact, kiểm tra SHA-256, schema và số dòng. Nó thực hiện raw GET và release PUT thật để xác nhận analyst bị từ chối quyền không được cấp.

Kết quả kỳ vọng của consumer:

```text
status: PASS
rows: 9610
raw_get_http: 403
```

Audit kiểm tra bảy probe UUID bị từ chối không được tham chiếu bởi một approved manifest và inventory release đúng danh sách artifact. Kết quả cuối phải là `PASS`.

## 8. Xem kết quả của lần chạy mới

Trong cùng cửa sổ PowerShell, biến `$runLab` vẫn trỏ tới thư mục kết quả vừa tạo:

```powershell
Get-Content -LiteralPath (Join-Path $runLab 'evidence\conservation.json') -Raw -Encoding UTF8
Get-Content -LiteralPath (Join-Path $runLab 'quality_after.json') -Raw -Encoding UTF8
Get-Content -LiteralPath (Join-Path $runLab 'consumer-check.json') -Raw -Encoding UTF8
```

| File/thư mục | Nội dung |
|---|---|
| `candidate/curated.parquet` | 9.610 quan sát, schema mười cột |
| `candidate/artifact-manifest.json` | Count, byte size và SHA-256 đầu ra |
| `restricted/snapshot/` | Bốn nguồn nguyên byte |
| `restricted/quarantine.csv` | 395 dòng bị cách ly và các lý do |
| `restricted/duplicates.csv` | 200 phiên bản SUPERSEDED |
| `quality_before.json`, `quality_after.json` | Chỉ số với tử số/mẫu số và cohort |
| `catalog.json`, `retention-plan.json` | Metadata và quyết định retention dry-run |
| `lineage.jsonl`, `run-record.json`, `publish-run-record.json` | Events và run provenance thực tế |
| `gate-tests.json` | Tám ca thử và lỗi được đo |
| `release_manifest.json` | Danh sách artifact của release mới |
| `consumer-check.json` | Checksum/schema/count và access denial |
| `evidence/rejected-probes-no-release.json` | Kiểm tra không phát hành các probe bị từ chối |
| `evidence/execution-*.log` | Log của các giai đoạn điều phối |

Tỷ lệ kịp thời là **9.410/9.610**, có **200 dòng late được giữ**. QA coverage **100/100**, QA agreement **95/100** trong dung sai 0,05 °C. Đây là kết quả tham chiếu cho đúng fixture và contract hiện có; đổi mã/contract cần được xem là một phiên bản chạy mới.

Các báo cáo Markdown và ZIP của bài nộp gốc mô tả lần chạy đã nộp trước đó. Các lệnh ở đây tạo **kết quả runtime mới**; không tự cập nhật báo cáo, chia lại folder cá nhân hoặc thay ZIP gốc. Đầu vào, quarantine, duplicate ledger và raw payload thuộc phạm vi hạn chế; các phần dùng để ghép bản đầy đủ có chứa những dữ liệu này và cần được lưu trong repository riêng tư.

## 9. Chạy lại hoặc xử lý lỗi

Để chạy lại toàn bộ, bắt đầu lại **mục 3** để tạo workspace mới, rồi tạo lại các Pod khách và thực hiện curate → publish → consume → audit. Không chạy lại trực tiếp `driver.py` trên `/work` của một run đã hoàn thành. Không gọi publish lần hai trên scratch đã có probe fixtures, hoặc consume lần hai trên scratch đã có thư mục download.

| Lỗi | Nguyên nhân và cách xử lý |
|---|---|
| `Fresh work directory required for each run` | Scratch đang chứa lineage cũ; tạo workspace và Pod mới |
| `FileExistsError` ở snapshot/probe/download | Đã lặp giai đoạn trong scratch cũ; tạo phiên chạy mới |
| `ModuleNotFoundError: duckdb`/`jsonschema` trong Pod | Image thiếu thư viện; build/load lại runtime và tạo lại Pod |
| `ErrImageNeverPull` | Image chưa được load đúng profile Minikube |
| `CreateContainerConfigError` | Kiểm tra Secret, ConfigMap và `kubectl describe pod` |
| Pod `Pending` do quota | Kiểm tra các Pod khách cũ và tài nguyên; không tăng quota để bỏ qua hợp đồng lab |
| `INPUT_INTEGRITY` | File nguồn không đúng byte/hash; khôi phục fixture đã cấp, không sửa trusted hash cho khớp |
| Timeout tới `objects:8333` | Kiểm tra readiness Deployment/store, Service và NetworkPolicy |
| `OWNER_GATE_REJECTED` | Đọc `evidence/owner-gate.json` hoặc log publish để biết check bị lỗi; chưa được xem là đã phát hành |
| `AccessDenied` ở thao tác cần được phép | Kiểm tra đúng Secret và chính sách S3 của Lab 1 |

Lệnh chẩn đoán không hiển thị giá trị Secret:

```powershell
kubectl -n bd-g10 describe pod curate
kubectl -n bd-g10 describe pod publisher
kubectl -n bd-g10 describe pod analyst
kubectl -n bd-g10 get events --sort-by=.lastTimestamp
kubectl -n bd-g10 get resourcequota team-budget
```

`AccessDenied` của curator release PUT và analyst raw GET/release PUT trong bài thử là **kết quả mong đợi**, không phải lỗi cần mở rộng quyền.
