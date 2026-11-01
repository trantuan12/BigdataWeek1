# BÁO CÁO CÁ NHÂN — INDIVIDUAL ASSESSMENT

**Học phần:** Cloud-Native Big Data Infrastructure & Governance  
**Bài thực hành:** Lab 3 — Lakehouse Processing & Analytics  
**Package Revision:** 18 September 2026

| Thông tin | Nội dung |
|---|---|
| Họ và tên | Lê Toàn |
| MSSV | 24022466 |
| Vai trò | Role E — Reproducibility Reviewer |
| Nhiệm vụ chính | Task 5 — validate_release_binding, catalog, lineage & release review |
| Nhiệm vụ chéo | Cross-check A: Verify trusted input and logical handoff identity |
| Nhóm thực hiện | `bd-g10` |
| Run Label | `roleD-20261101` |
| Context / Chế độ chạy | Spark standalone: hai worker container, S3A dùng chung (Spark 4.0.1, Delta-Spark 4.0.1, Python 3.11.18, Java 17.0.21, Hadoop 3.4.1) |
| Trusted Manifest SHA-256 | `3cdfc84b26d290100b39ce65d4ff7cb22839f4021b9caf26897c8a4ff68091a2` |
| Upstream Logical SHA-256 | `c50f8252bfb91890975d26b16fe1d2b32f0e283a58e46bd16260b4a3cca0bf0d` |
| Release ID | `lab3-bd-g10-roleD-20261101` |
| Cặp snapshot kiểm tra | Silver v1 / Gold v1; Gold lấy nguồn Silver v1 |
| Run ID Review | `f7637837-296c-45b5-94cc-2aa9d27a3a5a` |

---

## Phần 1: Đóng góp kỹ thuật (10 điểm)

### 1.1 Trước khi thực hiện (Bối cảnh và thách thức)

Sau khi Bronze, Silver và Gold được tạo và cập nhật, nhóm cần một release candidate mà người khác có thể kiểm tra lại. Một job báo thành công hoặc ba bảng đang tồn tại chưa chứng minh các giá trị Gold được tạo từ đúng snapshot Silver, hoặc metadata đang mô tả đúng mã và đầu vào đã thực thi.

1. **Ràng buộc nguồn và mã (Handoff & Code Binding):** Candidate phải giữ đúng handoff kind, digest logic, trusted manifest hash và SHA-256 của implementation đã tạo kết quả. Không lấy một hash mới tùy ý để thay mã đã chạy.
2. **Ràng buộc phiên bản (Snapshot Binding):** Ghép đúng Silver path/version, Gold path/version và `gold_source_silver_version`. Không dùng latest để thay phiên bản đã chọn và không trộn Gold của một snapshot với Silver khác.
3. **Kiểm tra từ dữ liệu thực (Actual-data Review):** Đọc các bảng Delta bằng versionAsOf, tính lại toàn bộ trường Gold rồi so với bảng và JSON candidate. Count khớp một mình chưa đủ.
4. **Phạm vi phê duyệt và metadata (Accountability & Governance):** Author, reviewer và steward phải có giá trị; reviewer khác author. Catalog, lineage và manifest mô tả dữ liệu và phụ thuộc, không thay chữ ký, IAM hoặc giao dịch xuyên bảng.

Vai trò của tôi (Lê Toàn — Thành viên E) là phụ trách kiểm tra binding, catalog và review kỹ thuật của candidate. Phần kiểm tra chéo tập trung vào tính toàn vẹn đầu vào và danh tính handoff do Thành viên A tiếp nhận.

---

### 1.2 Công việc kỹ thuật đã thực hiện

#### 1. Cài đặt và kiểm tra validate_release_binding

- Hàm nằm trong [`templates/student_impl.py`](../templates/student_impl.py), trả `(passed, reasons)` và không ghi bảng.
- Author, reviewer và steward phải là chuỗi không rỗng; self-review bị từ chối sau khi chuẩn hóa khoảng trắng và hoa/thường.
- Bảy trường cần ghim là `handoff_logical_sha256`, `code_sha256`, `silver_path`, `silver_version`, `gold_path`, `gold_version`, `gold_source_silver_version`. Các version phải là số nguyên không âm, không nhận BOOLEAN hoặc chuỗi số.
- Các mã từ chối gồm `REVIEW`, `METADATA`, `VERSION_BINDING`, `CODE_BINDING`, `HANDOFF_BINDING`. Thiếu khóa phải từ chối sạch, không gây accidental pass.

```python
def validate_release_binding(draft, expected):
    """T5. Return (passed: bool, reasons: list[str]) for a release binding.
    Require nonempty author, reviewer, steward and reviewer != author.
    Bind handoff_logical_sha256, code_sha256, silver_path, silver_version,
    gold_path, gold_version and gold_source_silver_version to expected.
    Also require draft.gold_source_silver_version == draft.silver_version.
    Rejection reasons: REVIEW, METADATA, VERSION_BINDING, CODE_BINDING,
    or HANDOFF_BINDING. Missing keys must reject, never crash/pass.
    This is a course release check, not a digital signature or IAM control.
    """
    import re

    reasons = []
    if not isinstance(draft, dict) or not isinstance(expected, dict):
        return False, ['METADATA', 'VERSION_BINDING', 'CODE_BINDING', 'HANDOFF_BINDING']

    identities = [draft.get(k) for k in ('author', 'reviewer', 'steward')]
    if not all(isinstance(v, str) and v.strip() for v in identities):
        reasons.append('METADATA')
    author, reviewer = identities[:2]
    if (isinstance(author, str) and isinstance(reviewer, str)
            and author.strip().casefold() == reviewer.strip().casefold()):
        reasons.append('REVIEW')

    def matches_hash(key):
        value, wanted = draft.get(key), expected.get(key)
        return (isinstance(value, str)
                and re.fullmatch(r'[a-f0-9]{64}', value) is not None
                and value == wanted)

    if not matches_hash('handoff_logical_sha256'):
        reasons.append('HANDOFF_BINDING')
    if not matches_hash('code_sha256'):
        reasons.append('CODE_BINDING')

    version_ok = True
    for key in ('silver_path', 'gold_path'):
        value = draft.get(key)
        if not isinstance(value, str) or not value.strip() or value != expected.get(key):
            version_ok = False
    for key in ('silver_version', 'gold_version', 'gold_source_silver_version'):
        value, wanted = draft.get(key), expected.get(key)
        if (type(value) is not int or value < 0 or type(wanted) is not int
                or value != wanted):
            version_ok = False
    if draft.get('gold_source_silver_version') != draft.get('silver_version'):
        version_ok = False
    if not version_ok:
        reasons.append('VERSION_BINDING')
    return not reasons, reasons
```

Trong review E, giá trị `expected` được dựng từ `state.json`, cấu hình đường dẫn, manifest cấp riêng và mã đã lưu, sau đó đối chiếu với dữ liệu thật. Không dựng expected bằng cách sao chép các giá trị đang cần kiểm chứng từ chính draft.

#### 2. Kiểm tra năm gate bắt buộc và các trường hợp thiếu metadata

| Ca thử | Kỳ vọng candidate | Quan sát | Lý do | Kết quả test |
| --- | --- | --- | --- | --- |
| R01_valid | Chấp nhận | Chấp nhận | Không có lỗi | PASS |
| R02_mixed_version | Từ chối | Từ chối | VERSION_BINDING | PASS |
| R03_self_review | Từ chối | Từ chối | REVIEW | PASS |
| R04_missing_steward | Từ chối | Từ chối | METADATA | PASS |
| R05_wrong_code | Từ chối | Từ chối | CODE_BINDING | PASS |


`PASS` trong cột cuối nghĩa là phép thử cho kết quả đúng kỳ vọng. Vì vậy một candidate sai bị từ chối là test PASS, không phải candidate đã được chấp nhận.

Ngoài năm ca bắt buộc, chương trình thực hiện **35 ca bổ sung**: thiếu từng khóa draft, thiếu từng khóa expected, version sai kiểu/âm, đường dẫn khác, sai digest handoff, self-review có thay đổi hoa/thường và khoảng trắng, cặp expected cũng bị trộn và draft sai kiểu. Tất cả được kiểm tra không làm thay đổi các đối số đầu vào.

#### 3. Ghim bảng, mã và kiểm tra Gold từ snapshot thực

| Trường binding | Giá trị đã kiểm tra |
| --- | --- |
| silver_path | `s3a://research-lakehouse/lab3/bd-g10/roleD-20261101/tables/silver` |
| silver_version | `1` |
| gold_path | `s3a://research-lakehouse/lab3/bd-g10/roleD-20261101/tables/gold` |
| gold_version | `1` |
| gold_source_silver_version | `1` |


Các kiểm tra được thực hiện trên:

- Silver v1: **9.710** quan sát, đọc bằng `versionAsOf`.
- Gold v1: **20** nhóm site/ngày, lấy nguồn Silver v1.
- `gold.json` của candidate: so toàn bộ **11 trường** trong mỗi nhóm với Gold thực.

Phép tính lại độc lập dùng Decimal, count và tập sensor ID từ các hàng Silver đã đọc, không gọi lại `build_gold`. Nó kiểm tra count, sum, mean, min, max, n_late, late_rate, n_above_25 và n_sensors cùng khóa site/ngày. Fixture nằm dưới giới hạn 25.000 dòng; ngoại lệ thu về driver phục vụ kiểm chứng này không áp dụng cho benchmark mở rộng hoặc pipeline production.

SHA-256 implementation tạo kết quả: `739d95ff27e0e85e8549b46d4b66d2c0f790bfab6873aa62dea490fa3f2b19e1`. SHA-256 module validator: `739d95ff27e0e85e8549b46d4b66d2c0f790bfab6873aa62dea490fa3f2b19e1`. Trong lượt này hai giá trị bằng nhau vì năm hàm nằm trong cùng module; bản mã đã thực thi được giữ theo hash trong `evidence/code_versions/`.

Trạng thái mới nhất của Silver có commit replay v2, nhưng candidate vẫn ghim Silver v1 là nguồn thực sự tạo Gold. Nội dung replay bằng nhau không cho phép âm thầm thay version trong binding.

#### 4. Kiểm tra catalog và hash các object candidate

| Dataset | Version | Rows đo thực | Phân loại |
| --- | --- | --- | --- |
| silver_observations | 1 | 9710 | RESTRICTED_WORKING_TABLE |
| gold_site_day | 1 | 20 | APPROVED_ONLY_AFTER_OWNER_REVIEW |


Catalog được đối chiếu với **schema JSON đã được trusted manifest bảo vệ** và schema thực của hai bảng. Ý nghĩa cột, kiểu dữ liệu, đơn vị Celsius, UTC và điều kiện ngưỡng/late phải thống nhất với hợp đồng.

Allowed use: `Synthetic teaching and assessment only`. Catalog ghi retention **90 ngày kể từ thời điểm công bố thực tế**, không tính từ AS_OF mô phỏng. Đồng thời phải giữ các Delta log/data file còn được snapshot đã ghim tham chiếu; không VACUUM trong phạm vi bài.

| Object | Bytes | SHA-256 |
| --- | --- | --- |
| gold.json | 6421 | 095c9b84ddcb2426f2a5cbbd7d46b0a35c579c7c609daba6021a7295cf5be8b4 |
| snapshot_binding.json | 1102 | 90743b2a2baea42fa01586a988f9a924b87252571d41c4f324250806f915509b |
| catalog.json | 5096 | 0599d6eec22856e8beff26c869101709717fb3433d07f8a77c568dd038778538 |


`RESTRICTED_WORKING_TABLE` và `APPROVED_ONLY_AFTER_OWNER_REVIEW` là nhãn mô tả. Chúng không tự tạo quyền truy cập. Candidate và hồ sơ không chứa payload raw hoặc thông tin đăng nhập.

#### 5. Kiểm tra lineage và lập biên bản review

Lineage pipeline có **14 sự kiện**, tạo **7 cặp START/terminal** của preflight, bootstrap, incremental, analytics, benchmark và release. Chương trình xác thực bằng schema OpenLineage 2-0-2 đã cache, rồi kiểm tra run/job identity, thứ tự thời gian, đủ các bước và hai input cụ thể của release: Silver v1 và Gold v1.

Review E có thêm cặp START/COMPLETE riêng trong `role_E_review_lineage.jsonl`; không ghi đè lịch sử của sáu bước pipeline. Catalog và lineage mô tả phụ thuộc, không phải các khối dữ liệu trung gian. Truy vết đến source object/ordinal/hash vẫn cần các trường provenance và nguồn đã kiểm tra ở phần A.

Biên bản [`release_review.json`](../evidence/release_review.json) ghi người được phân công reviewer, run ID, thời điểm kiểm tra, binding, kết quả gate và lý do đánh giá. Trạng thái là **TECHNICAL_CANDIDATE_REVIEW_PASSED**; candidate vẫn là **CANDIDATE_NOT_OWNER_PUBLISHED**. Đây là biên bản kiểm tra bằng chương trình, không phải chữ ký cá nhân hoặc owner publication.

---

### 1.3 Kết quả kiểm định các tiêu chí Task 5

| Tiêu chí | Nội dung kiểm tra | Điều kiện bắt buộc | Kết quả thực tế | Trạng thái |
|:---:|---|---|---|:---:|
| R01–R05 | Năm gate | Valid nhận; mixed snapshot, self-review, thiếu steward, sai code bị từ chối | 5/5 ca đúng kỳ vọng | PASS |
| Binding | Snapshot và mã | Path/version/digest đúng state và bản đã chạy | Silver v1 / Gold v1; code hash khớp | PASS |
| Values | Giá trị Gold | Đọc bảng đã ghim và so đủ trường, không chỉ count | 20 nhóm × 11 trường khớp phép tính độc lập và JSON candidate | PASS |
| Metadata | Catalog và manifest | Count, schema, đơn vị, allowed use, retention và object hash đúng | Hai dataset và ba object candidate khớp | PASS |
| Lineage | Trạng thái và I/O | Schema hợp lệ, cặp run đúng và ghim nguồn release | 14 sự kiện pipeline và cặp review riêng | PASS |
| E→A | Kiểm tra chéo A | Đúng 16 input, handoff kind/digest và Silver baseline | 16/16 input và 9.610 dòng v0 khớp | PASS |

**Các tệp bằng chứng kỹ thuật nộp kèm:**

- [`role_E_prediction.json`](../evidence/role_E_prediction.json), [`role_E_check.json`](../evidence/role_E_check.json) — Dự đoán và 16 kiểm tra E.
- [`role_E_gate_tests.json`](../evidence/role_E_gate_tests.json), [`role_E_edge_cases.json`](../evidence/role_E_edge_cases.json) — Năm ca bắt buộc và 35 ca bổ sung.
- [`snapshot_binding.json`](../evidence/release_candidate/snapshot_binding.json), [`catalog.json`](../evidence/release_candidate/catalog.json), [`release_manifest.json`](../evidence/release_candidate/release_manifest.json) — Candidate và binding.
- [`role_E_catalog_check.json`](../evidence/role_E_catalog_check.json), [`role_E_lineage_check.json`](../evidence/role_E_lineage_check.json) — Đối chiếu metadata và lineage.
- [`release_review.json`](../evidence/release_review.json), [`role_E_review_lineage.jsonl`](../evidence/role_E_review_lineage.jsonl) — Biên bản và sự kiện review thực.
- [`runtime_role_E_check.json`](../evidence/runtime_role_E_check.json), [`implementation_tests.json`](../evidence/implementation_tests.json) — Runtime và bộ 20 test.

---

## Phần 2: Kiểm tra chéo độc lập (10 điểm)

### Thao tác được chỉ định: Verify Member A's trusted input and handoff identity
*(Kiểm tra tính toàn vẹn đầu vào và danh tính handoff do Thành viên A tiếp nhận)*

#### 2.1 Mục tiêu kiểm tra

Thành viên A (Data Ingress — Trần Anh Tuấn, MSSV 24022484) chịu trách nhiệm tiếp nhận dữ liệu, trust evidence và runtime. Kiểm tra chéo E cần tự đọc lại byte nguồn và snapshot baseline, thay vì chỉ đọc chữ PASS trong báo cáo A.

Manifest cấp riêng được mount read-only vào container. So SHA-256 chứng minh khớp bản manifest đã cấp; tính xác thực của kênh cấp bản vẫn thuộc trách nhiệm phân phối của giảng viên. Bản convenience trong package không tự xác thực nguồn tin cậy.

#### 2.2 Kịch bản và Dự đoán

1. **16 artifact đầu vào:** Mỗi file đúng số byte và SHA-256 trong manifest cấp riêng. Hash manifest phải khớp state và metadata candidate.
2. **Handoff:** CSV có **9.610** dòng, đúng mười cột; mode là `course_fallback_not_student_approval`, không phải Parquet release do nhóm đã phê duyệt.
3. **Danh tính logic:** Digest CSV và digest đọc từ Silver v0 phải cùng bằng `c50f8252bfb91890975d26b16fe1d2b32f0e283a58e46bd16260b4a3cca0bf0d`; candidate phải tiếp tục ghim danh tính đó.
4. **Refusal demo:** Mixed version và self-review bị từ chối dù các field còn lại hợp lệ; số version và code hash của bản được nhận phải xác định được bằng chứng cụ thể.

Dự đoán được ghi lúc `2026-11-01T18:19:29.263781+00:00`, trước khi thực hiện các assertion.

#### 2.3 Thực thi kiểm chứng độc lập

Chạy tại thư mục có `compose.yaml` (`03_DU_AN` trong bộ đã quy hoạch):

```powershell
docker compose exec -T driver python tools/check_role_e.py --config config/team.json --trusted-manifest /instructor/trusted_lab3_inputs.json
```

Ví dụ sau tái lập hai ca từ chối bằng **expected lấy từ state**, không sửa candidate thật:

```python
import sys
from copy import deepcopy
from pathlib import Path
sys.path[:0] = ['src', 'templates', 'tools']
from common import read_json
from student_impl import validate_release_binding
from check_role_e import expected_binding

config = read_json('config/team.json')
ev = Path(config['evidence_dir'])
state = read_json(ev / 'state.json')
draft = read_json(ev / 'release_candidate/snapshot_binding.json')
expected = expected_binding(state)
assert validate_release_binding(draft, expected) == (True, [])

mixed = deepcopy(draft)
mixed['gold_source_silver_version'] = state['silver_initial_version']
passed, reasons = validate_release_binding(mixed, expected)
assert not passed and 'VERSION_BINDING' in reasons
print('Mixed snapshot:', passed, reasons)

self_review = deepcopy(draft)
self_review['reviewer'] = self_review['author']
passed, reasons = validate_release_binding(self_review, expected)
assert not passed and 'REVIEW' in reasons
print('Self-review:', passed, reasons)
```

#### 2.4 Kết quả quan sát và Đối chiếu

| Kiểm tra A | Giá trị quan sát | Kết quả |
| --- | --- | --- |
| E_cross_A_trusted_inputs | artifacts=16; trusted_manifest_sha256=3cdfc84b26d290100b39ce65d4ff7cb22839f4021b9caf26897c8a4ff68091a2 | PASS |
| E_cross_A_logical_handoff | rows=9610; handoff_kind=course_fallback_not_student_approval; logical_sha256=c50f8252bfb91890975d26b16fe1d2b32f0e283a58e46bd16260b4a3cca0bf0d | PASS |
| E_cross_A_actual_baseline | silver_initial_version=0; rows=9610; logical_sha256=c50f8252bfb91890975d26b16fe1d2b32f0e283a58e46bd16260b4a3cca0bf0d | PASS |


Toàn bộ review hoàn tất lúc `2026-11-01T18:20:00.502954+00:00`, đạt **16/16 kiểm tra**. Silver/Gold latest version trước và sau không đổi; số thao tác ghi Delta của review là **0**. Kết quả chi tiết gồm byte/hash input, digest logic của handoff, digest của baseline thực và phép tính lại Gold.

#### 2.5 Kết luận kiểm tra chéo

Đối với lượt này, input và baseline khớp danh tính A đã công bố, mode fallback được ghi đúng và candidate giữ nguyên nguồn gốc đã kiểm tra. Không có việc coi CSV fallback là bằng chứng nhóm đã có một Lab 2 Parquet release được phê duyệt.

Khi trình bày hai phút: chỉ trusted manifest và digest; mở binding để nêu Silver/Gold version cùng code hash; chạy ví dụ từ chối mixed snapshot/self-review; kết thúc bằng việc phân biệt candidate đã review kỹ thuật với owner publication. Log chương trình không thay cho phần thực hành và giải thích của thành viên.

---

## Phần 3: Trả lời câu hỏi phỏng vấn thoát (10 điểm)

### Câu 1: Tại sao một thao tác merge không thay đổi dữ liệu (no-op merge) vẫn có thể ghi thêm một phiên bản commit mới vào lịch sử Delta mà không vi phạm tính lũy đẳng (idempotence)?

**Trả lời:**  
Version giao dịch và trạng thái logic là hai khái niệm khác nhau. Silver v1 và replay v2 có cùng nội dung và provenance nên replay vẫn lũy đẳng dù history có thêm commit. Release giữ nguồn Silver v1 thực sự tạo Gold; không tự thay binding bằng replay version. Kiểm tra version là kiểm tra danh tính snapshot cụ thể, còn kiểm tra digest là kiểm tra nội dung logic; không khẳng định mọi no-op đều phải tăng version.

---

### Câu 2: Tại sao một kiểm tra miền giá trị hợp lệ (passing range check) không đồng nghĩa với việc xác lập tính chính xác của phép đo (measurement accuracy)?

**Trả lời:**  
Range check chỉ xác nhận thỏa hợp đồng miền giá trị. Một cảm biến có thể cho giá trị hợp lệ nhưng vẫn lệch mẫu QA hoặc tham chiếu hiệu chuẩn. Review và hash bảo đảm khớp byte/nội dung đã ghim, không biến một phép đo lệch thành phép đo đúng. Vì vậy catalog giữ ý nghĩa “released temperature, not QA-adjusted” và các giới hạn chất lượng; không dùng QA để thay giá trị quan sát khi dựng release.

---

### Câu 3: Tại sao tập dữ liệu benchmark nhân bản tổng hợp (replica-expanded benchmark) bắt buộc phải nằm ngoài các bảng Gold vận hành (operational Gold)?

**Trả lời:**  
971.000 dòng benchmark là các bản sao của 9.710 khóa, không phải quan sát độc lập mới. Nếu phát hành chúng như dữ liệu vận hành thì row count, provenance và ý nghĩa dataset trong catalog sẽ sai dù một vài tỷ số không đổi. Candidate hiện mô tả 9.710 quan sát và 20 nhóm site/ngày; các bảng A/B phục vụ đo bố trí và nằm riêng ngoài operational Gold.

---

### Câu 4: Tại sao một manifest phát hành (release manifest) không phải là một giao dịch đa bảng/đa đối tượng (cross-table transaction)?

**Trả lời:**  
Manifest ràng buộc path, version và hash sau khi các lần ghi riêng hoàn tất. Nó không atomic commit hoặc rollback cho Bronze, Silver, Gold và các object xuất. Manifest cũng không phải chữ ký, IAM policy hoặc backup. Biên bản E ghi rõ candidate và kết quả kỹ thuật; owner publication là bước riêng, cần ngữ cảnh owner, kiểm tra độc lập, prefix mới và manifest-last theo quy trình của bài.

---

### Câu 5: Tại sao việc một hệ thống lưu trữ tương thích giao diện S3 API thô (raw S3 API) vẫn chưa đủ để khẳng định tính tương thích với Delta Lake?

**Trả lời:**  
Một API object có thể đọc/ghi được nhưng chưa chứng minh hành vi commit/visibility phù hợp với LogStore và chế độ writer mà Delta sử dụng. Cần smoke test và các kiểm tra bảng trên backend thực. Bằng chứng trong lượt này giới hạn ở cấu hình đã thử và single writer với schema rejection, MERGE, replay, time travel; không suy thành kiểm thử mọi backend S3 hoặc nhiều driver ghi đồng thời.
