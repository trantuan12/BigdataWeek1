"""Run Task 2 from the supplied inputs, without external task dependencies."""
import argparse
import importlib.metadata
import json
import os
import platform
import sys
import uuid
from pathlib import Path

import duckdb
from jsonschema import Draft202012Validator, FormatChecker

TASK2 = Path(__file__).resolve().parents[1]
from prepare_inputs import prepare, now, save_json, sha256


def rows(con, sql):
    cursor = con.execute(sql)
    names = [x[0] for x in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def load_policy(con, contract):
    # The SQL predicates read parameters from the frozen contract, never QA answers.
    con.execute("CREATE TABLE policy AS SELECT ?::TIMESTAMP AS as_of, ?::DOUBLE AS min_c, "
                "?::DOUBLE AS max_c, ?::BIGINT AS late_seconds, ?::VARCHAR AS id_pattern, "
                "?::VARCHAR[] AS supported_units, ?::DOUBLE AS qa_tolerance",
                [contract["as_of"], contract["temperature_range_c"]["min"],
                 contract["temperature_range_c"]["max"], contract["late_threshold_seconds"],
                 contract["record_id_pattern"], contract["supported_units"],
                 contract["qa_policy"]["absolute_tolerance_c"]])


def chart(output, records):
    os.environ.setdefault("MPLCONFIGDIR", str(output / "restricted" / "matplotlib"))
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    names = ["Q_REQUIRED", "Q_UNIQUE", "Q_VALUE", "Q_SENSOR", "Q_TIME", "Q_TIMELY"]
    labels = ["Required fields", "Business-key uniqueness", "Value validity", "Sensor registry", "Chronology", "Timeliness"]
    fig, axes = plt.subplots(2, 3, figsize=(14, 8))
    for ax, rule, label in zip(axes.flat, names, labels):
        rr = [next(r for r in records if r["phase"] == p and r["rule_id"] == rule)
              for p in ["before", "after_diagnostic"]]
        values = [100 * r["value"] if r["value"] is not None else 0 for r in rr]
        bars = ax.bar(["Before", "After (diagnostic)"], values, color=["#667c99", "#208575"])
        ax.set_ylim(0, 116)
        ax.set_ylabel("Passing rows / eligible rows (%)")
        ax.set_title(label)
        ax.axhline(100 * rr[0]["threshold"], color="#a84a41", linestyle="--", linewidth=1)
        for bar, r in zip(bars, rr):
            text = f'{r["numerator"]:,}/{r["denominator"]:,}\n{r["value"]:.2%}' if r["value"] is not None else "NOT_EVALUATED"
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height()+1, text, ha="center", fontsize=9)
    fig.suptitle("Task 2: SQL-derived quality observations", fontsize=16)
    fig.text(0.5, 0.025, "Before: W (deterministic winners); uniqueness: key-eligible intake. After: diagnostic retained winners.\n"
             "Timeliness: chronologically valid rows only. No T3 verification or release approval is claimed.", ha="center", fontsize=10)
    fig.tight_layout(rect=[0, 0.08, 1, 0.94])
    fig.savefig(output / "quality_comparison.png", dpi=160)
    plt.close(fig)


def report(output, counts, records, investigations, runtime):
    lines = ["# Kết quả Task 2 — Profile and measure quality", "",
             "Dữ liệu synthetic được kiểm tra với `trusted_manifest.json` được cung cấp riêng. "
             "Bản chạy này chỉ đo chất lượng từ input đã cung cấp; chưa duyệt phát hành dữ liệu.", "",
             "Snapshot, envelopes và inventory nằm trong chính thư mục kết quả này. "
             "Contract đã được chụp và băm trước khi đo. Chưa có thành viên thứ hai ký review; "
             "trạng thái PENDING_INDEPENDENT_REVIEW được giữ nguyên, không tự nhận đã hoàn tất.", "",
             "## Task 2: quần thể và kết quả", "",
             f'- Raw: **{counts["raw_count"]:,}** bản ghi vật lý.',
             f'- Parse lỗi: **{counts["parse_failures"]:,}**; bị loại trước dedup vì KEY/INGEST_PARSE: **{counts["key_rejected_count"]:,}** (hợp của hai lỗi, không cộng trùng).',
             f'- Key-eligible: **{counts["key_eligible_count"]:,}**; phiên bản thừa: **{counts["duplicate_excess"]:,}**.',
             f'- W: **{counts["winner_count"]:,}** winners, được chọn trước lọc validity.',
             f'- Required-field failures trong parsed intake: **{counts["parsed_intake_required_failures"]:,}/{counts["parsed_intake_count"]:,}**.', "",
             "| Giai đoạn | Rule | Tử số | Mẫu số | Giá trị | Ngưỡng | Trạng thái | Loại khỏi mẫu số |",
             "|---|---|---:|---:|---:|---:|---|---:|"]
    for r in records:
        value = f'{r["value"]:.4%}' if r["value"] is not None else "null"
        lines.append(f'| {r["phase"]} | {r["rule_id"]} | {r["numerator"]:,} | {r["denominator"]:,} | {value} | {r["threshold"]:.0%} | {r["status"]} | {r["excluded"]:,} |')
    retained = counts["diagnostic_retained_count"]
    lines += ["", "Before completeness/value/registry/chronology dùng toàn bộ W. Uniqueness before dùng "
              "key-eligible trước dedup. Timeliness chỉ dùng các hàng có chronology hợp lệ. NULL là thất bại; "
              "mẫu số 0 có giá trị null và NOT_EVALUATED.", "",
              "After là phép chiếu chẩn đoán của các winners hợp lệ trong SQL, có làm tròn DECIMAL(8,2) khi "
              "so với QA; chưa phải kết quả kiểm chứng Task 3. Không xuất Parquet, quarantine ledger, duplicate ledger "
              "hay manifest phát hành. Sau Task 3 cần tính lại trên candidate thực tế.", "",
              f'Giữ lại chẩn đoán **{retained:,}/{counts["raw_count"]:,} ({retained/counts["raw_count"]:.2%})** so với raw; '
              f'**{retained:,}/{counts["winner_count"]:,} ({retained/counts["winner_count"]:.2%})** so với W. '
              f'Winners không đạt validity: **{counts["winner_count"]-retained:,}**. '
              f'Raw ngoài tập giữ lại: **{counts["raw_count"]-retained:,}**, gồm parse/key failures, superseded versions và winners không hợp lệ. '
              f'Còn **{counts["diagnostic_late_count"]:,}** hàng trễ được giữ lại.', "",
              "Completeness tăng sau lọc vì các hàng lỗi bị loại khỏi quần thể, không phải vì giá trị thiếu đã được phục hồi. "
              "QA agreement chỉ mô tả mẫu tham chiếu được cung cấp; coverage được đo riêng trên toàn bộ 100 IDs. "
              "Không dùng QA để sửa hay chọn phép đo.", "", "![So sánh trước và sau chẩn đoán](quality_comparison.png)", "",
              "## Điều tra lỗi", "", "Các nhóm sau có thể chồng lấp. Source references và tối đa 3 ví dụ/nhóm nằm trong "
              "`defect_investigation.json`; không đưa email hay raw payload vào báo cáo.", ""]
    explanations = {
        "PARSE": "JSON không parse được vẫn có envelope và source_row, không bị bỏ qua.",
        "KEY": "ID thiếu/sai mẫu R[0-9]{6}; loại trước khi xếp hạng.",
        "INGEST_PARSE": "Arrival time không đúng UTC format hoặc không parse được; không thể dùng để chọn winner.",
        "SUPERSEDED": "Phiên bản thừa theo thứ tự ingest giảm dần, source_object và source_row tăng dần.",
        "REQUIRED": "Một hay nhiều trường bắt buộc rỗng; email là tùy chọn nên không tham gia.",
        "NUMERIC": "Reading không chuyển thành số hữu hạn, bao gồm NULL/NaN/Infinity.",
        "UNIT": "Unit thiếu hoặc khác C/F; không mặc định thành Celsius.",
        "VALUE": "Không finite/không đổi đơn vị được hoặc ngoài [-30,60] trước khi làm tròn.",
        "REFERENCE": "Sensor không nằm trong registry; đây không phải QA disagreement.",
        "TIME": "Event/ingest không parse được hoặc sai thứ tự/cutoff AS_OF.",
        "LATE": "Chronology hợp lệ nhưng lag > 900 giây; vẫn giữ nếu các điều kiện khác hợp lệ."
    }
    for item in investigations:
        refs = "; ".join(f'{Path(x["source_object"]).name}:{x["source_row"]}' for x in item["sample_source_references"])
        lines.append(f'- **{item["category"]}: {item["count"]:,}** ({item["cohort"]}). {explanations[item["category"]]} Ví dụ: {refs}.')
    lines += ["", "## Môi trường và giới hạn", "",
              f'Python {runtime["python"]}; DuckDB {runtime["packages"]["duckdb"]}. Phiên bản đầy đủ: `runtime-versions.json`.',
              "Bài yêu cầu Python 3.11 và image được chuẩn bị trước. Máy local đang dùng Python 3.14; "
              "không có image digest Kubernetes để xác minh. Snapshot, envelopes và database chứa dữ liệu restricted; "
              "không đưa vào analyst release. Local folder không chứng minh chính sách quyền truy cập S3.", ""]
    (output / "REPORT.md").write_text("\n".join(lines), encoding="utf-8")


def run(args):
    run_id, start = str(uuid.uuid4()), now()
    output = (args.output or TASK2 / "runs" / run_id).resolve()
    input_output = output
    output.mkdir(parents=True, exist_ok=False)
    contract_bytes = (TASK2 / "contract.json").read_bytes()
    (output / "contract.json").write_bytes(contract_bytes)
    contract = json.loads(contract_bytes)
    schema = json.loads((TASK2 / "schemas/observation_contract_schema.json").read_text(encoding="utf-8"))
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(contract)
    save_json(input_output / "contract-freeze.json", {
        "frozen_at": now(), "contract_sha256": sha256(output / "contract.json"),
        "contract_version": contract["contract_version"], "schema_validation": "PASS",
        "independent_review": contract["review"]["status"]})
    # Reject unsupported implementation changes instead of silently ignoring new policies.
    if contract["deduplication"]["order"] != ["ingest_ts DESC", "source_object ASC", "source_row ASC"]:
        raise ValueError("Unsupported winner ordering; implement a new SQL version first")
    (output / "trusted_manifest.json").write_bytes(args.manifest.read_bytes())
    code_files = sorted([*Path(__file__).parent.glob("*.py"), *Path(__file__).parent.glob("*.sql"), TASK2 / "requirements.txt"], key=lambda p: p.relative_to(TASK2).as_posix())
    code_hashes = [{"file": p.relative_to(TASK2).as_posix(), "sha256": sha256(p)} for p in code_files]
    import hashlib
    code_digest = hashlib.sha256(json.dumps(code_hashes, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    frozen_code = output / "code"
    frozen_code.mkdir()
    for p in code_files:
        (frozen_code / p.name).write_bytes(p.read_bytes())
    save_json(output / "code-hashes.json", {"canonicalization": "UTF-8 JSON, filename-sorted entries, sorted keys, compact separators", "files": code_hashes, "code_sha256": code_digest})
    prepare(input_output / "trusted_manifest.json", args.input_dir, input_output, run_id)
    con = duckdb.connect(str(output / "restricted" / "work.duckdb"))
    con.execute("CREATE TABLE raw_envelopes AS SELECT * FROM read_csv(?, header=true, all_varchar=true)", [str(input_output / "restricted/envelopes.csv")])
    con.execute("CREATE TABLE registry AS SELECT upper(trim(sensor_id)) AS sensor_id, site FROM read_csv(?, all_varchar=true)", [str(input_output / "snapshot/sensors.csv")])
    con.execute("CREATE TABLE qa_reference AS SELECT upper(trim(record_id)) AS record_id, cast(reference_c AS DECIMAL(8,2)) AS reference_c FROM read_csv(?, all_varchar=true)", [str(input_output / "snapshot/qa_reference.csv")])
    for table, key, expected in [("registry", "sensor_id", 20), ("qa_reference", "record_id", 100)]:
        n, distinct = con.execute(f"SELECT count(*), count(DISTINCT {key}) FROM {table}").fetchone()
        if n != distinct or n != expected:
            raise RuntimeError("REFERENCE_INTEGRITY: " + table)
    load_policy(con, contract)
    for name in ["typed.sql", "quality.sql"]:
        con.execute((frozen_code / name).read_text(encoding="utf-8"))
    checks = rows(con, "SELECT source_object, count(*) AS envelopes, count(*) FILTER (WHERE NOT parse_ok) AS parse_failures FROM typed GROUP BY source_object ORDER BY source_object")
    repeated = con.execute("SELECT count(*) FROM (SELECT source_object, source_row FROM typed GROUP BY ALL HAVING count(*) > 1)").fetchone()[0]
    total = sum(r["envelopes"] for r in checks)
    if total != 10205 or repeated:
        raise RuntimeError("PHYSICAL_KEYS_FAILED")
    save_json(input_output / "envelope-check.json", {"check_id": "PHYSICAL_KEYS", "status": "PASS", "count": total,
              "repeated_physical_keys": repeated, "by_source": checks, "parse_failures_preserved": True})
    counts = rows(con, "SELECT * FROM intake_counts")[0]
    assert counts["raw_count"] == counts["parse_failures"] + counts["key_rejected_count"] + counts["duplicate_excess"] + counts["winner_count"]
    save_json(output / "intake_profile.json", counts)
    records = rows(con, "SELECT * FROM metric_counts ORDER BY CASE phase WHEN 'before' THEN 0 ELSE 1 END, rule_id")
    for r in records:
        n, d = r["numerator"], r["denominator"]
        if not 0 <= n <= d:
            raise RuntimeError("Invalid metric counts")
        r.update(dataset_id="deterministic-winners" if r["phase"] == "before" else "diagnostic-candidate",
                 dataset_version=contract["schema_version"] + ":" + sha256(args.manifest), run_id=run_id,
                 contract_version=contract["contract_version"], severity="BLOCK",
                 value=n/d if d else None, threshold=contract["release_thresholds"][r.pop("threshold_key")])
        r["status"] = "NOT_EVALUATED" if d == 0 else ("PASS" if r["value"] >= r["threshold"] else "FAIL")
        r["cohort"] = r["cohort"].replace("all W winners / diagnostic retained winners",
                                            "all W winners" if r["phase"] == "before" else "diagnostic retained winners")
    envelope = {"run_id": run_id, "as_of": contract["as_of"], "measured_at": now(),
                "source_manifest_sha256": sha256(args.manifest), "contract_sha256": sha256(output / "contract.json"),
                "code_sha256": code_digest, "input_counts": counts, "release_approved": False}
    save_json(output / "quality_before.json", dict(envelope, population="W: deterministic winners before row-validity filtering",
              metrics=[r for r in records if r["phase"] == "before"],
              reference_evaluation={"status": "NOT_EVALUATED", "reason": "QA is evaluated on the candidate, not on W."}))
    save_json(output / "quality_after_diagnostic.json", dict(envelope, population="Valid retained winners; SQL diagnostic projection only",
              task3_verification="NOT_PERFORMED", metrics=[r for r in records if r["phase"] == "after_diagnostic"]))
    save_json(output / "metric_records.json", records)
    investigations = rows(con, "SELECT category, cohort, count(*) AS count FROM defects GROUP BY ALL ORDER BY category")
    for item in investigations:
        item["sample_source_references"] = rows(con, "SELECT source_object, source_row FROM defects WHERE category='" + item["category"] + "' ORDER BY source_object, source_row LIMIT 3")
    save_json(output / "defect_investigation.json", {"categories_overlap": True, "categories": investigations})
    chart(output, records)
    con.close()
    runtime = {"python": platform.python_version(), "packages": {p: importlib.metadata.version(p) for p in ["duckdb", "jsonschema", "matplotlib"]},
               "installed_packages": {d.metadata["Name"]: d.version for d in importlib.metadata.distributions()},
               "image_digest": os.environ.get("IMAGE_DIGEST"), "image_digest_status": "RECORDED_FROM_ENV" if os.environ.get("IMAGE_DIGEST") else "UNAVAILABLE_LOCAL_EXECUTION",
               "prepared_environment_match": sys.version_info[:2] == (3, 11), "platform": platform.system()}
    save_json(output / "runtime-versions.json", runtime)
    # Store executed counterexample evidence alongside the observed data metrics.
    import subprocess
    test_result = subprocess.run([sys.executable, str(TASK2 / "code/test_quality.py")],
                                 capture_output=True, text=True, encoding="utf-8")
    save_json(output / "quality-tests.json", {
        "executed_at": now(), "command": "python code/test_quality.py", "exit_code": test_result.returncode,
        "status": "PASS" if test_result.returncode == 0 else "FAIL",
        "output": test_result.stdout + test_result.stderr})
    if test_result.returncode:
        raise RuntimeError("QUALITY_COUNTEREXAMPLES_FAILED")
    report(output, counts, records, investigations, runtime)
    record = dict(envelope, started_at=start, ended_at=now(), mode="provided-local-input",
                  operator=args.operator, reviewer=None, status="COMPLETED",
                  independent_contract_review="PENDING", runtime=runtime)
    save_json(output / "evidence-index.json", {"run_id": run_id, "operator": args.operator, "reviewer": None,
              "input_version": envelope["source_manifest_sha256"], "checks": [
                  {"id": "INPUT", "status": "PASS", "evidence": "input_inventory.json", "command": "verify supplied bytes against trusted manifest"},
                  {"id": "PHYSICAL_KEYS", "status": "PASS", "evidence": "envelope-check.json", "command": "count physical records and check repeated physical keys"},
                  {"id": "QUALITY", "status": "COMPUTED", "evidence": "quality_before.json", "command": "execute code/typed.sql then code/quality.sql"},
                  {"id": "COUNTEREXAMPLES", "status": "PASS", "evidence": "quality-tests.json", "command": "python code/test_quality.py"}]})
    record["outputs"] = [{"file": p.relative_to(output).as_posix(), "sha256": sha256(p)}
                         for p in sorted(output.rglob("*")) if p.is_file() and "matplotlib" not in p.parts]
    save_json(output / "run-record.json", record)
    save_json(TASK2 / "latest-run.json", {"run_id": run_id,
              "path": output.relative_to(TASK2).as_posix() if output.is_relative_to(TASK2) else str(output)})
    print(json.dumps({"output": str(output), "counts": counts}, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["local"], default="local", help="Local supplied input only")
    parser.add_argument("--manifest", type=Path, default=TASK2 / "trusted_manifest.json")
    parser.add_argument("--input-dir", type=Path, default=TASK2 / "input")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--operator", default="automated-quality-run")
    run(parser.parse_args())
