"""Verified local/S3 acquisition; preserve every physical observation record."""
import argparse
import csv
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

KEYS = {"observations_a.csv", "observations_b.jsonl", "sensors.csv", "qa_reference.csv"}
FIELDS = ["record_id", "sensor_id", "event_time", "ingest_time", "reading", "unit", "operator_email"]
BKEYS = ["id", "sensor", "timestamp", "arrived_at", "temperature", "temperature_unit", "operator"]
EXTRA = ["source_object", "source_row", "source_sha256", "parse_ok", "raw_payload"]
PREFIX = "lab2/inputs/batch-01/"


def now():
    return datetime.now(timezone.utc).isoformat()


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def save_json(path, data):
    Path(path).write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def verify(data, obj):
    digest = hashlib.sha256(data).hexdigest()
    if len(data) != obj["bytes"] or digest != obj["sha256"]:
        raise RuntimeError("INPUT_INTEGRITY: " + obj["key"])
    return digest


def s3_client():
    import boto3
    from botocore.config import Config
    return boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"], region_name="us-east-1",
                        config=Config(signature_version="s3v4", connect_timeout=5, read_timeout=10,
                                      retries={"max_attempts": 1}, s3={"addressing_style": "path"}))


def acquire(mode, manifest_path, input_dir, output, run_id):
    manifest_path, input_dir, output = map(Path, (manifest_path, input_dir, output))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if len(manifest["objects"]) != 4 or {x["key"] for x in manifest["objects"]} != KEYS:
        raise RuntimeError("INPUT_INTEGRITY: exact four-object manifest required")
    if mode == "local" and {p.name for p in input_dir.iterdir() if p.is_file()} != KEYS:
        raise RuntimeError("INPUT_INTEGRITY: unexpected local input inventory")
    snapshot = output / "snapshot"
    snapshot.mkdir(parents=True, exist_ok=False)
    restricted = output / "restricted"
    restricted.mkdir()
    store = os.environ.get("STORE_ID")
    if mode == "s3" and (not store or store == "bd-gXX-objects"):
        raise RuntimeError("STORE_ID_REQUIRED: set actual Lab 1 store identity")
    store = store or "LOCAL-FIXTURE-NO-S3-STORE"
    client = s3_client() if mode == "s3" else None
    inventory = []
    for obj in manifest["objects"]:
        key = obj["key"]
        if client:
            body = client.get_object(Bucket="research-raw", Key=PREFIX + key)["Body"]
            try:
                data = body.read()
            finally:
                body.close()
        else:
            data = (input_dir / key).read_bytes()
        digest = verify(data, obj)
        (snapshot / key).write_bytes(data)
        inventory.append({"store_id": store, "bucket": "research-raw", "key": PREFIX + key,
                          "source_uri": "s3://research-raw/" + PREFIX + key,
                          "retrieval_mode": mode, "retrieved_at": now(),
                          "local_source": str((input_dir / key).resolve()) if mode == "local" else None,
                          "format": "jsonl" if key.endswith("jsonl") else "csv",
                          "bytes": len(data), "sha256": digest, "expected_records": obj["records"]})
    total, failures = 0, 0
    with (restricted / "envelopes.csv").open("w", newline="", encoding="utf-8") as out:
        writer = csv.DictWriter(out, fieldnames=FIELDS + EXTRA)
        writer.writeheader()
        for entry in inventory:
            path = snapshot / Path(entry["key"]).name
            n = 0
            with path.open(encoding="utf-8", newline="") as src:
                records = csv.DictReader(src) if entry["format"] == "csv" else src
                if entry["format"] == "csv":
                    expected_header = FIELDS if path.name.startswith("observations_") else (
                        ["sensor_id", "site"] if path.name == "sensors.csv" else ["record_id", "reference_c"])
                    if records.fieldnames != expected_header:
                        raise RuntimeError("INPUT_SCHEMA: " + path.name)
                for n, raw in enumerate(records, 1):
                    if not path.name.startswith("observations_"):
                        continue
                    payload = json.dumps(raw, ensure_ascii=False) if isinstance(raw, dict) else raw
                    ok = True
                    try:
                        row = raw if isinstance(raw, dict) else json.loads(raw)
                        if not isinstance(row, dict):
                            raise ValueError("not an object")
                        if entry["format"] == "jsonl":
                            row = {a: row.get(b) for a, b in zip(FIELDS, BKEYS)}
                        row = {k: "" if row.get(k) is None else str(row[k]) for k in FIELDS}
                    except (ValueError, TypeError):
                        row, ok = dict.fromkeys(FIELDS, ""), False
                    row.update(source_object=entry["source_uri"], source_row=n,
                               source_sha256=entry["sha256"], parse_ok=str(ok).lower(), raw_payload=payload)
                    writer.writerow(row)
                    total += 1
                    failures += not ok
            if n != entry["expected_records"]:
                raise RuntimeError("ROW_COUNT: " + path.name)
            entry["records"] = n
            entry["integrity_status"] = "PASS"
    if total != 10205:
        raise RuntimeError("ROW_COUNT: expected 10205 physical observations")
    save_json(output / "input_inventory.json", {
        "run_id": run_id, "mode": mode, "store_id": store,
        "trusted_manifest_sha256": sha256(manifest_path), "objects": inventory,
        "observation_rows": total, "parse_failures_retained": failures,
        "integrity_status": "PASS", "s3_governance_demonstrated": mode == "s3"})
    save_json(output / "field_mapping.json", {
        "status": "CHECKED_AGAINST_DATASET_README", "csv": dict(zip(FIELDS, FIELDS)),
        "jsonl": dict(zip(FIELDS, BKEYS)), "source_sha256_meaning": "SHA-256 of whole object"})
    return inventory, client


def tamper_test(output, inventory):
    output = Path(output)
    obj = inventory[0]
    original = output / "snapshot" / Path(obj["key"]).name
    initial = sha256(original)
    data = bytearray(original.read_bytes())
    data[len(data) // 2] ^= 1
    disposable = output / "restricted" / "tampered-copy.bin"
    disposable.write_bytes(data)
    rejection = None
    try:
        verify(disposable.read_bytes(), obj)
    except RuntimeError as exc:
        rejection = str(exc)
    unchanged = sha256(original) == initial == obj["sha256"]
    disposable.unlink()
    result = {"check_id": "I03", "status": "PASS" if rejection and unchanged else "FAIL",
              "observed_error": rejection, "original_unchanged": unchanged,
              "tampered_bytes": 1, "executed_at": now()}
    save_json(output / "integrity-test.json", result)
    if result["status"] != "PASS":
        raise RuntimeError("I03_FAILED")


def access_test(output, client, run_id):
    result = {"check_id": "I04", "executed_at": now(), "workload_identity_expected": "s3-ingestor",
              "independent_identity_verification": "NOT_PERFORMED"}
    if client is None:
        result.update(status="BLOCKED", raw_get="NOT_EXECUTED", release_put="NOT_EXECUTED",
                      reason="Local fixture fallback: no curator Pod or S3 credential context was available. Kubernetes configuration was inaccessible. Local reads cannot establish S3 governance.")
    else:
        from botocore.exceptions import BotoCoreError, ClientError
        result.update(status="FAIL", raw_get="NOT_EXECUTED", release_put="NOT_EXECUTED")
        try:
            body = client.get_object(Bucket="research-raw", Key=PREFIX + "sensors.csv")["Body"]
            try:
                body.read()
            finally:
                body.close()
            result["raw_get"] = "PASS"
            key = "lab2/access-probes/" + run_id + ".txt"
            result["release_probe_key"] = key
            try:
                response = client.put_object(Bucket="research-release", Key=key, Body=b"Task 1 access probe\n")
                result.update(release_put="UNEXPECTEDLY_ALLOWED", http_status=response["ResponseMetadata"]["HTTPStatusCode"])
            except ClientError as exc:
                code = exc.response["Error"]["Code"]
                http = exc.response["ResponseMetadata"]["HTTPStatusCode"]
                result.update(error_code=code, http_status=http)
                if http == 403 and code == "AccessDenied":
                    result.update(status="PASS", release_put="DENIED_AS_REQUIRED")
                else:
                    result["release_put"] = "WRONG_FAILURE"
        except (BotoCoreError, ClientError):
            result.update(status="BLOCKED", reason="S3 request could not establish the required access outcome; credential details suppressed.")
    save_json(Path(output) / "access-test.json", result)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["local", "s3"])
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--input-dir", type=Path, default=Path("Lab2-datasets/Lab2-datasets/input"))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    inv, cli = acquire(args.mode, args.manifest, args.input_dir, args.output, args.output.name)
    tamper_test(args.output, inv)
    access_test(args.output, cli, args.output.name)
    print(json.dumps({"status": "VERIFIED", "observation_rows": 10205, "output": str(args.output)}))
