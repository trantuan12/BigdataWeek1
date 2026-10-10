"""Verify the supplied Task 2 inputs and preserve physical record provenance."""
import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

FIELDS = ["record_id", "sensor_id", "event_time", "ingest_time", "reading", "unit", "operator_email"]
BKEYS = ["id", "sensor", "timestamp", "arrived_at", "temperature", "temperature_unit", "operator"]
KEYS = {"observations_a.csv", "observations_b.jsonl", "sensors.csv", "qa_reference.csv"}
EXTRA = ["source_object", "source_row", "source_sha256", "parse_ok", "raw_payload"]


def now():
    return datetime.now(timezone.utc).isoformat()


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def save_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def prepare(manifest_path, input_dir, output, run_id):
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    objects = manifest["objects"]
    if len(objects) != 4 or {obj["key"] for obj in objects} != KEYS:
        raise RuntimeError("INPUT_INTEGRITY: exact four-object manifest required")
    snapshot = output / "snapshot"
    snapshot.mkdir()
    (output / "restricted").mkdir()
    inventory = []
    for obj in objects:
        data = (input_dir / obj["key"]).read_bytes()
        digest = hashlib.sha256(data).hexdigest()
        if len(data) != obj["bytes"] or digest != obj["sha256"]:
            raise RuntimeError("INPUT_INTEGRITY: " + obj["key"])
        (snapshot / obj["key"]).write_bytes(data)
        inventory.append(dict(obj, source_object="s3://research-raw/lab2/inputs/batch-01/" + obj["key"],
                              verification_status="PASS", verified_at=now()))
    count, parse_failures = 0, 0
    with (output / "restricted/envelopes.csv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS + EXTRA)
        writer.writeheader()
        for obj in inventory:
            key = obj["key"]
            n = 0
            with (snapshot / key).open(encoding="utf-8", newline="") as src:
                records = src if key.endswith(".jsonl") else csv.DictReader(src)
                if key.endswith(".csv"):
                    header = FIELDS if key.startswith("observations_") else (
                        ["sensor_id", "site"] if key == "sensors.csv" else ["record_id", "reference_c"])
                    if records.fieldnames != header:
                        raise RuntimeError("INPUT_SCHEMA: " + key)
                for n, raw in enumerate(records, 1):
                    if not key.startswith("observations_"):
                        continue
                    payload = json.dumps(raw, ensure_ascii=False) if isinstance(raw, dict) else raw
                    ok = True
                    try:
                        record = raw if isinstance(raw, dict) else json.loads(raw)
                        if not isinstance(record, dict):
                            raise ValueError("not an object")
                        if key.endswith(".jsonl"):
                            record = {a: record.get(b) for a, b in zip(FIELDS, BKEYS)}
                        record = {k: "" if record.get(k) is None else str(record[k]) for k in FIELDS}
                    except (ValueError, TypeError):
                        record, ok = dict.fromkeys(FIELDS, ""), False
                    record.update(source_object=obj["source_object"], source_row=n,
                                  source_sha256=obj["sha256"], parse_ok=str(ok).lower(), raw_payload=payload)
                    writer.writerow(record)
                    count += 1
                    parse_failures += not ok
            if n != obj["records"]:
                raise RuntimeError("ROW_COUNT: " + key)
    if count != 10205:
        raise RuntimeError("ROW_COUNT: expected 10205 observations")
    save_json(output / "input_inventory.json", {"run_id": run_id, "mode": "provided-local-input",
              "trusted_manifest_sha256": sha256(manifest_path), "objects": inventory,
              "observation_rows": count, "parse_failures_retained": parse_failures,
              "integrity_status": "PASS", "note": "Source URIs identify original objects; no S3 requests are executed by Task 2."})
    return inventory
