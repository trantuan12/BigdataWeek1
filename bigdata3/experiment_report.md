# Lab 3 Experiment Report — Lakehouse Processing & Analytics

**Group ID:** `bd-g10`  
**Run Label:** `run01`  
**Author (Task 1):** Trần Anh Tuấn (24022484, Role A)  
**Reviewer:** Nguyễn Tùng Dương (24022306, Role E)  
**Execution Environment:** `local[2]` (Spark 4.0.1, Delta Lake 4.0.1, Python 3.11.9, Java 23.0.2, Hadoop 3.4.1)

---

## 1. Upstream Handoff

### 1.1 Handoff Mode and Logical Identity
For this experimental run, the team utilized the course reference fallback dataset (`data/handoff/curated.csv`), derived by the teaching assistants under the exact rules of Lab 2. We explicitly record and document that this is a **CSV transport format of the Lab 2 ten-column schema (`course_fallback_not_student_approval`)**, and not a fabricated Parquet file or a student-signed release from our own team.

- **Handoff ID:** `lab2-research-telemetry-v1-reference-handoff`
- **Source Fixture Version:** `research-telemetry-v1`
- **Upstream Lab:** Lab 2
- **AS_OF Cutoff:** `2026-02-09T00:00:00Z`
- **Total Physical Records:** 9,610 valid observations
- **Data File Path:** `data/handoff/curated.csv`
- **Data Transport SHA-256:** `5770802a599a2931751f5ccd41e0305a03c98a08bdf5504ac8df0d1f990d8c1d`
- **Canonical Logical Baseline SHA-256:** `c50f8252bfb91890975d26b16fe1d2b32f0e283a58e46bd16260b4a3cca0bf0d`
- **Lab 2 Source Manifest Hash:** `77dacfdb5a8b4e82fb2316990972de731a51b31f9fc052d739d0dea78e992331`

### 1.2 Independent Trust Anchor
The authenticity of all experimental artifacts is anchored by the independently supplied manifest `trusted_lab3_inputs.json`, distributed via the instructor-controlled channel rather than relying on the convenience copy inside the package.
- **Trusted Manifest Path:** `trusted_lab3_inputs.json`
- **Authoritative SHA-256:** `3cdfc84b26d290100b39ce65d4ff7cb22839f4021b9caf26897c8a4ff68091a2`
- **Verification Evidence:** [`evidence/input_integrity.json`](evidence/input_integrity.json) (16/16 artifacts passed byte size and checksum verification).

### 1.3 Released Schema and Upstream Boundaries
The handoff preserves the exact 10 allowlisted columns from Lab 2 without alteration:
`record_id`, `sensor_id`, `site`, `event_time_utc`, `ingest_time_utc`, `temperature_c`, `is_late`, `source_object`, `source_row`, `source_sha256`.

- **Unit and Precision:** Timestamps are formatted in UTC second-resolution text (`YYYY-MM-DDTHH:MM:SSZ`), temperatures are stored as `DECIMAL(8,2)` in Celsius, `is_late` is a boolean flag (`lag > 900s`).
- **Location of Raw Data:** The original raw telemetry files (`observations_a.csv` and `observations_b.jsonl`) remain strictly in the Lab 2 workspace archive (`data/lab2_sources/`). They contain synthetic restricted fields (such as `operator_email`) and are strictly excluded from the release and working Lakehouse tables. Row-level provenance is preserved through `source_object`, `source_row`, and `source_sha256`, which act as verifiable audit pointers rather than retrieval authorizations.

### 1.4 Native Delta Schema Enforcement Probe
Prior to running operational tables, an isolated Delta smoke probe was executed (`evidence/delta_schema_probe.json`):
- A temporary Delta table with 8 initial rows was created at `d:/BigData/BigdataWeek1/bigdata3/lakehouse/bd-g10/run01/preflight-16188ad4-27ef-466c-847c-13ae8ad9f8c4`.
- Attempting to append a record with an unexpected column (`unexpected`) resulted in immediate rejection by Delta Lake with `AnalysisException: A schema mismatch detected when writing to the Delta table`.
- The table was re-read: row count remained strictly **8 rows**, confirming fail-closed schema protection without table corruption.

---

## 2. Table Semantics and Lineage
*(Assigned to Member B — Task 2)*
- Bronze URI & Version: `[Pending Bootstrap]`
- Silver URI & Version: `[Pending Bootstrap]`
- Gold URI & Version: `[Pending Bootstrap]`
- Provenance trace explanation: `[Pending Bootstrap]`

---

## 3. Incremental Evidence
*(Assigned to Member C — Task 3)*
- Incoming revisions: 140 physical rows
- Deduped winners, inserts, updates, stale records: `[Pending Incremental]`
- Replay & time-travel invariants: `[Pending Incremental]`

---

## 4. Analytics
*(Assigned to Member D — Task 4)*
- SQL queries Q1, Q2, Q3 outputs: `[Pending Analytics]`
- Observation-weighted mean vs naive mean: `[Pending Analytics]`
- Threshold interpretation: `[Pending Analytics]`

---

## 5. Controlled Layout Experiment
*(Assigned to Member D — Task 4)*
- Layout A vs Layout B 6-run measured benchmark: `[Pending Benchmark]`
- PartitionFilters analysis: `[Pending Benchmark]`

---

## 6. Release and Accountability
*(Assigned to Member E — Task 5)*
- Release candidate binding: `[Pending Release]`
- Gate outcomes R01–R05: `[Pending Release]`
- Lineage summary: `[Pending Release]`

