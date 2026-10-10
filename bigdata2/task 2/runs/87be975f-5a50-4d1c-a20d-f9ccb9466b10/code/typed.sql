SET TimeZone='UTC';
SET threads=2;
SET memory_limit='512MB';

-- raw_envelopes, registry, qa_reference and policy are loaded by the driver.
CREATE OR REPLACE VIEW typed AS
WITH normalized AS (
  SELECT * EXCLUDE(record_id, sensor_id, unit, source_row, parse_ok),
    nullif(upper(trim(record_id)), '') AS record_id,
    nullif(upper(trim(sensor_id)), '') AS sensor_id,
    nullif(upper(trim(unit)), '') AS unit,
    cast(source_row AS BIGINT) AS source_row,
    cast(parse_ok AS BOOLEAN) AS parse_ok
  FROM raw_envelopes
)
SELECT *, try_cast(reading AS DOUBLE) AS value_num,
  CASE WHEN regexp_full_match(event_time, '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z')
    THEN try_strptime(event_time, '%Y-%m-%dT%H:%M:%SZ') END AS event_ts,
  CASE WHEN regexp_full_match(ingest_time, '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z')
    THEN try_strptime(ingest_time, '%Y-%m-%dT%H:%M:%SZ') END AS ingest_ts
FROM normalized;

CREATE OR REPLACE VIEW pre_key AS
SELECT *,
  coalesce(regexp_full_match(record_id, p.id_pattern), false) AS key_ok
FROM typed CROSS JOIN policy p;

CREATE OR REPLACE VIEW key_eligible AS
SELECT * FROM pre_key WHERE parse_ok AND key_ok AND ingest_ts IS NOT NULL;

CREATE OR REPLACE VIEW ranked AS
SELECT *, row_number() OVER (
  PARTITION BY record_id ORDER BY ingest_ts DESC, source_object ASC, source_row ASC
) AS version_rank FROM key_eligible;

CREATE OR REPLACE VIEW winners AS
SELECT * FROM ranked WHERE version_rank = 1;

-- Evaluate predicates on every parsed row as well as W, so exclusions are visible.
CREATE OR REPLACE VIEW evaluated AS
WITH converted AS (
  SELECT t.*, r.site,
    CASE WHEN t.unit = 'C' THEN value_num
         WHEN t.unit = 'F' THEN (value_num - 32) * 5.0 / 9.0 END AS temperature_unrounded,
    coalesce(nullif(trim(record_id), '') IS NOT NULL
      AND nullif(trim(t.sensor_id), '') IS NOT NULL
      AND nullif(trim(event_time), '') IS NOT NULL
      AND nullif(trim(ingest_time), '') IS NOT NULL
      AND nullif(trim(reading), '') IS NOT NULL
      AND nullif(trim(unit), '') IS NOT NULL, false) AS required_ok,
    coalesce(isfinite(value_num), false) AS numeric_ok,
    coalesce(list_contains(p.supported_units, t.unit), false) AS unit_ok,
    r.sensor_id IS NOT NULL AS sensor_ok,
    event_ts IS NOT NULL AND ingest_ts IS NOT NULL AS time_parse_ok,
    coalesce(event_ts <= ingest_ts AND ingest_ts <= p.as_of, false) AS chronology_ok,
    epoch(ingest_ts - event_ts) AS lag_seconds
  FROM typed t CROSS JOIN policy p LEFT JOIN registry r ON t.sensor_id = r.sensor_id
)
SELECT c.*,
  coalesce(numeric_ok AND unit_ok AND isfinite(temperature_unrounded)
    AND temperature_unrounded BETWEEN p.min_c AND p.max_c, false) AS value_ok,
  coalesce(chronology_ok AND lag_seconds <= p.late_seconds, false) AS timely_ok
FROM converted c CROSS JOIN policy p;

CREATE OR REPLACE VIEW evaluated_winners AS
SELECT e.* FROM evaluated e JOIN winners w USING (source_object, source_row);

-- Supplemental after population for Task 2 chart only. This is not a T3 release.
-- No reference measurements participate in selection, conversion or validity.
CREATE OR REPLACE VIEW profile_candidate AS
SELECT *, cast(temperature_unrounded AS DECIMAL(8,2)) AS temperature_c,
  lag_seconds > (SELECT late_seconds FROM policy) AS is_late
FROM evaluated_winners
WHERE required_ok AND value_ok AND sensor_ok AND chronology_ok;
