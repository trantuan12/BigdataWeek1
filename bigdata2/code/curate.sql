-- Role C: pre-key rejection -> deterministic selection -> winner validity.
CREATE OR REPLACE VIEW pre_classified AS
SELECT *, list_filter([
  CASE WHEN NOT parse_ok THEN 'PARSE' END,
  CASE WHEN parse_ok AND NOT coalesce(regexp_full_match(record_id,(SELECT id_pattern FROM cfg)),false) THEN 'KEY' END,
  CASE WHEN parse_ok AND ingest_ts IS NULL THEN 'INGEST_PARSE' END
], x -> x IS NOT NULL) AS reason_codes
FROM typed;

CREATE OR REPLACE VIEW key_eligible AS SELECT * FROM pre_classified WHERE len(reason_codes)=0;
CREATE OR REPLACE VIEW ranked AS
SELECT *, row_number() OVER (PARTITION BY record_id ORDER BY ingest_ts DESC,source_object ASC,source_row ASC) AS version_rank,
  first_value(source_object) OVER (PARTITION BY record_id ORDER BY ingest_ts DESC,source_object ASC,source_row ASC) AS winner_source_object,
  first_value(source_row) OVER (PARTITION BY record_id ORDER BY ingest_ts DESC,source_object ASC,source_row ASC) AS winner_source_row
FROM key_eligible;
CREATE OR REPLACE VIEW winners AS SELECT * FROM ranked WHERE version_rank=1;

CREATE OR REPLACE VIEW evaluated AS
WITH converted AS (
 SELECT w.*, r.site,
   CASE WHEN w.unit='C' THEN value_num WHEN w.unit='F' THEN (value_num-32)*5.0/9.0 END AS temperature_unrounded,
   record_id IS NOT NULL AND w.sensor_id IS NOT NULL AND event_time IS NOT NULL
     AND ingest_time IS NOT NULL AND reading IS NOT NULL AND w.unit IS NOT NULL AS complete_ok,
   coalesce(isfinite(value_num),false) AS numeric_ok,
   coalesce(w.unit IN (SELECT unit FROM supported_units),false) AS unit_ok,
   coalesce(r.sensor_id IS NOT NULL,false) AS sensor_ok,
   coalesce(event_ts<=ingest_ts AND ingest_ts<=(SELECT as_of FROM cfg),false) AS chronology_ok,
   date_diff('second',event_ts,ingest_ts) AS lag_seconds
 FROM winners w LEFT JOIN registry r ON w.sensor_id=upper(blank(r.sensor_id))
)
SELECT * EXCLUDE(reason_codes),
  coalesce(numeric_ok AND unit_ok AND isfinite(temperature_unrounded)
    AND temperature_unrounded BETWEEN (SELECT lo FROM cfg) AND (SELECT hi FROM cfg),false) AS value_ok,
  list_filter([
    CASE WHEN NOT complete_ok THEN 'REQUIRED' END,
    CASE WHEN NOT numeric_ok THEN 'NUMERIC' END,
    CASE WHEN NOT unit_ok THEN 'UNIT' END,
    CASE WHEN numeric_ok AND unit_ok AND NOT coalesce(isfinite(temperature_unrounded) AND
        temperature_unrounded BETWEEN (SELECT lo FROM cfg) AND (SELECT hi FROM cfg),false) THEN 'RANGE' END,
    CASE WHEN event_ts IS NULL OR ingest_ts IS NULL THEN 'TIME_PARSE' END,
    CASE WHEN event_ts IS NOT NULL AND ingest_ts IS NOT NULL AND NOT chronology_ok THEN 'TIME_ORDER' END,
    CASE WHEN NOT sensor_ok THEN 'REFERENCE' END
  ], x -> x IS NOT NULL) AS reason_codes
FROM converted;

CREATE OR REPLACE VIEW quarantine AS
WITH q AS (
  SELECT source_object,source_row,source_sha256,record_id,sensor_id,event_time,ingest_time,
         reading,unit,parse_ok,raw_payload,reason_codes FROM pre_classified WHERE len(reason_codes)>0
  UNION ALL
  SELECT source_object,source_row,source_sha256,record_id,sensor_id,event_time,ingest_time,
         reading,unit,parse_ok,raw_payload,reason_codes FROM evaluated WHERE len(reason_codes)>0
)
SELECT * EXCLUDE(reason_codes),
  (SELECT r.reason FROM reason_order r WHERE list_contains(q.reason_codes,r.reason) ORDER BY ordinal LIMIT 1) AS primary_reason,
  array_to_string(reason_codes,'|') AS all_reasons
FROM q;

CREATE OR REPLACE VIEW duplicates AS
SELECT source_object,source_row,source_sha256,record_id,sensor_id,
       winner_source_object,winner_source_row,'SUPERSEDED'::VARCHAR AS reason
FROM ranked WHERE version_rank>1;

CREATE OR REPLACE VIEW curated AS
SELECT record_id,sensor_id,site,event_ts::TIMESTAMP AS event_time_utc,
  ingest_ts::TIMESTAMP AS ingest_time_utc,temperature_unrounded::DECIMAL(8,2) AS temperature_c,
  (lag_seconds>(SELECT late_seconds FROM cfg))::BOOLEAN AS is_late,
  source_object,source_row::BIGINT AS source_row,source_sha256
FROM evaluated WHERE len(reason_codes)=0;
