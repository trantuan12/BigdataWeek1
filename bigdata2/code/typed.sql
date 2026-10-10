-- Role C: exact UTC parsing and normalization; missing values remain failures.
CREATE OR REPLACE VIEW typed AS
WITH norm AS (
  SELECT * EXCLUDE(record_id,sensor_id,unit,reading,event_time,ingest_time,source_row,parse_ok),
    upper(blank(record_id)) AS record_id,
    upper(blank(sensor_id)) AS sensor_id,
    upper(blank(unit)) AS unit,
    blank(reading) AS reading, blank(event_time) AS event_time,
    blank(ingest_time) AS ingest_time,
    cast(source_row AS BIGINT) AS source_row, cast(parse_ok AS BOOLEAN) AS parse_ok
  FROM raw_envelopes
)
SELECT *, try_cast(reading AS DOUBLE) AS value_num,
  CASE WHEN regexp_full_match(event_time,(SELECT timestamp_pattern FROM cfg))
       THEN try_strptime(event_time,'%Y-%m-%dT%H:%M:%SZ') END AS event_ts,
  CASE WHEN regexp_full_match(ingest_time,(SELECT timestamp_pattern FROM cfg))
       THEN try_strptime(ingest_time,'%Y-%m-%dT%H:%M:%SZ') END AS ingest_ts
FROM norm;
