-- Every denominator is explicit; NULL predicates are failures, never dropped rows.
CREATE OR REPLACE VIEW metric_counts AS
WITH populations AS (
  SELECT 'before' AS phase, * FROM evaluated_winners
  UNION ALL BY NAME
  SELECT 'after_diagnostic' AS phase, * FROM profile_candidate
), phases AS (
  SELECT unnest(['before', 'after_diagnostic']) AS phase
), grouped AS (
  SELECT phase, count(*) AS n,
    count(*) FILTER (WHERE required_ok) AS required_n,
    count(*) FILTER (WHERE value_ok) AS value_n,
    count(*) FILTER (WHERE sensor_ok) AS sensor_n,
    count(*) FILTER (WHERE chronology_ok) AS chronology_n,
    count(*) FILTER (WHERE timely_ok) AS timely_n
  FROM populations GROUP BY phase
), g AS (
  SELECT phase, coalesce(n,0) AS n, coalesce(required_n,0) AS required_n,
    coalesce(value_n,0) AS value_n, coalesce(sensor_n,0) AS sensor_n,
    coalesce(chronology_n,0) AS chronology_n, coalesce(timely_n,0) AS timely_n
  FROM phases LEFT JOIN grouped USING (phase)
)
SELECT phase, 'Q_REQUIRED' AS rule_id, required_n AS numerator, n AS denominator,
  'all W winners / diagnostic retained winners' AS cohort, 'required_completeness' AS threshold_key,
  0::BIGINT AS excluded FROM g
UNION ALL SELECT phase, 'Q_VALUE', value_n, n, 'all W winners / diagnostic retained winners', 'value_validity', 0 FROM g
UNION ALL SELECT phase, 'Q_SENSOR', sensor_n, n, 'all W winners / diagnostic retained winners', 'sensor_consistency', 0 FROM g
UNION ALL SELECT phase, 'Q_TIME', chronology_n, n, 'all W winners / diagnostic retained winners', 'chronology', 0 FROM g
UNION ALL SELECT phase, 'Q_TIMELY', timely_n, chronology_n, 'chronologically valid rows only', 'timeliness', n-chronology_n FROM g
UNION ALL SELECT 'before', 'Q_UNIQUE', count(DISTINCT record_id), count(*), 'key_eligible before deduplication', 'business_key_uniqueness', 0 FROM key_eligible
UNION ALL SELECT 'after_diagnostic', 'Q_UNIQUE', count(DISTINCT record_id), count(*), 'diagnostic retained winners', 'business_key_uniqueness', 0 FROM profile_candidate
UNION ALL SELECT 'after_diagnostic', 'Q_QA_COVERAGE', count(c.record_id), count(*), 'all frozen QA reference IDs', 'qa_coverage', 0
  FROM qa_reference q LEFT JOIN profile_candidate c USING (record_id)
UNION ALL SELECT 'after_diagnostic', 'Q_QA_AGREEMENT',
  count(*) FILTER (WHERE abs(c.temperature_c-q.reference_c) <= p.qa_tolerance), count(*),
  'matched QA reference IDs only; exported precision', 'qa_agreement', 0
  FROM qa_reference q JOIN profile_candidate c USING (record_id) CROSS JOIN policy p;

CREATE OR REPLACE VIEW intake_counts AS
SELECT
  (SELECT count(*) FROM typed) AS raw_count,
  (SELECT count(*) FROM typed WHERE NOT parse_ok) AS parse_failures,
  (SELECT count(*) FROM pre_key WHERE parse_ok AND NOT key_ok) AS invalid_key_count,
  (SELECT count(*) FROM pre_key WHERE parse_ok AND ingest_ts IS NULL) AS ingest_parse_failure_count,
  (SELECT count(*) FROM pre_key WHERE parse_ok AND (NOT key_ok OR ingest_ts IS NULL)) AS key_rejected_count,
  (SELECT count(*) FROM key_eligible) AS key_eligible_count,
  (SELECT count(*) FROM ranked WHERE version_rank > 1) AS duplicate_excess,
  (SELECT count(*) FROM winners) AS winner_count,
  (SELECT count(*) FROM evaluated WHERE parse_ok AND NOT required_ok) AS parsed_intake_required_failures,
  (SELECT count(*) FROM typed WHERE parse_ok) AS parsed_intake_count,
  (SELECT count(*) FROM profile_candidate) AS diagnostic_retained_count,
  (SELECT count(*) FROM profile_candidate WHERE is_late) AS diagnostic_late_count;

-- Categories overlap intentionally; samples contain source references, no raw values.
CREATE OR REPLACE VIEW defects AS
SELECT 'PARSE' AS category, 'raw intake' AS cohort, source_object, source_row FROM typed WHERE NOT parse_ok
UNION ALL SELECT 'KEY', 'parsed intake before dedup', source_object, source_row FROM pre_key WHERE parse_ok AND NOT key_ok
UNION ALL SELECT 'INGEST_PARSE', 'parsed intake before dedup', source_object, source_row FROM pre_key WHERE parse_ok AND ingest_ts IS NULL
UNION ALL SELECT 'SUPERSEDED', 'key_eligible', source_object, source_row FROM ranked WHERE version_rank > 1
UNION ALL SELECT 'REQUIRED', 'W', source_object, source_row FROM evaluated_winners WHERE NOT required_ok
UNION ALL SELECT 'NUMERIC', 'W', source_object, source_row FROM evaluated_winners WHERE NOT numeric_ok
UNION ALL SELECT 'UNIT', 'W', source_object, source_row FROM evaluated_winners WHERE NOT unit_ok
UNION ALL SELECT 'VALUE', 'W', source_object, source_row FROM evaluated_winners WHERE NOT value_ok
UNION ALL SELECT 'REFERENCE', 'W sensor registry check', source_object, source_row FROM evaluated_winners WHERE NOT sensor_ok
UNION ALL SELECT 'TIME', 'W', source_object, source_row FROM evaluated_winners WHERE NOT chronology_ok
UNION ALL SELECT 'LATE', 'chronologically valid W', source_object, source_row FROM evaluated_winners WHERE chronology_ok AND NOT timely_ok;
