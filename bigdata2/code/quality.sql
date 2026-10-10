-- Role B: every denominator explicitly includes failures and NULLs.
CREATE OR REPLACE VIEW metric_counts AS
SELECT 'before' AS phase,'Q_REQUIRED' AS rule_id,
  'W: deterministic winners before row-validity filtering' AS cohort,
  count(*) FILTER (WHERE complete_ok) AS numerator,count(*) AS denominator FROM evaluated
UNION ALL SELECT 'before','Q_UNIQUE','Key-eligible physical records before deduplication',count(DISTINCT record_id),count(*) FROM key_eligible
UNION ALL SELECT 'before','Q_VALUE','W: deterministic winners before row-validity filtering',count(*) FILTER (WHERE value_ok),count(*) FROM evaluated
UNION ALL SELECT 'before','Q_SENSOR','W: deterministic winners before row-validity filtering',count(*) FILTER (WHERE sensor_ok),count(*) FROM evaluated
UNION ALL SELECT 'before','Q_TIME','W: deterministic winners before row-validity filtering',count(*) FILTER (WHERE chronology_ok),count(*) FROM evaluated
UNION ALL SELECT 'before','Q_TIMELY','Chronologically valid W only; invalid chronology excluded',count(*) FILTER (WHERE chronology_ok AND lag_seconds<=(SELECT late_seconds FROM cfg)),count(*) FILTER (WHERE chronology_ok) FROM evaluated
UNION ALL SELECT 'after','Q_REQUIRED','All curated rows',count(*) FILTER (WHERE record_id IS NOT NULL AND sensor_id IS NOT NULL AND site IS NOT NULL AND event_time_utc IS NOT NULL AND ingest_time_utc IS NOT NULL AND temperature_c IS NOT NULL AND is_late IS NOT NULL AND source_object IS NOT NULL AND source_row IS NOT NULL AND source_sha256 IS NOT NULL),count(*) FROM curated
UNION ALL SELECT 'after','Q_UNIQUE','All curated rows',count(DISTINCT record_id),count(*) FROM curated
UNION ALL SELECT 'after','Q_VALUE','All curated rows',count(*) FILTER (WHERE coalesce(isfinite(temperature_c) AND temperature_c BETWEEN (SELECT lo FROM cfg) AND (SELECT hi FROM cfg),false)),count(*) FROM curated
UNION ALL SELECT 'after','Q_SENSOR','All curated rows',count(*) FILTER (WHERE sensor_id IN (SELECT upper(blank(sensor_id)) FROM registry)),count(*) FROM curated
UNION ALL SELECT 'after','Q_TIME','All curated rows',count(*) FILTER (WHERE coalesce(event_time_utc<=ingest_time_utc AND ingest_time_utc<=(SELECT as_of FROM cfg),false)),count(*) FROM curated
UNION ALL SELECT 'after','Q_TIMELY','Chronologically valid curated rows only',count(*) FILTER (WHERE event_time_utc<=ingest_time_utc AND ingest_time_utc<=(SELECT as_of FROM cfg) AND date_diff('second',event_time_utc,ingest_time_utc)<=(SELECT late_seconds FROM cfg)),count(*) FILTER (WHERE event_time_utc<=ingest_time_utc AND ingest_time_utc<=(SELECT as_of FROM cfg)) FROM curated
UNION ALL SELECT 'after','Q_QA_COVERAGE','All supplied reference IDs',count(DISTINCT c.record_id),(SELECT count(*) FROM qa_reference) FROM qa_reference q LEFT JOIN curated c USING(record_id)
UNION ALL SELECT 'after','Q_QA_AGREEMENT','Matched supplied reference IDs only',count(*) FILTER (WHERE abs(c.temperature_c-cast(q.reference_c AS DECIMAL(8,2)))<=0.05),count(*) FROM qa_reference q JOIN curated c USING(record_id);
