-- Apply unchanged to either pinned A or B table.
SELECT site, COUNT(*) AS n_observations,
       CAST(SUM(temperature_c) AS DECIMAL(18,2)) AS sum_temperature_c,
       CAST(SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS BIGINT) AS n_late
FROM benchmark_layout
WHERE event_date = DATE '2026-02-06'
GROUP BY site;
