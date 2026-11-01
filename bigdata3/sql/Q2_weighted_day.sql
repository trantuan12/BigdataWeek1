SELECT event_date, SUM(n_observations) AS n_observations,
                          CAST(SUM(sum_temperature_c)/SUM(n_observations) AS DECIMAL(18,6)) AS weighted_mean_c,
                          CAST(AVG(mean_temperature_c) AS DECIMAL(18,6)) AS naive_mean_of_means_c
                          FROM gold GROUP BY event_date ORDER BY event_date;
