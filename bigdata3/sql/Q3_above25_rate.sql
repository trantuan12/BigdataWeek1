SELECT site,event_date,n_observations,n_above_25,
                         CAST(n_above_25/n_observations AS DECIMAL(18,6)) AS above25_rate
                         FROM gold ORDER BY event_date,above25_rate DESC,site;
