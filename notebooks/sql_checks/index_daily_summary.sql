SELECT location_id,
       DATE(ts_utc) AS date_utc,
       COUNT(*) AS rows,
       MIN(aqi) AS min_aqi,
       MAX(aqi) AS max_aqi,
       MAX(dominant_pollutant) AS dominant_sample
FROM hadoop_catalog.aq.silver.aq_index_hourly
WHERE ts_utc BETWEEN TIMESTAMP '2024-01-01 00:00:00'
                AND TIMESTAMP '2025-09-01 23:00:00'
GROUP BY location_id, DATE(ts_utc)
ORDER BY location_id, date_utc;
