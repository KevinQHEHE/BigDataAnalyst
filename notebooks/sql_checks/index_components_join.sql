SELECT c.location_id,
       c.ts_utc,
       c.pm25,
       cmp.pm25_24h_avg,
       cmp.o3_8h_max,
       idx.aqi,
       idx.dominant_pollutant
FROM hadoop_catalog.aq.silver.air_quality_hourly_clean c
JOIN hadoop_catalog.aq.silver.aq_components_hourly cmp
  ON c.location_id = cmp.location_id AND c.ts_utc = cmp.ts_utc
JOIN hadoop_catalog.aq.silver.aq_index_hourly idx
  ON c.location_id = idx.location_id AND c.ts_utc = idx.ts_utc
WHERE c.location_id = 'Hà Nội'
  AND c.ts_utc BETWEEN TIMESTAMP '2024-05-01 00:00:00'
                   AND TIMESTAMP '2024-05-01 12:00:00'
ORDER BY c.ts_utc;
