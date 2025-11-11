--Hourly Traffic Volume by Sensor
SELECT 
    (date + INTERVAL '1 hour' * hour) AS time,
    CONCAT('Sensor ', atd_device_id) AS metric,  -- Better labeling
    total_hourly_volume AS value
FROM hourly_sensor_metrics 
WHERE date + INTERVAL '1 hour' * hour >= $__timeFrom()
    AND date + INTERVAL '1 hour' * hour <= $__timeTo()
ORDER BY time, atd_device_id;


--Daily Peak vs Daily Average Volumes
SELECT 
    date AS time,
    peak_hour_volume AS "Daily Peak Volume",
    ROUND(total_daily_volume / 24.0, 2) AS "Daily Average Volume"--,
    --total_daily_volume AS "Total Daily Volume"
FROM daily_peak_metrics 
WHERE date >= (SELECT MAX(date) - INTERVAL '30 days' FROM daily_peak_metrics)
ORDER BY date;