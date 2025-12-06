--Daily Peak vs Daily Average Volumes (Select the date period for one day)
SELECT 
    date AS time,
    peak_hour_volume AS "Daily Peak Volume",
    ROUND(total_daily_volume::numeric / 24.0, 2) AS "Daily Average Volume"
FROM daily_peak_metrics 
WHERE date BETWEEN $__timeFrom()::date AND $__timeTo()::date
ORDER BY date;

--Over all Sensor Health (Select the date period for one day)
SELECT 
    COUNT(*) AS "Total Sensors",
    COUNT(CASE WHEN status = 'EXCELLENT' THEN 1 END) AS "Excellent",
    COUNT(CASE WHEN status = 'GOOD' THEN 1 END) AS "Good", 
    COUNT(CASE WHEN status = 'POOR' THEN 1 END) AS "Poor",
    COUNT(CASE WHEN status = 'CRITICAL' THEN 1 END) AS "Critical",
    COUNT(CASE WHEN availability_percentage >= 95 THEN 1 END) AS "Healthy (≥95%)",
    COUNT(CASE WHEN availability_percentage < 80 THEN 1 END) AS "Needs Attention (<80%)"
FROM sensor_availability_metrics 
WHERE date BETWEEN $__timeFrom()::date AND $__timeTo()::date

--Hourly average vehicle count per sensor (Select the date period for one day)
SELECT 
    (date + INTERVAL '1 hour' * hour) AS time,
    CONCAT('Sensor ', atd_device_id) AS metric,
    average_per_record AS value
FROM hourly_sensor_metrics 
WHERE date BETWEEN $__timeFrom()::date AND $__timeTo()::date
ORDER BY time, atd_device_id;

--Sensor Availability (Select the date period for one day)
SELECT 
    atd_device_id AS "Sensor ID",
    COALESCE(intersection_name, 'Unknown') AS "Location",
    availability_percentage AS "Availability %",
    status AS "Status",
    actual_hours AS "Actual Active Hours",
    expected_hours AS "Expected Active Hours",
    (expected_hours - actual_hours) AS "Missing Hours",
    first_seen_hour AS "First Active Hour",
    last_seen_hour AS "Last Active Hour"
FROM sensor_availability_metrics 
WHERE date BETWEEN $__timeFrom()::date AND $__timeTo()::date
ORDER BY availability_percentage DESC, atd_device_id;