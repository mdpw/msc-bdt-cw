--Daily Peak vs Daily Average Volumes
SELECT 
    date AS time,
    peak_hour_volume AS "Daily Peak Volume",
    ROUND(total_daily_volume / 24.0, 2) AS "Daily Average Volume"--,
    --total_daily_volume AS "Total Daily Volume"
FROM daily_peak_metrics 
WHERE date >= (SELECT MAX(date) - INTERVAL '30 days' FROM daily_peak_metrics)
ORDER BY date;

--Over all Sensor Health - Today
SELECT 
    COUNT(*) AS "Total Sensors",
    COUNT(CASE WHEN status = 'EXCELLENT' THEN 1 END) AS "Excellent",
    COUNT(CASE WHEN status = 'GOOD' THEN 1 END) AS "Good", 
    COUNT(CASE WHEN status = 'POOR' THEN 1 END) AS "Poor",
    COUNT(CASE WHEN status = 'CRITICAL' THEN 1 END) AS "Critical",
    COUNT(CASE WHEN availability_percentage >= 95 THEN 1 END) AS "Healthy (≥95%)",
    COUNT(CASE WHEN availability_percentage < 80 THEN 1 END) AS "Needs Attention (<80%)"
FROM sensor_availability_metrics 
WHERE date = (SELECT MAX(date) FROM sensor_availability_metrics);

--Hourly average vehicle count per sensor - Last 3 Days
SELECT 
    (date + INTERVAL '1 hour' * hour) AS time,
    CONCAT('Sensor ', atd_device_id) AS metric,
    average_per_record AS value
FROM hourly_sensor_metrics 
WHERE date BETWEEN $__timeFrom()::date AND $__timeTo()::date
ORDER BY time, atd_device_id;

--Sensor Availability - Today
SELECT 
    atd_device_id AS "Sensor ID",
    COALESCE(intersection_name, 'Unknown') AS "Location",
    availability_percentage AS "Availability %",
    status AS "Status",
    actual_hours AS "Active Hours",
    expected_hours AS "Expected Hours",
    (expected_hours - actual_hours) AS "Missing Hours",
    first_seen_hour AS "First Active",
    last_seen_hour AS "Last Active",
    CASE 
        WHEN availability_percentage >= 95 THEN 'Excellent'
        WHEN availability_percentage >= 85 THEN 'Good' 
        WHEN availability_percentage >= 70 THEN 'Poor'
        ELSE 'Critical'
    END AS "Health Status"
FROM sensor_availability_metrics 
WHERE date = (SELECT MAX(date) FROM sensor_availability_metrics)
ORDER BY availability_percentage DESC, atd_device_id;