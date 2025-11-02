-- Use commands in kafka-clean-up-commands.sh file first
-- Clear PostgreSQL Database Tables

-- Delete all records from the 3 metrics tables
TRUNCATE TABLE hourly_vehicle_metrics;
TRUNCATE TABLE daily_peak_metrics;
TRUNCATE TABLE sensor_availability_metrics;

-- Verify tables are empty
SELECT COUNT(*) FROM hourly_vehicle_metrics;
SELECT COUNT(*) FROM daily_peak_metrics; 
SELECT COUNT(*) FROM sensor_availability_metrics;