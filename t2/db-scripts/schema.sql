-- =====================================================
-- DROP AND RECREATE TRAFFIC METRICS TABLES
-- Fresh Start Script for Austin Traffic Data Processing
-- =====================================================

-- Connect to traffic-sensor database first
-- \c traffic-sensor;

-- =====================================================
-- DROP EXISTING TABLES (if they exist)
-- =====================================================
DROP TABLE IF EXISTS hourly_vehicle_metrics CASCADE;
DROP TABLE IF EXISTS daily_peak_metrics CASCADE;
DROP TABLE IF EXISTS sensor_availability_metrics CASCADE;

-- =====================================================
-- CREATE TABLE 1: Hourly Vehicle Metrics
-- =====================================================
CREATE TABLE hourly_vehicle_metrics (
    id SERIAL PRIMARY KEY,
    hour_timestamp TIMESTAMP NOT NULL,
    sensor_id VARCHAR(50) NOT NULL,
    vehicle_count INTEGER NOT NULL,
    avg_vehicles_per_hour DECIMAL(10,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(hour_timestamp, sensor_id)
);

-- Create indexes for better performance
CREATE INDEX idx_hourly_metrics_timestamp ON hourly_vehicle_metrics(hour_timestamp);
CREATE INDEX idx_hourly_metrics_sensor ON hourly_vehicle_metrics(sensor_id);
CREATE INDEX idx_hourly_metrics_created ON hourly_vehicle_metrics(created_at);

-- =====================================================
-- CREATE TABLE 2: Daily Peak Metrics
-- =====================================================
CREATE TABLE daily_peak_metrics (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    total_volume INTEGER NOT NULL,
    peak_sensor_id VARCHAR(50) NOT NULL,
    peak_sensor_volume INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date)
);

-- Create indexes for better performance
CREATE INDEX idx_daily_metrics_date ON daily_peak_metrics(date);
CREATE INDEX idx_daily_metrics_peak_sensor ON daily_peak_metrics(peak_sensor_id);
CREATE INDEX idx_daily_metrics_created ON daily_peak_metrics(created_at);

-- =====================================================
-- CREATE TABLE 3: Sensor Availability Metrics
-- =====================================================
CREATE TABLE sensor_availability_metrics (
    id SERIAL PRIMARY KEY,
    measurement_timestamp TIMESTAMP NOT NULL,
    sensor_id VARCHAR(50) NOT NULL,
    availability_percentage DECIMAL(5,2) NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('OK', 'DEGRADED', 'DOWN')),
    last_seen TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX idx_availability_timestamp ON sensor_availability_metrics(measurement_timestamp);
CREATE INDEX idx_availability_sensor ON sensor_availability_metrics(sensor_id);
CREATE INDEX idx_availability_status ON sensor_availability_metrics(status);
CREATE INDEX idx_availability_created ON sensor_availability_metrics(created_at);

-- =====================================================
-- GRANT PERMISSIONS
-- =====================================================
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO admin;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO admin;

-- =====================================================
-- VERIFY TABLES CREATED SUCCESSFULLY
-- =====================================================
SELECT 
    'TABLES CREATED SUCCESSFULLY' as status,
    COUNT(*) as table_count
FROM information_schema.tables 
WHERE table_schema = 'public' 
    AND table_name IN ('hourly_vehicle_metrics', 'daily_peak_metrics', 'sensor_availability_metrics');

-- Show table structures
SELECT 
    table_name,
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns 
WHERE table_schema = 'public' 
    AND table_name IN ('hourly_vehicle_metrics', 'daily_peak_metrics', 'sensor_availability_metrics')
ORDER BY table_name, ordinal_position;

-- =====================================================
-- VERIFICATION QUERIES
-- =====================================================
-- Check all tables are empty (should return 0 for all)
SELECT 'hourly_vehicle_metrics' as table_name, COUNT(*) as record_count FROM hourly_vehicle_metrics
UNION ALL
SELECT 'daily_peak_metrics' as table_name, COUNT(*) as record_count FROM daily_peak_metrics
UNION ALL
SELECT 'sensor_availability_metrics' as table_name, COUNT(*) as record_count FROM sensor_availability_metrics;

-- Show confirmation message
SELECT 'DATABASE FRESH START COMPLETE - ALL TABLES RECREATED' as message;