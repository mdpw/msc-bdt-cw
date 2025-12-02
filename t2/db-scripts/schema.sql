-- =====================================================
-- DROP AND RECREATE CAMERA TRAFFIC METRICS TABLES
-- Complete Schema with Proper Permissions
-- =====================================================

-- =====================================================
-- DROP EXISTING TABLES (if they exist)
-- =====================================================
DROP TABLE IF EXISTS hourly_sensor_metrics CASCADE;
DROP TABLE IF EXISTS daily_peak_metrics CASCADE;
DROP TABLE IF EXISTS sensor_availability_metrics CASCADE;

-- =====================================================
-- CREATE TABLE 1: Hourly Sensor Metrics
-- =====================================================
CREATE TABLE hourly_sensor_metrics (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    hour INTEGER NOT NULL CHECK (hour >= 0 AND hour <= 23),
    atd_device_id BIGINT NOT NULL,
    intersection_name VARCHAR(500),
    total_hourly_volume INTEGER DEFAULT 0,
    record_count INTEGER DEFAULT 0,
    average_per_record DECIMAL(10,2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date, hour, atd_device_id)
);

CREATE INDEX idx_hourly_metrics_date_hour ON hourly_sensor_metrics(date, hour);
CREATE INDEX idx_hourly_metrics_device ON hourly_sensor_metrics(atd_device_id);
CREATE INDEX idx_hourly_metrics_intersection ON hourly_sensor_metrics(intersection_name);
CREATE INDEX idx_hourly_metrics_created ON hourly_sensor_metrics(created_at);

-- =====================================================
-- CREATE TABLE 2: Daily Peak Metrics
-- =====================================================
CREATE TABLE daily_peak_metrics (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    peak_hour INTEGER CHECK (peak_hour >= 0 AND peak_hour <= 23),
    peak_hour_volume INTEGER DEFAULT 0,
    peak_sensor_id BIGINT,
    peak_sensor_volume INTEGER DEFAULT 0,
    total_daily_volume INTEGER DEFAULT 0,
    active_sensors_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_daily_metrics_date ON daily_peak_metrics(date);
CREATE INDEX idx_daily_metrics_peak_hour ON daily_peak_metrics(peak_hour);
CREATE INDEX idx_daily_metrics_peak_sensor ON daily_peak_metrics(peak_sensor_id);
CREATE INDEX idx_daily_metrics_created ON daily_peak_metrics(created_at);

-- =====================================================
-- CREATE TABLE 3: Sensor Availability Metrics
-- =====================================================
CREATE TABLE sensor_availability_metrics (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    atd_device_id BIGINT NOT NULL,
    intersection_name VARCHAR(500),
    expected_hours INTEGER DEFAULT 24,
    actual_hours INTEGER DEFAULT 0,
    availability_percentage DECIMAL(5,2) DEFAULT 0,
    status VARCHAR(20) DEFAULT 'UNKNOWN' CHECK (status IN ('EXCELLENT', 'GOOD', 'POOR', 'CRITICAL', 'UNKNOWN')),
    first_seen_hour INTEGER CHECK (first_seen_hour >= 0 AND first_seen_hour <= 23),
    last_seen_hour INTEGER CHECK (last_seen_hour >= 0 AND last_seen_hour <= 23),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date, atd_device_id)
);

CREATE INDEX idx_availability_date ON sensor_availability_metrics(date);
CREATE INDEX idx_availability_device ON sensor_availability_metrics(atd_device_id);
CREATE INDEX idx_availability_status ON sensor_availability_metrics(status);
CREATE INDEX idx_availability_percentage ON sensor_availability_metrics(availability_percentage);
CREATE INDEX idx_availability_created ON sensor_availability_metrics(created_at);

-- =====================================================
-- CREATE GRAFANA USER AND GRANT PERMISSIONS
-- IMPORTANT: User creation MUST come AFTER table creation
-- =====================================================

-- Drop and recreate the grafana user (if exists)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grafana') THEN
        REASSIGN OWNED BY grafana TO admin;
        DROP OWNED BY grafana CASCADE;
        DROP USER grafana;
    END IF;
END
$$;

-- Create grafana user with password
CREATE USER grafana WITH PASSWORD 'grafana';

-- Grant database connection permission
GRANT CONNECT ON DATABASE "traffic-sensor" TO grafana;

-- Grant schema usage permission
GRANT USAGE ON SCHEMA public TO grafana;

-- Grant SELECT permissions on all three tables
GRANT SELECT ON TABLE hourly_sensor_metrics TO grafana;
GRANT SELECT ON TABLE daily_peak_metrics TO grafana;
GRANT SELECT ON TABLE sensor_availability_metrics TO grafana;

-- Grant SELECT on all sequences (for id columns, useful for future)
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO grafana;

-- Set default privileges for any future tables created by admin
ALTER DEFAULT PRIVILEGES FOR ROLE admin IN SCHEMA public GRANT SELECT ON TABLES TO grafana;

-- =====================================================
-- VERIFICATION QUERIES (Optional - Run After Schema)
-- =====================================================

-- Verify tables were created
SELECT 
    tablename, 
    schemaname 
FROM pg_tables 
WHERE schemaname = 'public' 
    AND tablename IN ('hourly_sensor_metrics', 'daily_peak_metrics', 'sensor_availability_metrics')
ORDER BY tablename;

-- Verify grafana user permissions
SELECT 
    grantee, 
    table_name, 
    privilege_type
FROM information_schema.role_table_grants 
WHERE grantee = 'grafana' 
    AND table_schema = 'public'
ORDER BY table_name, privilege_type;

-- Verify indexes were created
SELECT 
    tablename, 
    indexname, 
    indexdef
FROM pg_indexes 
WHERE schemaname = 'public' 
    AND tablename IN ('hourly_sensor_metrics', 'daily_peak_metrics', 'sensor_availability_metrics')
ORDER BY tablename, indexname;