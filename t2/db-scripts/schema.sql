-- =====================================================
-- DROP AND RECREATE CAMERA TRAFFIC METRICS TABLES
-- Minimal Schema - Tables with Indexes Only
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