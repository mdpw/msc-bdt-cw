-- PostgreSQL Database Schema for Traffic Detector System

-- Table 1: Raw sensor data
CREATE TABLE IF NOT EXISTS sensor_data (
    id SERIAL PRIMARY KEY,
    detector_id VARCHAR(50),
    detector_type VARCHAR(50),
    detector_status VARCHAR(50),
    detector_direction TEXT,
    detector_movement TEXT,
    location_name TEXT,
    location_latitude FLOAT,
    location_longitude FLOAT,
    stream_timestamp TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for faster queries on detector_id
CREATE INDEX IF NOT EXISTS idx_sensor_data_detector_id ON sensor_data(detector_id);
CREATE INDEX IF NOT EXISTS idx_sensor_data_timestamp ON sensor_data(stream_timestamp);

-- Table 2: Hourly metrics
CREATE TABLE IF NOT EXISTS hourly_metrics (
    id SERIAL PRIMARY KEY,
    detector_id VARCHAR(50),
    hour_timestamp TIMESTAMP,
    vehicle_count INT,
    avg_count FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(detector_id, hour_timestamp)
);

-- Index for faster queries
CREATE INDEX IF NOT EXISTS idx_hourly_detector_id ON hourly_metrics(detector_id);
CREATE INDEX IF NOT EXISTS idx_hourly_timestamp ON hourly_metrics(hour_timestamp);

-- Table 3: Daily metrics
CREATE TABLE IF NOT EXISTS daily_metrics (
    id SERIAL PRIMARY KEY,
    date DATE,
    peak_detector_id VARCHAR(50),
    peak_volume INT,
    total_volume INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date)
);

-- Index for faster queries
CREATE INDEX IF NOT EXISTS idx_daily_date ON daily_metrics(date);

-- Table 4: Sensor availability
CREATE TABLE IF NOT EXISTS sensor_availability (
    id SERIAL PRIMARY KEY,
    detector_id VARCHAR(50),
    availability_percent FLOAT,
    last_seen TIMESTAMP,
    status VARCHAR(20),
    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for faster queries
CREATE INDEX IF NOT EXISTS idx_availability_detector_id ON sensor_availability(detector_id);
CREATE INDEX IF NOT EXISTS idx_availability_checked_at ON sensor_availability(checked_at);

-- Useful views for Grafana

-- View 1: Latest sensor status
CREATE OR REPLACE VIEW latest_sensor_status AS
SELECT DISTINCT ON (detector_id)
    detector_id,
    detector_type,
    detector_status,
    location_name,
    location_latitude,
    location_longitude,
    stream_timestamp
FROM sensor_data
ORDER BY detector_id, stream_timestamp DESC;

-- View 2: Hourly traffic summary
CREATE OR REPLACE VIEW hourly_traffic_summary AS
SELECT 
    hour_timestamp,
    COUNT(DISTINCT detector_id) as active_sensors,
    SUM(vehicle_count) as total_vehicles,
    AVG(avg_count) as avg_vehicles_per_sensor
FROM hourly_metrics
GROUP BY hour_timestamp
ORDER BY hour_timestamp DESC;

-- View 3: Current sensor availability
CREATE OR REPLACE VIEW current_sensor_availability AS
SELECT DISTINCT ON (detector_id)
    detector_id,
    availability_percent,
    status,
    last_seen,
    checked_at
FROM sensor_availability
ORDER BY detector_id, checked_at DESC;

-- Grant permissions (adjust username as needed)
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO your_username;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO your_username;