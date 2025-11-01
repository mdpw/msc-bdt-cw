-- Traffic Metrics Database Setup Script
-- Run this script to create the required tables for traffic metrics storage

-- Create database (run this as superuser if database doesn't exist)
-- CREATE DATABASE "traffic-sensor";

-- Connect to the traffic-sensor database before running the rest

-- =====================================================
-- Table 1: Hourly Vehicle Metrics
-- =====================================================
CREATE TABLE IF NOT EXISTS hourly_vehicle_metrics (
    id SERIAL PRIMARY KEY,
    hour_timestamp TIMESTAMP NOT NULL,
    sensor_id VARCHAR(50) NOT NULL,
    vehicle_count INTEGER NOT NULL,
    avg_vehicles_per_hour DECIMAL(10,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(hour_timestamp, sensor_id)
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_hourly_metrics_timestamp ON hourly_vehicle_metrics(hour_timestamp);
CREATE INDEX IF NOT EXISTS idx_hourly_metrics_sensor ON hourly_vehicle_metrics(sensor_id);
CREATE INDEX IF NOT EXISTS idx_hourly_metrics_created ON hourly_vehicle_metrics(created_at);

-- =====================================================
-- Table 2: Daily Peak Metrics
-- =====================================================
CREATE TABLE IF NOT EXISTS daily_peak_metrics (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    total_volume INTEGER NOT NULL,
    peak_sensor_id VARCHAR(50) NOT NULL,
    peak_sensor_volume INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date)
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_daily_metrics_date ON daily_peak_metrics(date);
CREATE INDEX IF NOT EXISTS idx_daily_metrics_peak_sensor ON daily_peak_metrics(peak_sensor_id);
CREATE INDEX IF NOT EXISTS idx_daily_metrics_created ON daily_peak_metrics(created_at);

-- =====================================================
-- Table 3: Sensor Availability Metrics
-- =====================================================
CREATE TABLE IF NOT EXISTS sensor_availability_metrics (
    id SERIAL PRIMARY KEY,
    measurement_timestamp TIMESTAMP NOT NULL,
    sensor_id VARCHAR(50) NOT NULL,
    availability_percentage DECIMAL(5,2) NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('OK', 'DEGRADED', 'DOWN')),
    last_seen TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_availability_timestamp ON sensor_availability_metrics(measurement_timestamp);
CREATE INDEX IF NOT EXISTS idx_availability_sensor ON sensor_availability_metrics(sensor_id);
CREATE INDEX IF NOT EXISTS idx_availability_status ON sensor_availability_metrics(status);
CREATE INDEX IF NOT EXISTS idx_availability_created ON sensor_availability_metrics(created_at);

-- =====================================================
-- Sample Queries for Data Analysis
-- =====================================================

-- Query 1: Get hourly vehicle count for a specific sensor
/*
SELECT 
    hour_timestamp,
    vehicle_count,
    avg_vehicles_per_hour
FROM hourly_vehicle_metrics 
WHERE sensor_id = '12345' 
    AND hour_timestamp >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY hour_timestamp DESC;
*/

-- Query 2: Get daily peak traffic for last 30 days
/*
SELECT 
    date,
    total_volume,
    peak_sensor_id,
    peak_sensor_volume
FROM daily_peak_metrics 
WHERE date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY date DESC;
*/

-- Query 3: Get current sensor availability status
/*
SELECT DISTINCT ON (sensor_id)
    sensor_id,
    availability_percentage,
    status,
    last_seen,
    measurement_timestamp
FROM sensor_availability_metrics 
ORDER BY sensor_id, measurement_timestamp DESC;
*/

-- Query 4: Top 10 busiest sensors (by total vehicle count)
/*
SELECT 
    sensor_id,
    SUM(vehicle_count) as total_vehicles,
    COUNT(*) as hours_recorded,
    AVG(avg_vehicles_per_hour) as avg_hourly_rate
FROM hourly_vehicle_metrics 
WHERE hour_timestamp >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY sensor_id 
ORDER BY total_vehicles DESC 
LIMIT 10;
*/

-- Query 5: Sensor availability summary
/*
SELECT 
    status,
    COUNT(*) as sensor_count,
    AVG(availability_percentage) as avg_availability
FROM (
    SELECT DISTINCT ON (sensor_id)
        sensor_id,
        availability_percentage,
        status
    FROM sensor_availability_metrics 
    ORDER BY sensor_id, measurement_timestamp DESC
) latest_status
GROUP BY status
ORDER BY avg_availability DESC;
*/

-- =====================================================
-- Data Retention (Optional - Run periodically)
-- =====================================================

-- Keep only last 90 days of hourly data
/*
DELETE FROM hourly_vehicle_metrics 
WHERE hour_timestamp < CURRENT_DATE - INTERVAL '90 days';
*/

-- Keep only last 365 days of daily data
/*
DELETE FROM daily_peak_metrics 
WHERE date < CURRENT_DATE - INTERVAL '365 days';
*/

-- Keep only last 30 days of availability data
/*
DELETE FROM sensor_availability_metrics 
WHERE measurement_timestamp < CURRENT_DATE - INTERVAL '30 days';
*/

-- =====================================================
-- Grant permissions (adjust username as needed)
-- =====================================================
/*
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO admin;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO admin;
*/

-- =====================================================
-- Verify tables were created successfully
-- =====================================================
SELECT 
    table_name,
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns 
WHERE table_schema = 'public' 
    AND table_name IN ('hourly_vehicle_metrics', 'daily_peak_metrics', 'sensor_availability_metrics')
ORDER BY table_name, ordinal_position;