#!/usr/bin/env python3
"""
Camera Traffic Counts Consumer
Processes camera traffic data and calculates:
1. Hourly average vehicle count per sensor
2. Daily peak traffic volume across all sensors  
3. Daily sensor availability (%) based on data presence
"""

import json
import logging
import yaml
import psycopg2
from collections import defaultdict
from datetime import datetime, timedelta
import threading
import time
from kafka import KafkaConsumer, KafkaProducer

# Load configuration
def load_config():
    try:
        with open('config.yml', 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        print("Error: config.yml not found")
        exit(1)

config = load_config()
logging.basicConfig(
    level=getattr(logging, config['logging']['level']),
    format=config['logging']['format']
)
logger = logging.getLogger(__name__)


class PostgreSQLPersistence:
    """Handle PostgreSQL database operations for metrics"""
    
    def __init__(self, db_config):
        self.config = db_config
        self.conn = None
        self.cursor = None
        self.enabled = db_config.get('enabled', True)
        
        if self.enabled:
            self.connect()            
    
    def connect(self):
        """Connect to PostgreSQL database"""
        try:
            self.conn = psycopg2.connect(
                host=self.config['host'],
                port=self.config['port'],
                database=self.config['database'],
                user=self.config['user'],
                password=self.config['password']
            )
            self.cursor = self.conn.cursor()
            logger.info("Connected to PostgreSQL for metrics persistence")
            
            # Verify tables exist
            self.verify_tables()
            
            return True
        except Exception as e:
            logger.error(f"PostgreSQL connection failed: {e}")
            self.enabled = False
            return False
    
    def verify_tables(self):
        """Verify that required tables exist"""
        try:
            tables_to_check = ['hourly_sensor_metrics', 'daily_peak_metrics', 'sensor_availability_metrics']
            
            for table in tables_to_check:
                self.cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = %s
                    )
                """, (table,))
                
                table_exists = self.cursor.fetchone()[0]
                if table_exists:
                    # Get record count
                    self.cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    record_count = self.cursor.fetchone()[0]
                    logger.info(f"Table {table} exists with {record_count} records")
                else:
                    logger.error(f"Table {table} does not exist! Please run schema.sql first.")
                    
        except Exception as e:
            logger.error(f"Error verifying tables: {e}")
    
    def save_hourly_metrics(self, metrics_data):
        """Save hourly vehicle count per sensor (sum of 15-min intervals)"""
        if not self.enabled:
            logger.warning("PostgreSQL not enabled - skipping hourly metrics save")
            return
        
        if not self.conn:
            logger.error("No PostgreSQL connection - attempting to reconnect")
            if not self.connect():
                return
        
        try:
            logger.debug(f"Saving {len(metrics_data)} hourly metrics to database")
            
            for key, data in metrics_data.items():
                date, hour, device_id = key.split('|')
                
                logger.debug(f"Saving: {date} hour {hour} device {device_id} - volume: {data['total_hourly_volume']}")
                
                self.cursor.execute("""
                    INSERT INTO hourly_sensor_metrics 
                    (date, hour, atd_device_id, intersection_name, total_hourly_volume, record_count, 
                     average_per_record)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (date, hour, atd_device_id) 
                    DO UPDATE SET 
                        total_hourly_volume = EXCLUDED.total_hourly_volume,
                        record_count = EXCLUDED.record_count,
                        average_per_record = EXCLUDED.average_per_record,
                        intersection_name = EXCLUDED.intersection_name
                """, (date, int(hour), int(device_id), data['intersection_name'], 
                      data['total_hourly_volume'], data['record_count'], 
                      data['average_per_record']))
            
            self.conn.commit()
            logger.info(f"Successfully saved {len(metrics_data)} hourly metrics to database")
            
            # Verify the save by checking record count
            self.cursor.execute("SELECT COUNT(*) FROM hourly_sensor_metrics")
            total_records = self.cursor.fetchone()[0]
            logger.info(f"Total records in hourly_sensor_metrics table: {total_records}")
            
        except Exception as e:
            logger.error(f"Error saving hourly metrics: {e}")
            if self.conn:
                self.conn.rollback()
    
    def save_daily_peak_metrics(self, daily_data):
        """Save daily peak traffic volume across all sensors (simplified structure)"""
        if not self.enabled:
            logger.warning("PostgreSQL not enabled - skipping daily peak metrics save")
            return
        
        if not self.conn:
            logger.error("No PostgreSQL connection - attempting to reconnect")
            if not self.connect():
                return
        
        try:
            logger.debug(f"Saving daily peak metrics for {len(daily_data)} dates")
            
            for date, data in daily_data.items():
                logger.debug(f"Saving daily peak for {date}: Hour {data['peak_hour']} ({data['peak_hour_volume']} vehicles)")
                
                self.cursor.execute("""
                    INSERT INTO daily_peak_metrics 
                    (date, peak_hour, peak_hour_volume, peak_sensor_id, peak_sensor_volume, 
                     total_daily_volume, active_sensors_count)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (date) 
                    DO UPDATE SET 
                        peak_hour = EXCLUDED.peak_hour,
                        peak_hour_volume = EXCLUDED.peak_hour_volume,
                        peak_sensor_id = EXCLUDED.peak_sensor_id,
                        peak_sensor_volume = EXCLUDED.peak_sensor_volume,
                        total_daily_volume = EXCLUDED.total_daily_volume,
                        active_sensors_count = EXCLUDED.active_sensors_count
                """, (date, data['peak_hour'], data['peak_hour_volume'], 
                      None, None,  # No individual sensor tracking for Interpretation 1
                      data['total_daily_volume'], data.get('active_sensors_count', 0)))
            
            self.conn.commit()
            logger.info(f"Successfully saved daily peak metrics for {len(daily_data)} dates")
            
            # Verify the save
            for date in daily_data.keys():
                self.cursor.execute("SELECT peak_hour, peak_hour_volume FROM daily_peak_metrics WHERE date = %s", (date,))
                result = self.cursor.fetchone()
                if result:
                    logger.debug(f"Verified: {date} peak hour {result[0]} with {result[1]} vehicles")
            
        except Exception as e:
            logger.error(f"Error saving daily peak metrics: {e}")
            if self.conn:
                self.conn.rollback()
    
    def save_availability_metrics(self, availability_data):
        """Save daily sensor availability metrics (based on hourly presence)"""
        if not self.enabled:
            logger.warning("PostgreSQL not enabled - skipping availability metrics save")
            return
        
        if not self.conn:
            logger.error("No PostgreSQL connection - attempting to reconnect")
            if not self.connect():
                return
        
        try:
            logger.debug(f"Saving availability metrics for {len(availability_data)} sensor-date combinations")
            
            for key, data in availability_data.items():
                date, device_id = key.split('|')
                
                logger.debug(f"Saving availability for {date} device {device_id}: {data['availability_percentage']}% ({data['status']})")
                
                self.cursor.execute("""
                    INSERT INTO sensor_availability_metrics 
                    (date, atd_device_id, intersection_name, expected_hours, actual_hours, 
                     availability_percentage, status, first_seen_hour, last_seen_hour)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (date, atd_device_id) 
                    DO UPDATE SET 
                        intersection_name = EXCLUDED.intersection_name,
                        expected_hours = EXCLUDED.expected_hours,
                        actual_hours = EXCLUDED.actual_hours,
                        availability_percentage = EXCLUDED.availability_percentage,
                        status = EXCLUDED.status,
                        first_seen_hour = EXCLUDED.first_seen_hour,
                        last_seen_hour = EXCLUDED.last_seen_hour
                """, (date, int(device_id), data['intersection_name'], data.get('expected_hours', 24),
                      data['actual_hours'], data['availability_percentage'], data['status'],
                      data['first_seen_hour'], data['last_seen_hour']))
            
            self.conn.commit()
            logger.info(f"Successfully saved availability metrics for {len(availability_data)} sensor-date combinations")
            
            # Verify critical availability issues
            self.cursor.execute("""
                SELECT COUNT(*) FROM sensor_availability_metrics 
                WHERE date = CURRENT_DATE AND status = 'CRITICAL'
            """)
            critical_count = self.cursor.fetchone()[0]
            if critical_count > 0:
                logger.warning(f"Alert: {critical_count} sensors have CRITICAL availability today")
            
        except Exception as e:
            logger.error(f"Error saving availability metrics: {e}")
            if self.conn:
                self.conn.rollback()
    
    def close(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("PostgreSQL connection closed")


class CameraTrafficMetrics:
    """Calculate real-time rolling metrics for camera traffic data"""
    
    def __init__(self, kafka_producer=None, postgres_db=None, daily_metrics_batch_size=50, db_save_frequency=25):
        # Real-time rolling metrics data structures
        # Key: "date|hour|device_id" -> {total_volume, record_count, intersection_name}
        self.hourly_running_totals = defaultdict(lambda: {
            'total_volume': 0, 
            'record_count': 0,
            'intersection_name': None
        })
        
        # Daily peak tracking (updates with each record)
        # Key: "date|hour" -> total_volume_for_that_hour (sum across all sensors)
        self.hourly_totals = defaultdict(int)
        # Key: "date|device_id" -> total_daily_volume_for_sensor
        self.daily_sensor_totals = defaultdict(int)
        
        # Sensor availability tracking
        # Key: "date|device_id" -> set of hours when sensor was active
        self.sensor_daily_activity = defaultdict(set)
        self.sensor_intersection_names = {}  # device_id -> intersection_name
        
        # Output handlers
        self.kafka_producer = kafka_producer
        self.postgres_db = postgres_db
        
        # Configurable processing frequencies
        self.daily_metrics_batch_size = daily_metrics_batch_size
        self.db_save_frequency = db_save_frequency
        
        # Track completed hours to avoid duplicate saves
        self.completed_hours = set()
        
        # Record processing stats
        self.total_records_processed = 0
        self.last_stats_time = time.time()
        
    def extract_date_time_info(self, record):
        """Extract date and time information from producer record"""
        try:
            # Producer outputs these exact column names
            year = record.get('Year', 2024)
            month = record.get('Month', 1) 
            day = record.get('Day', 1)
            hour = record.get('Hour', 0)
            
            # Create date and hour for aggregation
            date = f"{year}-{month:02d}-{day:02d}"
            
            return {
                'date': date,
                'hour': hour,
                'year': year,
                'month': month,
                'day': day
            }
        except Exception as e:
            logger.error(f"Error extracting date/time from record: {e}")
            return None
    
    def process_record(self, record):
        """Process individual record and update rolling averages"""
        try:
            self.total_records_processed += 1
            
            # Extract date and time information
            time_info = self.extract_date_time_info(record)
            if not time_info:
                return
            
            # Extract key fields using exact producer column names
            device_id = str(record.get('ATD Device ID', 'unknown'))
            intersection_name = record.get('Intersection Name', 'UNKNOWN')
            volume = int(record.get('Volume', 0))
            
            date = time_info['date']
            hour = time_info['hour']
            
            # Update intersection name mapping
            self.sensor_intersection_names[device_id] = intersection_name
            
            # Key for hourly aggregation
            hourly_key = f"{date}|{hour}|{device_id}"
            
            # Update hourly running totals (rolling average) - REAL-TIME
            hourly_data = self.hourly_running_totals[hourly_key]
            hourly_data['total_volume'] += volume
            hourly_data['record_count'] += 1
            hourly_data['intersection_name'] = intersection_name
            hourly_data['total_hourly_volume'] = hourly_data['total_volume']
            hourly_data['average_per_record'] = hourly_data['total_volume'] / hourly_data['record_count']
            
            # Update daily sensor activity tracking (for batch processing)
            activity_key = f"{date}|{device_id}"
            self.sensor_daily_activity[activity_key].add(hour)
            
            # Update hourly totals (sum across all sensors for peak detection)
            hour_total_key = f"{date}|{hour}"
            self.hourly_totals[hour_total_key] += volume
            
            # Update daily sensor totals
            daily_sensor_key = f"{date}|{device_id}"
            self.daily_sensor_totals[daily_sensor_key] += volume
            
            # 1. REAL-TIME CALCULATION: Update hourly rolling average immediately (most accurate)
            # But don't publish yet - wait for batch
            
            # 2. BATCH: Process and publish ALL metrics together every N records (synchronized)
            if self.total_records_processed % self.daily_metrics_batch_size == 0:
                self.process_batch_all_metrics()
                self.save_current_metrics_to_db()
                self.log_processing_stats()
                
        except Exception as e:
            logger.error(f"Error processing record: {e}")
    
    def process_batch_all_metrics(self):
        """Process and publish ALL metrics together every batch (synchronized publishing)"""
        try:
            logger.debug("Processing batch - all metrics together...")
            
            # Get all active dates from current data
            active_dates = set()
            for key in self.hourly_totals.keys():
                date = key.split('|')[0]
                active_dates.add(date)
            
            # 1. Publish current hourly rolling averages (calculated real-time, published in batch)
            self.publish_batch_hourly_averages()
            
            # 2. Calculate and publish daily peaks
            for date in active_dates:
                self.calculate_and_publish_daily_peaks(date)
                
            # 3. Calculate and publish availability for all sensors
            for date in active_dates:
                self.calculate_and_publish_availability_for_date(date)
            
            logger.debug(f"Completed batch processing for all metrics on {len(active_dates)} dates")
            
        except Exception as e:
            logger.error(f"Error processing batch metrics: {e}")
    
    def publish_batch_hourly_averages(self):
        """Publish current state of all hourly rolling averages (calculated real-time, published in batch)"""
        try:
            if not self.kafka_producer:
                return
            
            published_count = 0
            for hourly_key, hourly_data in self.hourly_running_totals.items():
                date, hour, device_id = hourly_key.split('|')
                
                rolling_update = {
                    'timestamp': datetime.now().isoformat(),
                    'date': date,
                    'hour': int(hour),
                    'device_id': device_id,
                    'intersection_name': hourly_data['intersection_name'],
                    'current_total_volume': hourly_data['total_volume'],
                    'current_record_count': hourly_data['record_count'],
                    'current_average_per_record': hourly_data['average_per_record'],
                    'metric_type': 'batch_hourly_rolling_average',
                    'update_frequency': 'every_batch'
                }
                
                self.kafka_producer.send('hourly-rolling-metrics', rolling_update)
                published_count += 1
            
            logger.debug(f"Published {published_count} hourly rolling averages in batch")
            
        except Exception as e:
            logger.error(f"Error publishing batch hourly averages: {e}")
    
    def calculate_and_publish_daily_peaks(self, target_date):
        """Calculate and publish daily peak HOUR across all sensors for a specific date"""
        try:
            # Get hourly totals for target date (sum across ALL sensors per hour)
            date_hourly_totals = {}
            
            for key, volume in self.hourly_totals.items():
                date, hour = key.split('|')
                if date == target_date:
                    date_hourly_totals[int(hour)] = volume
            
            if date_hourly_totals:
                # Find the PEAK HOUR (hour with highest total volume across all sensors)
                peak_hour = max(date_hourly_totals.keys(), key=lambda h: date_hourly_totals[h])
                peak_hour_volume = date_hourly_totals[peak_hour]
                
                # Calculate additional stats
                total_daily_volume = sum(date_hourly_totals.values())
                hours_with_data = len(date_hourly_totals)
                average_hourly_volume = total_daily_volume / hours_with_data if hours_with_data > 0 else 0
                
                daily_peak_update = {
                    'timestamp': datetime.now().isoformat(),
                    'date': target_date,
                    'peak_hour': peak_hour,
                    'peak_hour_volume': peak_hour_volume,
                    'peak_hour_description': f"{peak_hour}:00-{peak_hour+1}:00",
                    'total_daily_volume': total_daily_volume,
                    'hours_with_data': hours_with_data,
                    'average_hourly_volume': round(average_hourly_volume, 2),
                    'peak_vs_average_ratio': round(peak_hour_volume / average_hourly_volume, 2) if average_hourly_volume > 0 else 0,
                    'metric_type': 'daily_peak_hour_all_sensors',
                    'update_frequency': 'every_10_seconds'
                }
                
                # Publish to Kafka
                if self.kafka_producer:
                    self.kafka_producer.send('daily-peak-metrics', daily_peak_update)
                
                logger.debug(f"Daily peak hour for {target_date}: Hour {peak_hour}:00 with {peak_hour_volume} vehicles total across all sensors")
                
        except Exception as e:
            logger.error(f"Error calculating daily peak hour for {target_date}: {e}")
    
    def calculate_and_publish_availability_for_date(self, target_date):
        """Calculate and publish availability metrics for all sensors on a specific date"""
        try:
            sensors_for_date = set()
            
            # Find all sensors active on target date
            for key in self.sensor_daily_activity.keys():
                date, device_id = key.split('|')
                if date == target_date:
                    sensors_for_date.add(device_id)
            
            for device_id in sensors_for_date:
                activity_key = f"{target_date}|{device_id}"
                hours_active = self.sensor_daily_activity.get(activity_key, set())
                
                if hours_active:
                    actual_hours = len(hours_active)
                    current_hour = max(hours_active)
                    hours_elapsed_today = current_hour + 1
                    
                    # Calculate availability based on hours elapsed so far
                    availability_percentage = (actual_hours / hours_elapsed_today) * 100
                    
                    # Status classification
                    if availability_percentage >= 90: status = 'EXCELLENT'
                    elif availability_percentage >= 75: status = 'GOOD'
                    elif availability_percentage >= 50: status = 'POOR'
                    else: status = 'CRITICAL'
                    
                    availability_update = {
                        'timestamp': datetime.now().isoformat(),
                        'date': target_date,
                        'device_id': device_id,
                        'intersection_name': self.sensor_intersection_names.get(device_id, 'UNKNOWN'),
                        'hours_elapsed_today': hours_elapsed_today,
                        'actual_hours_active': actual_hours,
                        'availability_percentage': round(availability_percentage, 2),
                        'status': status,
                        'first_seen_hour': min(hours_active),
                        'last_seen_hour': max(hours_active),
                        'gaps_detected': hours_elapsed_today - actual_hours,
                        'metric_type': 'batch_sensor_availability',
                        'update_frequency': 'every_10_seconds'
                    }
                    
                    # Publish to Kafka
                    if self.kafka_producer:
                        self.kafka_producer.send('sensor-availability-metrics', availability_update)
                    
                    # Alert for critical availability
                    if availability_percentage < 50:
                        logger.warning(f"ALERT: Sensor {device_id} availability {availability_percentage:.1f}% "
                                     f"({actual_hours}/{hours_elapsed_today} hours)")
            
            logger.debug(f"Availability calculated for {len(sensors_for_date)} sensors on {target_date}")
            
        except Exception as e:
            logger.error(f"Error calculating availability for {target_date}: {e}")
    
    def save_current_metrics_to_db(self):
        """Save current metrics to database (every 50 records)"""
        if not self.postgres_db:
            return
        
        try:
            # 1. Save current hourly metrics
            hourly_metrics = self.get_current_hourly_metrics()
            if hourly_metrics:
                self.postgres_db.save_hourly_metrics(hourly_metrics)
                logger.debug(f"Saved {len(hourly_metrics)} hourly metrics to database")
            
            # 2. Save current daily peak metrics (create compatible data structure)
            daily_peaks = self.create_daily_peaks_for_db()
            if daily_peaks:
                self.postgres_db.save_daily_peak_metrics(daily_peaks)
                logger.debug(f"Saved {len(daily_peaks)} daily peak metrics to database")
            
            # 3. Save current availability metrics (create compatible data structure)  
            availability_data = self.create_availability_for_db()
            if availability_data:
                self.postgres_db.save_availability_metrics(availability_data)
                logger.debug(f"Saved {len(availability_data)} availability metrics to database")
                
        except Exception as e:
            logger.error(f"Error saving metrics to database: {e}")
    
    def create_daily_peaks_for_db(self):
        """Create daily peak data structure compatible with database save method"""
        daily_peaks = {}
        
        # Get all active dates
        active_dates = set()
        for key in self.hourly_totals.keys():
            date = key.split('|')[0]
            active_dates.add(date)
        
        for date in active_dates:
            # Get hourly totals for this date
            date_hourly_totals = {}
            date_sensor_totals = {}
            
            for key, volume in self.hourly_totals.items():
                date_key, hour = key.split('|')
                if date_key == date:
                    date_hourly_totals[int(hour)] = volume
            
            for key, volume in self.daily_sensor_totals.items():
                date_key, device_id = key.split('|')
                if date_key == date:
                    date_sensor_totals[device_id] = volume
            
            if date_hourly_totals:
                # Calculate peak hour
                peak_hour = max(date_hourly_totals.keys(), key=lambda h: date_hourly_totals[h])
                peak_hour_volume = date_hourly_totals[peak_hour]
                total_daily_volume = sum(date_hourly_totals.values())
                active_sensors_count = len(date_sensor_totals)
                
                daily_peaks[date] = {
                    'peak_hour': peak_hour,
                    'peak_hour_volume': peak_hour_volume,
                    'total_daily_volume': total_daily_volume,
                    'active_sensors_count': active_sensors_count
                }
        
        return daily_peaks
    
    def create_availability_for_db(self):
        """Create availability data structure compatible with database save method"""
        availability_data = {}
        
        for key, hours_set in self.sensor_daily_activity.items():
            date, device_id = key.split('|')
            
            if hours_set:
                actual_hours = len(hours_set)
                current_hour = max(hours_set)
                hours_elapsed_today = current_hour + 1
                
                # Calculate availability based on hours elapsed so far
                availability_percentage = (actual_hours / hours_elapsed_today) * 100
                
                # Status classification
                if availability_percentage >= 90: status = 'EXCELLENT'
                elif availability_percentage >= 75: status = 'GOOD'
                elif availability_percentage >= 50: status = 'POOR'
                else: status = 'CRITICAL'
                
                availability_data[key] = {
                    'intersection_name': self.sensor_intersection_names.get(device_id, 'UNKNOWN'),
                    'expected_hours': hours_elapsed_today,  # Expected = hours elapsed so far
                    'actual_hours': actual_hours,
                    'availability_percentage': round(availability_percentage, 2),
                    'status': status,
                    'first_seen_hour': min(hours_set),
                    'last_seen_hour': max(hours_set)
                }
        
        return availability_data
    
    def force_publish_all_metrics(self):
        """Force publish all current metrics (called on shutdown)"""
        logger.info("Publishing final metrics...")
        self.process_batch_all_metrics()
        self.save_current_metrics_to_db()
        logger.info("Final metrics published successfully")
        
        # Publish all current hourly metrics
        hourly_metrics = self.get_current_hourly_metrics()
        if hourly_metrics and self.postgres_db:
            self.postgres_db.save_hourly_metrics(hourly_metrics)
        
        # Publish daily peak metrics
        daily_peaks = self.calculate_daily_peaks()
        if daily_peaks and self.postgres_db:
            self.postgres_db.save_daily_peak_metrics(daily_peaks)
        
        # Publish availability metrics
        availability_data = self.calculate_availability_metrics()
        if availability_data and self.postgres_db:
            self.postgres_db.save_availability_metrics(availability_data)
        
        logger.info(f"Published final metrics - Hourly: {len(hourly_metrics)}, "
                   f"Daily Peaks: {len(daily_peaks)}, Availability: {len(availability_data)}")
    
    def get_current_hourly_metrics(self):
        """Get current state of hourly metrics"""
        hourly_metrics = {}
        
        for key, data in self.hourly_running_totals.items():
            hourly_metrics[key] = {
                'total_hourly_volume': data['total_volume'],
                'record_count': data['record_count'],
                'average_per_record': data['average_per_record'],
                'intersection_name': data['intersection_name']
            }
        
        return hourly_metrics
    
    def calculate_daily_peaks(self):
        """Calculate daily peak metrics from current data"""
        daily_peaks = {}
        
        # Group hourly totals by date
        date_hourly_totals = defaultdict(dict)
        for key, volume in self.hourly_totals.items():
            date, hour = key.split('|')
            date_hourly_totals[date][int(hour)] = volume
        
        # Group daily sensor totals by date
        date_sensor_totals = defaultdict(dict)
        for key, volume in self.daily_sensor_totals.items():
            date, device_id = key.split('|')
            date_sensor_totals[date][device_id] = volume
        
        # Calculate peaks for each date
        for date in date_hourly_totals.keys():
            hourly_data = date_hourly_totals[date]
            sensor_data = date_sensor_totals.get(date, {})
            
            if hourly_data:
                # Find peak hour
                peak_hour = max(hourly_data.keys(), key=lambda h: hourly_data[h])
                peak_hour_volume = hourly_data[peak_hour]
                
                # Find peak sensor
                peak_sensor_id = max(sensor_data.keys(), key=lambda s: sensor_data[s]) if sensor_data else None
                peak_sensor_volume = sensor_data[peak_sensor_id] if peak_sensor_id else 0
                
                # Calculate totals
                total_daily_volume = sum(sensor_data.values())
                active_sensors_count = len(sensor_data)
                
                daily_peaks[date] = {
                    'peak_hour': peak_hour,
                    'peak_hour_volume': peak_hour_volume,
                    'peak_sensor_id': peak_sensor_id,
                    'peak_sensor_volume': peak_sensor_volume,
                    'total_daily_volume': total_daily_volume,
                    'active_sensors_count': active_sensors_count
                }
        
        return daily_peaks
    
    def calculate_availability_metrics(self):
        """Calculate sensor availability metrics from current data"""
        availability_data = {}
        
        for key, hours_set in self.sensor_daily_activity.items():
            date, device_id = key.split('|')
            
            actual_hours = len(hours_set)
            expected_hours = 24
            availability_percentage = (actual_hours / expected_hours) * 100
            
            # Determine status based on availability
            if availability_percentage >= 90:
                status = 'EXCELLENT'
            elif availability_percentage >= 75:
                status = 'GOOD'
            elif availability_percentage >= 50:
                status = 'POOR'
            else:
                status = 'CRITICAL'
            
            # Get first and last seen hours
            first_seen_hour = min(hours_set) if hours_set else None
            last_seen_hour = max(hours_set) if hours_set else None
            
            availability_data[key] = {
                'intersection_name': self.sensor_intersection_names.get(device_id, 'UNKNOWN'),
                'expected_hours': expected_hours,
                'actual_hours': actual_hours,
                'availability_percentage': round(availability_percentage, 2),
                'status': status,
                'first_seen_hour': first_seen_hour,
                'last_seen_hour': last_seen_hour
            }
        
        return availability_data
    
    def log_processing_stats(self):
        """Log processing statistics"""
        current_time = time.time()
        time_elapsed = current_time - self.last_stats_time
        
        if time_elapsed >= 10:  # Log every 10 seconds
            records_per_sec = self.total_records_processed / (current_time - self.last_stats_time) if time_elapsed > 0 else 0
            
            logger.info(f"PROCESSING STATS:")
            logger.info(f"  Records processed: {self.total_records_processed}")
            logger.info(f"  Rate: {records_per_sec:.1f} records/sec")
            logger.info(f"  Active hourly aggregations: {len(self.hourly_running_totals)}")
            logger.info(f"  Active sensors: {len(self.sensor_intersection_names)}")
            
            self.last_stats_time = current_time


class CameraTrafficConsumer:
    """Main consumer class for camera traffic data"""
    
    def __init__(self):
        # Load configurations
        self.kafka_config = config['kafka']
        self.consumer_config = config['consumer']
        self.postgres_config = config.get('postgres', {})
        
        # Configurable batch processing
        self.daily_metrics_batch_size = self.consumer_config.get('daily_metrics_batch_size', 50)
        # Align database saves with metric calculations (same frequency)
        self.db_save_frequency = self.consumer_config.get('db_save_frequency', self.daily_metrics_batch_size)
        
        # Enable/disable features
        self.enable_kafka_output = self.kafka_config.get('enable_output_topics', True)
        self.enable_postgres = self.postgres_config.get('enabled', True)
        
        # Initialize connections
        self.consumer = None
        self.kafka_producer = None
        self.postgres_db = None
        
        # Initialize output handlers
        self.init_output_handlers()
        
        # Metrics calculator
        self.metrics = CameraTrafficMetrics(
            kafka_producer=self.kafka_producer,
            postgres_db=self.postgres_db,
            daily_metrics_batch_size=self.daily_metrics_batch_size,
            db_save_frequency=self.db_save_frequency
        )
        
    def init_output_handlers(self):
        """Initialize optional output handlers"""
        # Initialize Kafka producer for output topics
        if self.enable_kafka_output:
            try:
                self.kafka_producer = KafkaProducer(
                    bootstrap_servers=self.kafka_config['bootstrap_servers'],
                    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
                )
                logger.info("Kafka producer initialized for output topics")
                logger.info("Output topics: hourly-sensor-metrics, daily-peak-metrics, sensor-availability-metrics")
            except Exception as e:
                logger.error(f"Failed to initialize Kafka producer: {e}")
                self.enable_kafka_output = False
        
        # Initialize PostgreSQL persistence
        if self.enable_postgres:
            try:
                self.postgres_db = PostgreSQLPersistence(self.postgres_config)
                if self.postgres_db.enabled:
                    logger.info("PostgreSQL persistence initialized")
                else:
                    logger.warning("PostgreSQL persistence disabled due to connection issues")
            except Exception as e:
                logger.error(f"Failed to initialize PostgreSQL: {e}")
                self.enable_postgres = False
    
    def process_message(self, message):
        """Process incoming Kafka message with real-time rolling averages"""
        try:
            record = message.value
            # Process record with real-time rolling averages
            self.metrics.process_record(record)
                    
        except Exception as e:
            logger.error(f"Error processing message: {e}")
    
    def connect(self):
        """Connect to Kafka"""
        try:
            self.consumer = KafkaConsumer(
                self.kafka_config['topic'],
                bootstrap_servers=self.kafka_config['bootstrap_servers'],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset=self.consumer_config['auto_offset_reset'],
                enable_auto_commit=self.consumer_config['enable_auto_commit'],
                group_id=self.kafka_config['consumer_group_id']
            )
            logger.info(f"Connected to Kafka: {self.kafka_config['bootstrap_servers']}")
            logger.info(f"Subscribed to topic: {self.kafka_config['topic']}")
            logger.info(f"Consumer group: {self.kafka_config['consumer_group_id']}")
            return True
        except Exception as e:
            logger.error(f"Kafka connection failed: {e}")
            return False
    
    def start_consuming(self):
        """Start consuming messages with real-time rolling average processing"""
        logger.info("=" * 80)
        logger.info("CAMERA TRAFFIC COUNTS CONSUMER - FULLY SYNCHRONIZED PROCESSING")
        logger.info("=" * 80)
        logger.info("CALCULATION vs PUBLISHING:")
        logger.info("1. Hourly rolling averages: Calculate every record, publish every batch")
        logger.info(f"2. Daily peak metrics: Calculate and publish every {self.metrics.daily_metrics_batch_size} records")
        logger.info(f"3. Sensor availability: Calculate and publish every {self.metrics.daily_metrics_batch_size} records")
        logger.info("=" * 80)
        logger.info(f"Input Topic: {self.kafka_config['topic']}")
        
        if self.enable_kafka_output:
            logger.info("Kafka Output Topics (ALL synchronized):")
            logger.info(f"  • hourly-rolling-metrics (every {self.metrics.daily_metrics_batch_size} records)")
            logger.info(f"  • daily-peak-metrics (every {self.metrics.daily_metrics_batch_size} records)")
            logger.info(f"  • sensor-availability-metrics (every {self.metrics.daily_metrics_batch_size} records)")
        else:
            logger.info("Kafka Output: Disabled")
            
        if self.enable_postgres:
            logger.info("PostgreSQL Operations (synchronized):")
            logger.info(f"  • Database saves: Every {self.metrics.daily_metrics_batch_size} records")
            logger.info("  • UPSERT operations for all three tables together")
            logger.info("  • Perfect data consistency across all tables")
        else:
            logger.info("PostgreSQL Persistence: Disabled")
            
        logger.info("FULLY SYNCHRONIZED FLOW:")
        logger.info("  Each record → Calculate hourly average (most accurate)")
        logger.info(f"  Every {self.metrics.daily_metrics_batch_size} records → Publish ALL metrics → Save ALL to DB")
        logger.info("  All outputs perfectly synchronized at same frequency!")
        logger.info("Press Ctrl+C to stop")
        logger.info("=" * 80)
        
        if not self.connect():
            return
        
        try:
            logger.info("Starting real-time rolling average processing...")
            
            for message in self.consumer:
                self.process_message(message)
                
        except KeyboardInterrupt:
            logger.info("\nStopped by user (Ctrl+C)")
            logger.info("Publishing final metrics...")
            self.metrics.force_publish_all_metrics()
        except Exception as e:
            logger.error(f"Error in consumer loop: {e}")
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Close connections"""
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer closed")
        
        if self.kafka_producer:
            self.kafka_producer.close()
            logger.info("Kafka producer closed")
        
        if self.postgres_db:
            self.postgres_db.close()
            logger.info("PostgreSQL connection closed")


def main():
    consumer = CameraTrafficConsumer()
    consumer.start_consuming()


if __name__ == "__main__":
    main()