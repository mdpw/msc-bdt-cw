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
            return True
        except Exception as e:
            logger.error(f"PostgreSQL connection failed: {e}")
            self.enabled = False
            return False
    
    def save_hourly_metrics(self, hour_timestamp, sensor_metrics):
        """Save hourly metrics to PostgreSQL"""
        if not self.enabled:
            return
        
        try:
            for sensor_id, data in sensor_metrics.items():
                self.cursor.execute("""
                    INSERT INTO hourly_vehicle_metrics 
                    (hour_timestamp, sensor_id, vehicle_count, avg_vehicles_per_hour)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (hour_timestamp, sensor_id) 
                    DO UPDATE SET 
                        vehicle_count = EXCLUDED.vehicle_count,
                        avg_vehicles_per_hour = EXCLUDED.avg_vehicles_per_hour
                """, (hour_timestamp, sensor_id, data['count'], data['avg']))
            
            self.conn.commit()
            logger.debug(f"Saved hourly metrics for {hour_timestamp}")
            
        except Exception as e:
            logger.error(f"Error saving hourly metrics: {e}")
            self.conn.rollback()
    
    def save_daily_metrics(self, date, total_volume, peak_sensor_id, peak_volume):
        """Save daily peak metrics to PostgreSQL"""
        if not self.enabled:
            return
        
        try:
            self.cursor.execute("""
                INSERT INTO daily_peak_metrics 
                (date, total_volume, peak_sensor_id, peak_sensor_volume)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (date) 
                DO UPDATE SET 
                    total_volume = EXCLUDED.total_volume,
                    peak_sensor_id = EXCLUDED.peak_sensor_id,
                    peak_sensor_volume = EXCLUDED.peak_sensor_volume
            """, (date, total_volume, peak_sensor_id, peak_volume))
            
            self.conn.commit()
            logger.debug(f"Saved daily metrics for {date}")
            
        except Exception as e:
            logger.error(f"Error saving daily metrics: {e}")
            self.conn.rollback()
    
    def save_availability_metrics(self, measurement_time, availability_data):
        """Save sensor availability metrics to PostgreSQL"""
        if not self.enabled:
            return
        
        try:
            for sensor_id, data in availability_data.items():
                self.cursor.execute("""
                    INSERT INTO sensor_availability_metrics 
                    (measurement_timestamp, sensor_id, availability_percentage, status, last_seen)
                    VALUES (%s, %s, %s, %s, %s)
                """, (measurement_time, sensor_id, data['percentage'], 
                      data['status'], data.get('last_seen')))
            
            self.conn.commit()
            logger.debug(f"Saved availability metrics for {len(availability_data)} sensors")
            
        except Exception as e:
            logger.error(f"Error saving availability metrics: {e}")
            self.conn.rollback()
    
    def close(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("PostgreSQL connection closed")


class TimeBasedTrafficMetrics:
    """Handles time-based aggregations for traffic data with output capabilities"""
    
    def __init__(self, kafka_producer=None, postgres_db=None):
        # Hourly metrics: {hour_timestamp: {sensor_id: count}}
        self.hourly_vehicle_counts = defaultdict(lambda: defaultdict(int))
        
        # Daily metrics: {date: {sensor_id: total_count}}
        self.daily_vehicle_counts = defaultdict(lambda: defaultdict(int))
        
        # Sensor availability tracking: {sensor_id: [timestamps]}
        self.sensor_activity = defaultdict(list)
        
        # Last seen data for each sensor
        self.last_seen = {}
        self.last_modified_dates = {}
        
        # Output handlers
        self.kafka_producer = kafka_producer
        self.postgres_db = postgres_db
        
        # Lock for thread safety
        self.lock = threading.Lock()
        
        # Statistics
        self.total_messages = 0
        self.vehicle_detections = 0
        
    def process_vehicle_detection(self, record):
        """Process a record that indicates vehicle detection"""
        with self.lock:
            try:
                detector_id = str(record.get('detector_id', 'unknown'))
                modified_date = record.get('modified_date')
                
                if not modified_date:
                    return False
                
                # Check if this is a NEW vehicle detection
                last_seen_modified = self.last_modified_dates.get(detector_id)
                if last_seen_modified and modified_date <= last_seen_modified:
                    return False  # Not a new detection
                
                # NEW VEHICLE DETECTED!
                self.last_modified_dates[detector_id] = modified_date
                self.vehicle_detections += 1
                
                # Parse the modified timestamp
                detection_time = datetime.fromisoformat(modified_date.replace('Z', '+00:00'))
                
                # Create time keys
                hour_key = detection_time.strftime('%Y-%m-%d %H:00:00')
                date_key = detection_time.strftime('%Y-%m-%d')
                
                # Increment vehicle counts
                self.hourly_vehicle_counts[hour_key][detector_id] += 1
                self.daily_vehicle_counts[date_key][detector_id] += 1
                
                logger.debug(f"Vehicle detected: Sensor {detector_id} at {detection_time}")
                return True
                
            except Exception as e:
                logger.error(f"Error processing vehicle detection: {e}")
                return False
    
    def update_sensor_status(self, record):
        """Update sensor status/availability regardless of vehicle detection"""
        with self.lock:
            try:
                detector_id = str(record.get('detector_id', 'unknown'))
                stream_time = record.get('stream_time')
                
                if stream_time:
                    timestamp = datetime.fromisoformat(stream_time.replace('Z', '+00:00'))
                    
                    # Keep only recent activity (last 24 hours)
                    cutoff = timestamp - timedelta(hours=24)
                    self.sensor_activity[detector_id].append(timestamp)
                    self.sensor_activity[detector_id] = [
                        t for t in self.sensor_activity[detector_id] if t > cutoff
                    ]
                    self.last_seen[detector_id] = timestamp
                    
            except Exception as e:
                logger.error(f"Error updating sensor status: {e}")
    
    def get_hourly_metrics(self, hours_back=24):
        """Get hourly metrics for output"""
        with self.lock:
            now = datetime.now()
            results = {}
            
            for hours_ago in range(hours_back):
                hour_time = now - timedelta(hours=hours_ago)
                hour_key = hour_time.strftime('%Y-%m-%d %H:00:00')
                
                if hour_key in self.hourly_vehicle_counts:
                    sensor_data = {}
                    for sensor_id, count in self.hourly_vehicle_counts[hour_key].items():
                        sensor_data[sensor_id] = {
                            'count': count,
                            'avg': count  # For single hour, count = average
                        }
                    results[hour_key] = sensor_data
            
            return results
    
    def get_daily_metrics(self, days_back=7):
        """Get daily peak traffic metrics"""
        with self.lock:
            now = datetime.now()
            results = {}
            
            for days_ago in range(days_back):
                date = now - timedelta(days=days_ago)
                date_key = date.strftime('%Y-%m-%d')
                
                if date_key in self.daily_vehicle_counts:
                    sensors = self.daily_vehicle_counts[date_key]
                    total_volume = sum(sensors.values())
                    peak_sensor = max(sensors.items(), key=lambda x: x[1], default=('unknown', 0))
                    
                    results[date_key] = {
                        'total_volume': total_volume,
                        'peak_sensor_id': peak_sensor[0],
                        'peak_sensor_volume': peak_sensor[1],
                        'all_sensors': dict(sensors)
                    }
            
            return results
    
    def get_availability_metrics(self):
        """Calculate sensor availability metrics"""
        with self.lock:
            now = datetime.now()
            availability = {}
            
            # Expected: data every 10 minutes = 144 data points per day
            expected_daily_points = 24 * 6  # 6 points per hour
            
            for sensor_id, activities in self.sensor_activity.items():
                if not activities:
                    availability[sensor_id] = {
                        'percentage': 0.0,
                        'status': 'DOWN',
                        'last_seen': None
                    }
                    continue
                
                # Count activity in last 24 hours
                last_24h = now - timedelta(hours=24)
                recent_activities = [a for a in activities if a > last_24h]
                
                # Calculate availability percentage
                actual_points = len(recent_activities)
                availability_pct = min(100.0, (actual_points / expected_daily_points) * 100)
                
                status = 'OK' if availability_pct > 80 else 'DEGRADED' if availability_pct > 50 else 'DOWN'
                
                availability[sensor_id] = {
                    'percentage': round(availability_pct, 2),
                    'status': status,
                    'last_seen': max(activities).isoformat() if activities else None
                }
            
            return availability
    
    def publish_metrics_to_kafka(self):
        """Publish metrics to Kafka topics for visualization"""
        if not self.kafka_producer:
            return
        
        try:
            current_time = datetime.now().isoformat()
            
            # 1. Hourly metrics topic
            hourly_data = self.get_hourly_metrics(24)
            if hourly_data:
                hourly_message = {
                    'timestamp': current_time,
                    'type': 'hourly_vehicle_metrics',
                    'data': hourly_data
                }
                self.kafka_producer.send('hourly-vehicle-metrics', value=hourly_message)
                logger.debug("Published hourly metrics to Kafka")
            
            # 2. Daily metrics topic
            daily_data = self.get_daily_metrics(7)
            if daily_data:
                daily_message = {
                    'timestamp': current_time,
                    'type': 'daily_peak_metrics',
                    'data': daily_data
                }
                self.kafka_producer.send('daily-peak-metrics', value=daily_message)
                logger.debug("Published daily metrics to Kafka")
            
            # 3. Availability metrics topic
            availability_data = self.get_availability_metrics()
            if availability_data:
                availability_message = {
                    'timestamp': current_time,
                    'type': 'sensor_availability_metrics',
                    'data': availability_data
                }
                self.kafka_producer.send('sensor-availability-metrics', value=availability_message)
                logger.debug("Published availability metrics to Kafka")
            
            # Flush all messages
            self.kafka_producer.flush()
            logger.info("Published all metrics to Kafka topics")
            
        except Exception as e:
            logger.error(f"Error publishing metrics to Kafka: {e}")
    
    def persist_metrics_to_postgres(self):
        """Persist metrics to PostgreSQL for historical analysis"""
        if not self.postgres_db or not self.postgres_db.enabled:
            return
        
        try:
            current_time = datetime.now()
            
            # Save hourly metrics
            hourly_data = self.get_hourly_metrics(24)
            for hour_timestamp, sensor_metrics in hourly_data.items():
                self.postgres_db.save_hourly_metrics(hour_timestamp, sensor_metrics)
            
            # Save daily metrics
            daily_data = self.get_daily_metrics(7)
            for date, metrics in daily_data.items():
                self.postgres_db.save_daily_metrics(
                    date, 
                    metrics['total_volume'],
                    metrics['peak_sensor_id'],
                    metrics['peak_sensor_volume']
                )
            
            # Save availability metrics
            availability_data = self.get_availability_metrics()
            self.postgres_db.save_availability_metrics(current_time, availability_data)
            
            logger.info("Persisted all metrics to PostgreSQL")
            
        except Exception as e:
            logger.error(f"Error persisting metrics to PostgreSQL: {e}")
    
    def print_metrics(self):
        """Print current metrics to console"""
        logger.info("\n" + "="*80)
        logger.info("REAL-TIME TRAFFIC METRICS")
        logger.info("="*80)
        logger.info(f"Total messages processed: {self.total_messages}")
        logger.info(f"Vehicle detections: {self.vehicle_detections}")
        
        # Hourly averages
        hourly_data = self.get_hourly_metrics(24)
        if hourly_data:
            logger.info(f"\nHourly Vehicle Count (Last 24 hours):")
            for hour, sensors in sorted(hourly_data.items())[-5:]:  # Show last 5 hours
                total_hour = sum(s['count'] for s in sensors.values())
                logger.info(f"  {hour}: {total_hour} vehicles ({len(sensors)} sensors)")
        
        # Daily peaks
        daily_data = self.get_daily_metrics(7)
        if daily_data:
            logger.info(f"\nDaily Peak Traffic (Last 7 days):")
            for date, data in sorted(daily_data.items()):
                logger.info(f"  {date}: {data['total_volume']} total, "
                           f"peak sensor {data['peak_sensor_id']} ({data['peak_sensor_volume']})")
        
        # Sensor availability
        availability = self.get_availability_metrics()
        active_sensors = {k: v for k, v in availability.items() if v['percentage'] > 0}
        if active_sensors:
            logger.info(f"\nSensor Availability (Top 10 active sensors):")
            for sensor_id, data in sorted(active_sensors.items(), 
                                        key=lambda x: x[1]['percentage'], reverse=True)[:10]:
                status_emoji = "Green" if data['status'] == 'OK' else "Yellow" if data['status'] == 'DEGRADED' else "Red"
                logger.info(f"  Sensor {sensor_id}: {data['percentage']}% {status_emoji} {data['status']}")
        
        logger.info("="*80)
        
        # Publish to outputs
        self.publish_metrics_to_kafka()
        self.persist_metrics_to_postgres()


class TrafficConsumer:
    def __init__(self):
        self.kafka_config = config['kafka']
        self.consumer_config = config['consumer']
        self.postgres_config = config.get('postgres', {})
        
        # Kafka consumer
        self.consumer = None
        
        # Kafka producer for output topics (optional)
        self.kafka_producer = None
        self.enable_kafka_output = self.kafka_config.get('enable_output_topics', False)
        
        # PostgreSQL persistence (optional)
        self.postgres_db = None
        self.enable_postgres = self.postgres_config.get('enabled', False)
        
        # Initialize output handlers
        self.init_output_handlers()
        
        # Time-based metrics calculator with output capabilities
        self.metrics = TimeBasedTrafficMetrics(
            kafka_producer=self.kafka_producer,
            postgres_db=self.postgres_db
        )
        
        # Start background metrics reporting
        self.start_metrics_reporter()
    
    def init_output_handlers(self):
        """Initialize optional output handlers"""
        # Initialize Kafka producer for output topics
        if self.enable_kafka_output:
            try:
                self.kafka_producer = KafkaProducer(
                    bootstrap_servers=self.kafka_config['bootstrap_servers'],
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
                logger.info("Kafka producer initialized for output topics")
                logger.info("Output topics: hourly-vehicle-metrics, daily-peak-metrics, sensor-availability-metrics")
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
    
    def start_metrics_reporter(self):
        """Start background thread to report metrics every hour"""
        def report_metrics():
            while True:
                time.sleep(3600)  # Report every hour
                self.metrics.print_metrics()
        
        reporter_thread = threading.Thread(target=report_metrics, daemon=True)
        reporter_thread.start()
        logger.info("Started background metrics reporter (hourly)")
    
    def process_message(self, message):
        """Process incoming Kafka message"""
        try:
            record = message.value
            self.metrics.total_messages += 1
            
            # Always update sensor status (for availability tracking)
            self.metrics.update_sensor_status(record)
            
            # Check if this is a new vehicle detection
            if self.metrics.process_vehicle_detection(record):
                detector_id = record.get('detector_id', 'unknown')
                logger.info(f"Vehicle detected by sensor {detector_id} "
                           f"(Total detections: {self.metrics.vehicle_detections})")
            
            # Report progress
            if self.metrics.total_messages % 100 == 0:
                logger.info(f"Processed {self.metrics.total_messages} messages, "
                           f"{self.metrics.vehicle_detections} vehicle detections")
                
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
        """Start consuming messages"""
        logger.info("="*80)
        logger.info("ENHANCED ADVANCED TRAFFIC DATA CONSUMER")
        logger.info("Real-time Hourly/Daily Metrics with Output Capabilities")
        logger.info("="*80)
        logger.info(f"Input Topic: {self.kafka_config['topic']}")
        
        if self.enable_kafka_output:
            logger.info("Kafka Output: hourly-vehicle-metrics, daily-peak-metrics, sensor-availability-metrics")
        else:
            logger.info("Kafka Output: Disabled")
            
        if self.enable_postgres:
            logger.info("PostgreSQL Persistence: Enabled")
        else:
            logger.info("PostgreSQL Persistence: Disabled")
            
        logger.info("Tracking: Vehicle detections via modified_date changes")
        logger.info("Press Ctrl+C to stop")
        logger.info("="*80)
        
        if not self.connect():
            return
        
        try:
            logger.info("Waiting for messages...")
            
            for message in self.consumer:
                self.process_message(message)
                
        except KeyboardInterrupt:
            logger.info("\nStopped by user (Ctrl+C)")
            self.metrics.print_metrics()
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
    consumer = TrafficConsumer()
    consumer.start_consuming()


if __name__ == "__main__":
    main()