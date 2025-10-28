import json
import yaml
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime
from collections import defaultdict
import psycopg2

def load_config():
    with open('config.yml', 'r') as f:
        return yaml.safe_load(f)


class PostgresDB:
    """Handle PostgreSQL database operations"""
    
    def __init__(self, config):
        self.config = config
        self.conn = None
        self.cursor = None
    
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
            print("Connected to PostgreSQL")
            return True
        except Exception as e:
            print(f"Error connecting to PostgreSQL: {e}")
            return False
    
    def insert_sensor_data(self, record):
        """Insert raw sensor data"""
        try:
            self.cursor.execute("""
                INSERT INTO sensor_data 
                (detector_id, detector_type, detector_status, detector_direction, 
                 detector_movement, location_name, location_latitude, location_longitude, 
                 stream_timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                record.get('detector_id'),
                record.get('detector_type'),
                record.get('detector_status'),
                record.get('detector_direction'),
                record.get('detector_movement'),
                record.get('location_name'),
                record.get('location_latitude'),
                record.get('location_longitude'),
                record.get('stream_timestamp')
            ))
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Error inserting sensor data: {e}")
            self.conn.rollback()
            return False
    
    def update_hourly_metrics(self, detector_id, hour_timestamp, count, avg):
        """Update hourly metrics"""
        try:
            self.cursor.execute("""
                INSERT INTO hourly_metrics (detector_id, hour_timestamp, vehicle_count, avg_count)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (detector_id, hour_timestamp) 
                DO UPDATE SET vehicle_count = %s, avg_count = %s
            """, (detector_id, hour_timestamp, count, avg, count, avg))
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Error updating hourly metrics: {e}")
            self.conn.rollback()
            return False
    
    def update_daily_metrics(self, date, peak_detector, peak_volume, total_volume):
        """Update daily metrics"""
        try:
            self.cursor.execute("""
                INSERT INTO daily_metrics (date, peak_detector_id, peak_volume, total_volume)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (date) 
                DO UPDATE SET peak_detector_id = %s, peak_volume = %s, total_volume = %s
            """, (date, peak_detector, peak_volume, total_volume, 
                  peak_detector, peak_volume, total_volume))
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Error updating daily metrics: {e}")
            self.conn.rollback()
            return False
    
    def update_sensor_availability(self, detector_id, availability, last_seen, status):
        """Update sensor availability"""
        try:
            self.cursor.execute("""
                INSERT INTO sensor_availability 
                (detector_id, availability_percent, last_seen, status)
                VALUES (%s, %s, %s, %s)
            """, (detector_id, availability, last_seen, status))
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Error updating sensor availability: {e}")
            self.conn.rollback()
            return False
    
    def close(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("Database connection closed")


class TrafficMetrics:
    """Calculate real-time traffic metrics"""
    
    def __init__(self, db=None):
        self.hourly_counts = defaultdict(lambda: defaultdict(int))
        self.daily_volumes = defaultdict(int)
        self.sensor_last_seen = {}
        self.total_records = 0
        self.db = db
    
    def update(self, record):
        """Update metrics with new record"""
        self.total_records += 1
        
        detector_id = record.get('detector_id', 'unknown')
        timestamp = record.get('stream_timestamp', datetime.now().isoformat())
        
        try:
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            hour_key = dt.strftime('%Y-%m-%d %H:00')
            date_key = dt.strftime('%Y-%m-%d')
            
            # Update hourly count
            self.hourly_counts[hour_key][detector_id] += 1
            
            # Update daily volume
            self.daily_volumes[detector_id] += 1
            
            # Update last seen time
            self.sensor_last_seen[detector_id] = timestamp
            
            # Persist to database if available
            if self.db:
                # Save raw data
                self.db.insert_sensor_data(record)
                
                # Save hourly metrics
                count = self.hourly_counts[hour_key][detector_id]
                avg = self.get_hourly_average(detector_id)
                self.db.update_hourly_metrics(detector_id, hour_key, count, avg)
                
                # Save daily metrics
                peak_detector, peak_volume = self.get_daily_peak()
                total_volume = sum(self.daily_volumes.values())
                if peak_detector:
                    self.db.update_daily_metrics(date_key, peak_detector, peak_volume, total_volume)
                
                # Save sensor availability
                availability = self.get_sensor_availability()
                if detector_id in availability:
                    status = self._get_status_text(availability[detector_id])
                    self.db.update_sensor_availability(
                        detector_id, 
                        availability[detector_id], 
                        timestamp, 
                        status
                    )
            
        except Exception as e:
            print(f"Error processing record: {e}")
    
    def _get_status_text(self, availability):
        """Convert availability percentage to status text"""
        if availability == 100:
            return "OK"
        elif availability > 0:
            return "DEGRADED"
        else:
            return "DOWN"
    
    def get_hourly_average(self, detector_id=None):
        """Get hourly average vehicle count"""
        if detector_id:
            total = sum(self.hourly_counts[hour].get(detector_id, 0) 
                       for hour in self.hourly_counts)
            hours = len(self.hourly_counts)
            return total / hours if hours > 0 else 0
        else:
            totals = []
            for hour in self.hourly_counts:
                totals.append(sum(self.hourly_counts[hour].values()))
            return sum(totals) / len(totals) if totals else 0
    
    def get_daily_peak(self):
        """Get daily peak traffic volume"""
        if not self.daily_volumes:
            return None, 0
        peak_detector = max(self.daily_volumes, key=self.daily_volumes.get)
        peak_volume = self.daily_volumes[peak_detector]
        return peak_detector, peak_volume
    
    def get_sensor_availability(self):
        """Calculate sensor availability percentage"""
        if not self.sensor_last_seen:
            return {}
        
        now = datetime.now()
        availability = {}
        
        for detector_id, last_seen in self.sensor_last_seen.items():
            try:
                last_dt = datetime.fromisoformat(last_seen.replace('Z', '+00:00'))
                time_diff = (now - last_dt).total_seconds() / 60
                
                if time_diff < 10:
                    availability[detector_id] = 100.0
                elif time_diff < 60:
                    availability[detector_id] = 50.0
                else:
                    availability[detector_id] = 0.0
            except:
                availability[detector_id] = 0.0
        
        return availability
    
    def get_processed_data(self):
        """Generate processed data for Kafka output"""
        processed = {
            'timestamp': datetime.now().isoformat(),
            'total_records': self.total_records,
            'hourly_averages': {},
            'daily_peak': {},
            'sensor_availability': {}
        }
        
        # Hourly averages per detector
        for detector_id in self.daily_volumes.keys():
            processed['hourly_averages'][str(detector_id)] = round(self.get_hourly_average(detector_id), 2)
        
        # Daily peak
        peak_detector, peak_volume = self.get_daily_peak()
        if peak_detector:
            processed['daily_peak'] = {
                'detector_id': str(peak_detector),
                'volume': peak_volume
            }
        
        # Sensor availability
        availability = self.get_sensor_availability()
        for detector_id, avail_pct in availability.items():
            processed['sensor_availability'][str(detector_id)] = {
                'availability_percent': avail_pct,
                'status': self._get_status_text(avail_pct)
            }
        
        return processed
    
    def display_metrics(self):
        """Display current metrics"""
        print("\n" + "="*60)
        print("REAL-TIME TRAFFIC METRICS")
        print("="*60)
        
        print(f"\nTotal records processed: {self.total_records}")
        
        print("\n--- Hourly Average Vehicle Count ---")
        for detector_id in self.daily_volumes.keys():
            avg = self.get_hourly_average(detector_id)
            print(f"Detector {detector_id}: {avg:.2f} vehicles/hour")
        
        print("\n--- Daily Peak Traffic ---")
        peak_detector, peak_volume = self.get_daily_peak()
        if peak_detector:
            print(f"Peak Detector: {peak_detector} with {peak_volume} total vehicles")
        
        print("\n--- Sensor Availability ---")
        availability = self.get_sensor_availability()
        for detector_id, avail_pct in availability.items():
            status = "OK" if avail_pct == 100 else "DEGRADED" if avail_pct > 0 else "DOWN"
            print(f"Detector {detector_id}: {avail_pct:.0f}% {status}")
        
        print("="*60 + "\n")


def consume_traffic_data():
    """Consume messages from Kafka, calculate metrics, and persist to PostgreSQL"""
    
    # Load config
    config = load_config()
    
    print("Starting Kafka consumer with PostgreSQL persistence...")
    print(f"Kafka server: {config['kafka']['bootstrap_servers']}")
    print(f"Input topic: {config['kafka']['topic']}")
    print(f"Output topic: {config['kafka']['processed_topic']}")
    print("Press Ctrl+C to stop\n")
    
    # Initialize PostgreSQL
    db = PostgresDB(config['postgres'])
    if not db.connect():
        print("Warning: Running without database persistence")
        db = None
    
    # Create Kafka consumer
    consumer = KafkaConsumer(
        config['kafka']['topic'],
        bootstrap_servers=config['kafka']['bootstrap_servers'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='traffic-metrics-consumer'
    )
    
    # Create Kafka producer for processed data
    producer = KafkaProducer(
        bootstrap_servers=config['kafka']['bootstrap_servers'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    # Initialize metrics calculator
    metrics = TrafficMetrics(db)
    message_count = 0
    
    try:
        for message in consumer:
            message_count += 1
            record = message.value
            
            # Update metrics (and persist to DB)
            metrics.update(record)
            
            # Print progress
            print(f"[{message_count}] Detector {record.get('detector_id')} - "
                  f"Status: {record.get('detector_status')}")
            
            # Publish processed data every 10 messages
            if message_count % 10 == 0:
                processed_data = metrics.get_processed_data()
                producer.send(config['kafka']['processed_topic'], value=processed_data)
                print(f"Published processed data to {config['kafka']['processed_topic']}")
                metrics.display_metrics()
    
    except KeyboardInterrupt:
        print("\n\nConsumer stopped by user")
        metrics.display_metrics()
    
    finally:
        consumer.close()
        producer.close()
        if db:
            db.close()
        print("Consumer closed")


if __name__ == "__main__":
    consume_traffic_data()