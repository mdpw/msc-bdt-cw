#!/usr/bin/env python3
"""
Simple Real-time Austin Traffic Data Producer
Fetches data from Austin Open Data API every 10 minutes and sends to Kafka
"""

import json
import time
import logging
import yaml
from datetime import datetime
import pandas as pd
from sodapy import Socrata
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Load configuration
def load_config():
    """Load configuration from config.yml"""
    try:
        with open('config.yml', 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        print("Error: config.yml not found. Please create the configuration file.")
        exit(1)
    except Exception as e:
        print(f"Error loading config.yml: {e}")
        exit(1)

# Load config first
config = load_config()

# Setup logging from config
logging.basicConfig(
    level=getattr(logging, config['logging']['level']),
    format=config['logging']['format']
)
logger = logging.getLogger(__name__)


class TrafficProducer:
    def __init__(self):
        # Load configuration from config.yml
        self.kafka_config = config['kafka']
        self.api_config = config['api']
        self.producer_config = config['producer']
        
        # Initialize clients
        self.kafka_producer = None
        self.socrata_client = None
        
    def connect_kafka(self):
        """Connect to Kafka"""
        try:
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=self.kafka_config['bootstrap_servers'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8') if k else None
            )
            logger.info(f"Connected to Kafka: {self.kafka_config['bootstrap_servers']}")
            return True
        except Exception as e:
            logger.error(f"Kafka connection failed: {e}")
            return False
    
    def connect_api(self):
        """Connect to Austin Open Data API"""
        try:
            # Check if authentication is provided
            if (self.api_config.get('app_token') and 
                self.api_config.get('username') and 
                self.api_config.get('password')):
                # Authenticated client
                self.socrata_client = Socrata(
                    self.api_config['domain'],
                    self.api_config['app_token'],
                    username=self.api_config['username'],
                    password=self.api_config['password']
                )
                logger.info(f"Connected to Austin Open Data API (authenticated)")
            else:
                # Unauthenticated client (works with public data)
                self.socrata_client = Socrata(self.api_config['domain'], None)
                logger.info(f"Connected to Austin Open Data API (public access)")
            return True
        except Exception as e:
            logger.error(f"API connection failed: {e}")
            return False
    
    def handle_missing_values(self, df):
        """Fill missing string columns with 'UNKNOWN'"""
        string_columns = ['detector_type', 'detector_status', 'detector_direction', 
                         'detector_movement', 'location_name', 'atd_location_id', 'signal_id', 'ip_comm_status']
        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].fillna('UNKNOWN')
        
        # Fill missing numeric columns with appropriate defaults
        if 'detector_id' in df.columns:
            df['detector_id'] = df['detector_id'].fillna(0)        
        if 'atd_location_id' in df.columns:
            df['atd_location_id'] = df['atd_location_id'].fillna('')
        
        # Fill missing coordinates with 0
        if 'location_latitude' in df.columns:
            df['location_latitude'] = df['location_latitude'].fillna(0.0)
        if 'location_longitude' in df.columns:
            df['location_longitude'] = df['location_longitude'].fillna(0.0)
        
        logger.info("Missing values handled")
        return df
    
    def convert_timestamps(self, df):
        """Convert timestamp columns to ISO format"""
        timestamp_columns = ['created_date', 'modified_date', 'comm_status_datetime_utc']
        
        for col in timestamp_columns:
            if col in df.columns:
                try:
                    # Try parsing the datetime
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    # Convert to ISO format string
                    df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%S')
                    # Replace NaT with None
                    df[col] = df[col].replace('NaT', None)
                except Exception as e:
                    logger.warning(f"Could not convert {col}: {e}")
        
        logger.info("Timestamps converted to ISO format")
        return df
    
    def parse_location(self, df):
        """Parse POINT coordinates from LOCATION column"""
        if 'LOCATION' in df.columns:
            def extract_coordinates(location_str):
                try:
                    if pd.isna(location_str):
                        return None, None
                    # Extract coordinates from POINT (-97.85701 30.172396)
                    coords = location_str.replace('POINT (', '').replace(')', '').split()
                    return float(coords[0]), float(coords[1])
                except:
                    return None, None
            
            # Only parse if columns don't exist or have missing values
            if 'location_longitude' not in df.columns or 'location_latitude' not in df.columns:
                df[['location_longitude', 'location_latitude']] = \
                    df['LOCATION'].apply(lambda x: pd.Series(extract_coordinates(x)))
            else:
                # Fill missing coordinates from LOCATION column
                mask = df['location_longitude'].isna() | df['location_latitude'].isna()
                if mask.any():
                    df.loc[mask, ['location_longitude', 'location_latitude']] = \
                        df.loc[mask, 'LOCATION'].apply(lambda x: pd.Series(extract_coordinates(x)))
            
            logger.info("Location coordinates parsed")
        return df
    
    def clean_string_columns(self, df):
        """Clean and standardize string columns"""
        string_columns = df.select_dtypes(include=['object']).columns
        
        for col in string_columns:
            # Strip whitespace
            df[col] = df[col].astype(str).str.strip()
            # Replace empty strings with None
            df[col] = df[col].replace('', None)
            df[col] = df[col].replace('nan', None)
        
        logger.info("String columns cleaned")
        return df
    
    def fetch_and_clean_data(self, limit=None):
        """Fetch data from API and apply comprehensive preprocessing"""
        try:
            # Use limit from config if not specified
            if limit is None:
                limit = self.producer_config['records_per_fetch']
                
            logger.info(f"Fetching up to {limit} records from API...")
            
            # Fetch data from Austin Traffic Detectors dataset
            results = self.socrata_client.get(self.api_config['dataset_id'], limit=limit)
            
            if not results:
                logger.warning("No data received from API")
                return []
            
            # Convert to DataFrame for processing
            df = pd.DataFrame.from_records(results)
            logger.info(f"Received {len(df)} records from API")
            
            # Apply comprehensive preprocessing pipeline
            logger.info("Starting preprocessing pipeline...")
            
            # Step 1: Handle missing values
            df = self.handle_missing_values(df)
            
            # Step 2: Parse location coordinates
            df = self.parse_location(df)
            
            # Step 3: Convert timestamps
            df = self.convert_timestamps(df)
            
            # Step 4: Clean string columns
            df = self.clean_string_columns(df)
            
            # Step 5: Add processing timestamps
            df['api_fetch_time'] = datetime.now().isoformat()
            df['stream_time'] = datetime.now().isoformat()
            
            # Convert back to list of dictionaries
            clean_records = df.to_dict('records')
            
            logger.info(f"Preprocessing completed - prepared {len(clean_records)} clean records")
            return clean_records
            
        except Exception as e:
            logger.error(f"Error fetching/preprocessing data: {e}")
            return []
    
    def send_to_kafka(self, records):
        """Send records to Kafka"""
        if not records:
            logger.warning("No records to send")
            return 0
        
        success_count = 0
        timeout = self.producer_config['kafka_timeout_seconds']
        
        for record in records:
            try:
                # Use detector_id as key for partitioning
                key = str(record.get('detector_id', ''))
                
                # Send to Kafka
                future = self.kafka_producer.send(self.kafka_config['topic'], key=key, value=record)
                future.get(timeout=timeout)  # Wait for send to complete
                
                success_count += 1
                
            except Exception as e:
                logger.error(f"Failed to send record: {e}")
        
        logger.info(f"Successfully sent {success_count}/{len(records)} records to Kafka topic '{self.kafka_config['topic']}'")
        return success_count
    
    def run_once(self):
        """Fetch data once and send to Kafka"""
        logger.info("=" * 60)
        logger.info(f"FETCH CYCLE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 60)
        
        # Fetch and clean data
        records = self.fetch_and_clean_data()
        
        if records:
            # Send to Kafka
            sent_count = self.send_to_kafka(records)
            
            # Show sample record
            sample = records[0]
            logger.info(f"\nSample record:")
            logger.info(f"  Detector ID: {sample.get('detector_id', 'N/A')}")
            logger.info(f"  Status: {sample.get('detector_status', 'N/A')}")
            logger.info(f"  Location: {sample.get('location_name', 'N/A')}")
            logger.info(f"  Type: {sample.get('detector_type', 'N/A')}")
            
            return sent_count
        else:
            logger.warning("No records fetched")
            return 0
    
    def start_continuous(self):
        """Start continuous fetching every 10 minutes"""
        logger.info("=" * 80)
        logger.info("AUSTIN TRAFFIC DATA REAL-TIME PRODUCER")
        logger.info("=" * 80)
        logger.info(f"Kafka Server: {self.kafka_config['bootstrap_servers']}")
        logger.info(f"Kafka Topic: {self.kafka_config['topic']}")
        logger.info(f"API Domain: {self.api_config['domain']}")
        logger.info(f"Dataset ID: {self.api_config['dataset_id']}")
        logger.info(f"Fetch Interval: {self.producer_config['fetch_interval_minutes']} minutes")
        logger.info(f"Records per fetch: {self.producer_config['records_per_fetch']}")
        logger.info("Press Ctrl+C to stop")
        logger.info("=" * 80)
        
        # Connect to services
        if not self.connect_kafka():
            return
        if not self.connect_api():
            return
        
        try:
            while True:
                # Fetch and send data
                self.run_once()
                
                # Wait for next interval
                wait_seconds = self.producer_config['fetch_interval_minutes'] * 60
                logger.info(f"\nWaiting {self.producer_config['fetch_interval_minutes']} minutes until next fetch...")
                logger.info("=" * 60 + "\n")
                time.sleep(wait_seconds)
                
        except KeyboardInterrupt:
            logger.info("\nStopped by user (Ctrl+C)")
        except Exception as e:
            logger.error(f"Error in continuous loop: {e}")
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Close connections"""
        if self.kafka_producer:
            self.kafka_producer.flush()
            self.kafka_producer.close()
            logger.info("Kafka producer closed")
        
        if self.socrata_client:
            self.socrata_client.close()
            logger.info("API client closed")


def main():
    """Main function"""
    producer = TrafficProducer()
    
    # You can test with a single fetch:
    # producer.connect_kafka()
    # producer.connect_api()
    # producer.run_once()
    
    # Or start continuous operation:
    producer.start_continuous()


if __name__ == "__main__":
    main()