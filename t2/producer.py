#!/usr/bin/env python3
"""
Simple CSV-based Real-time Austin Camera Traffic Counts Data Producer
Clean and simple data processing with specific type conversions
"""

import json
import time
import logging
import yaml
from datetime import datetime
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError
import sys
import os
import re

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


def clean_to_digits_only(value):
    """Extract only digits from a value"""
    if pd.isna(value):
        return 0
    # Extract only digits
    digits = re.findall(r'\d', str(value))
    if digits:
        return int(''.join(digits))
    return 0


def convert_to_boolean(value):
    """Convert value to boolean"""
    if pd.isna(value):
        return False
    str_val = str(value).lower().strip()
    return str_val in ['true', '1', 'yes', 't']


def convert_to_string(value):
    """Convert value to clean string"""
    if pd.isna(value):
        return 'UNKNOWN'
    return str(value).strip()


def convert_read_date(value):
    """Convert Read Date to timestamp"""
    if pd.isna(value):
        return datetime.now().isoformat()
    try:
        # Try to parse the date string
        dt = pd.to_datetime(value)
        return dt.isoformat()
    except:
        return datetime.now().isoformat()


class CameraTrafficProducer:
    def __init__(self, csv_file_path=None):
        # Load configuration from config.yml
        self.kafka_config = config['kafka']
        self.producer_config = config['producer']
        
        # CSV file settings - read from config
        if csv_file_path is None:
            self.csv_file_path = self.producer_config.get('csv_file_path', 'Camera_Traffic_Counts_20251110.csv')
        else:
            self.csv_file_path = csv_file_path
            
        # Read batch configuration from config.yml
        self.max_records = self.producer_config.get('max_records', 200000)
        self.records_per_batch = self.producer_config.get('records_per_batch', 10)
        self.batch_interval_seconds = self.producer_config.get('batch_interval_seconds', 5)
        
        # Initialize Kafka producer
        self.kafka_producer = None
        
        # Data tracking
        self.total_records = 0
        self.sent_records = 0
        self.current_batch = 0
        
    def connect_kafka(self):
        """Connect to Kafka"""
        try:
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=self.kafka_config['bootstrap_servers'],
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8') if k else None
            )
            logger.info(f"Connected to Kafka: {self.kafka_config['bootstrap_servers']}")
            return True
        except Exception as e:
            logger.error(f"Kafka connection failed: {e}")
            return False
    
    def load_and_process_csv(self):
        """Load CSV file and process data types"""
        try:
            logger.info(f"Loading CSV file: {self.csv_file_path}")
            
            # Check if file exists
            if not os.path.exists(self.csv_file_path):
                logger.error(f"CSV file not found: {self.csv_file_path}")
                return None
            
            # Load CSV
            df = pd.read_csv(self.csv_file_path)
            logger.info(f"Successfully loaded {len(df)} records")
            
            # Limit records to first 200,000
            if len(df) > self.max_records:
                logger.info(f"Limiting to first {self.max_records:,} records")
                df = df.head(self.max_records).copy()
            
            logger.info(f"Original columns: {list(df.columns)}")
            
            # Process each column according to requirements
            logger.info("Processing data types...")
            
            # Convert "Record ID" to string
            if 'Record ID' in df.columns:
                df['Record ID'] = df['Record ID'].apply(convert_to_string)
                logger.info("✅ Converted 'Record ID' to string")
            
            # Convert "Intersection Name" to string
            if 'Intersection Name' in df.columns:
                df['Intersection Name'] = df['Intersection Name'].apply(convert_to_string)
                logger.info("✅ Converted 'Intersection Name' to string")
            
            # Convert "Direction" to string
            if 'Direction' in df.columns:
                df['Direction'] = df['Direction'].apply(convert_to_string)
                logger.info("✅ Converted 'Direction' to string")
            
            # Convert "Movement" to string
            if 'Movement' in df.columns:
                df['Movement'] = df['Movement'].apply(convert_to_string)
                logger.info("✅ Converted 'Movement' to string")
            
            # Convert "Heavy Vehicle" to boolean
            if 'Heavy Vehicle' in df.columns:
                df['Heavy Vehicle'] = df['Heavy Vehicle'].apply(convert_to_boolean)
                logger.info("Converted 'Heavy Vehicle' to boolean")
            
            # Convert "Year" to numeric (digits only)
            if 'Year' in df.columns:
                df['Year'] = df['Year'].apply(clean_to_digits_only)
                logger.info(f"Converted 'Year' to numeric - range: {df['Year'].min()} to {df['Year'].max()}")
            
            # Convert "ATD Device ID" to numeric (digits only)
            if 'ATD Device ID' in df.columns:
                df['ATD Device ID'] = df['ATD Device ID'].apply(clean_to_digits_only)
                logger.info(f"Converted 'ATD Device ID' to numeric - range: {df['ATD Device ID'].min()} to {df['ATD Device ID'].max()}")
            
            # Convert "Read Date" to timestamp
            if 'Read Date' in df.columns:
                df['Read Date'] = df['Read Date'].apply(convert_read_date)
                logger.info("Converted 'Read Date' to timestamp")
            
            # Show sample of processed data
            logger.info("Sample processed record:")
            if len(df) > 0:
                sample = df.iloc[0]
                for col in df.columns:
                    logger.info(f"  {col}: {sample[col]} ({type(sample[col]).__name__})")
            
            self.total_records = len(df)
            logger.info(f"✅ Successfully processed {self.total_records} records")
            return df
            
        except Exception as e:
            logger.error(f"Error loading CSV file: {e}")
            return None
    
    def send_batch_to_kafka(self, batch_df):
        """Send a batch of records to Kafka"""
        if batch_df.empty:
            return 0
        
        success_count = 0
        
        for _, record in batch_df.iterrows():
            try:
                # Convert record to dictionary
                record_dict = record.to_dict()
                
                # Use ATD device ID as key for partitioning
                key = str(record_dict.get('ATD Device ID', ''))
                
                # Send to Kafka
                future = self.kafka_producer.send(
                    self.kafka_config['topic'], 
                    key=key, 
                    value=record_dict
                )
                future.get(timeout=10)
                
                success_count += 1
                self.sent_records += 1
                
            except Exception as e:
                logger.error(f"Failed to send record: {e}")
        
        # Flush to ensure all messages are sent
        self.kafka_producer.flush()
        
        return success_count
    
    def simulate_streaming(self):
        """Main streaming simulation function"""
        logger.info("=" * 80)
        logger.info("CSV CAMERA TRAFFIC DATA PRODUCER")
        logger.info("=" * 80)
        logger.info(f"CSV File: {self.csv_file_path}")
        logger.info(f"Max Records: {self.max_records:,}")
        logger.info(f"Records per Batch: {self.records_per_batch}")
        logger.info(f"Batch Interval: {self.batch_interval_seconds} seconds")
        logger.info(f"Kafka Server: {self.kafka_config['bootstrap_servers']}")
        logger.info(f"Kafka Topic: {self.kafka_config['topic']}")
        logger.info("Press Ctrl+C to stop")
        logger.info("=" * 80)
        
        # Connect to Kafka
        if not self.connect_kafka():
            logger.error("Failed to connect to Kafka. Exiting.")
            return
        
        # Load and process CSV data
        df = self.load_and_process_csv()
        if df is None:
            logger.error("Failed to load CSV data. Exiting.")
            return
        
        logger.info(f"Ready to stream {self.total_records} records")
        
        try:
            # Calculate total batches
            total_batches = (self.total_records + self.records_per_batch - 1) // self.records_per_batch
            estimated_duration = total_batches * self.batch_interval_seconds
            logger.info(f"Estimated streaming duration: {estimated_duration} seconds ({estimated_duration/60:.1f} minutes)")
            
            # Stream data in batches
            for batch_num in range(total_batches):
                start_idx = batch_num * self.records_per_batch
                end_idx = min(start_idx + self.records_per_batch, self.total_records)
                
                # Get current batch
                batch_df = df.iloc[start_idx:end_idx].copy()
                
                # Send batch to Kafka
                sent_count = self.send_batch_to_kafka(batch_df)
                
                # Progress logging
                self.current_batch = batch_num + 1
                progress_pct = (self.sent_records / self.total_records) * 100
                
                # Show sample from current batch
                if not batch_df.empty:
                    sample = batch_df.iloc[0]
                    logger.info(f"BATCH {self.current_batch}/{total_batches} - Progress: {progress_pct:.1f}%")
                    logger.info(f"  Sent: {sent_count}/{len(batch_df)} records")
                    logger.info(f"  Sample: Device {sample.get('ATD Device ID')} at {sample.get('Read Date')}")
                    logger.info(f"    {sample.get('Intersection Name')} - {sample.get('Direction')} {sample.get('Movement')}")
                    logger.info(f"    Volume: {sample.get('Volume')}, Heavy Vehicle: {sample.get('Heavy Vehicle')}")
                    logger.info(f"  Total sent so far: {self.sent_records}/{self.total_records}")
                
                # Check if we're done
                if self.sent_records >= self.total_records:
                    break
                
                # Wait before next batch (unless it's the last batch)
                if batch_num < total_batches - 1:
                    logger.info(f"  Waiting {self.batch_interval_seconds} seconds...")
                    time.sleep(self.batch_interval_seconds)
            
            logger.info("=" * 80)
            logger.info("STREAMING COMPLETED SUCCESSFULLY!")
            logger.info(f"Total records sent: {self.sent_records}/{self.total_records}")
            logger.info("=" * 80)
                
        except KeyboardInterrupt:
            logger.info("\n" + "=" * 80)
            logger.info("STREAMING STOPPED BY USER (Ctrl+C)")
            logger.info(f"Records sent: {self.sent_records}/{self.total_records}")
            logger.info("=" * 80)
        except Exception as e:
            logger.error(f"Error during streaming: {e}")
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Close connections"""
        if self.kafka_producer:
            self.kafka_producer.flush()
            self.kafka_producer.close()
            logger.info("Kafka producer closed")


def main():
    """Main function"""
    producer = CameraTrafficProducer()
    
    # Check if CSV file exists
    if not os.path.exists(producer.csv_file_path):
        logger.error(f"CSV file not found: {producer.csv_file_path}")
        logger.info("Please check the 'csv_file_path' setting in config.yml")
        sys.exit(1)
    
    # Start streaming
    producer.simulate_streaming()


if __name__ == "__main__":
    main()