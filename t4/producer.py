#!/usr/bin/env python3
"""
Social Media Data Producer - Config-based Version
Uses config.yml for all configuration settings
"""

import csv
import json
import time
import logging
import yaml
import os
from typing import List, Dict, Any
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SocialMediaProducer:
    def __init__(self, config_file: str = 'config.yml'):
        """Initialize the Kafka producer using config file"""
        self.config = self.load_config(config_file)
        self.producer = None
        self.facebook_data = []
        self.twitter_data = []
        self.facebook_index = 0
        self.twitter_index = 0
        
        # Initialize Kafka producer
        self._init_producer()
        
    def load_config(self, config_file: str) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        try:
            with open(config_file, 'r') as f:
                config = yaml.safe_load(f)
            logger.info(f"Configuration loaded from {config_file}")
            return config
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            raise
        
    def _init_producer(self):
        """Initialize Kafka producer with config-based settings"""
        try:
            bootstrap_servers = self.config['kafka']['servers']
            timeout_seconds = self.config.get('producer', {}).get('timeout_seconds', 30)
            
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=5,
                retry_backoff_ms=500,
                request_timeout_ms=timeout_seconds * 1000,
                max_block_ms=timeout_seconds * 1000,
                security_protocol='PLAINTEXT',
                connections_max_idle_ms=30000,
                metadata_max_age_ms=30000
            )
            logger.info(f"Kafka producer initialized successfully for {bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            raise

    def load_facebook_data(self):
        """Load Facebook data from CSV file using config"""
        try:
            data_folder = self.config.get('producer', {}).get('data_folder', 'data')
            facebook_csv = self.config.get('producer', {}).get('facebook_csv', 'Facebook-datasets.csv')
            csv_file = os.path.join(data_folder, facebook_csv)
            
            with open(csv_file, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                self.facebook_data = [row for row in reader if any(row.values())]
            logger.info(f"Loaded {len(self.facebook_data)} Facebook records from {csv_file}")
        except Exception as e:
            logger.error(f"Failed to load Facebook data: {e}")
            raise

    def load_twitter_data(self):
        """Load Twitter data from CSV file using config"""
        try:
            data_folder = self.config.get('producer', {}).get('data_folder', 'data')
            twitter_csv = self.config.get('producer', {}).get('twitter_csv', 'Twitter-datasets.csv')
            csv_file = os.path.join(data_folder, twitter_csv)
            
            with open(csv_file, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                self.twitter_data = [row for row in reader if any(row.values())]
            logger.info(f"Loaded {len(self.twitter_data)} Twitter records from {csv_file}")
        except Exception as e:
            logger.error(f"Failed to load Twitter data: {e}")
            raise

    def send_facebook_message(self) -> bool:
        """Send a Facebook message to Kafka topic"""
        if not self.facebook_data:
            logger.warning("No Facebook data available")
            return False
            
        # Get current record and move to next (circular)
        record = self.facebook_data[self.facebook_index]
        self.facebook_index = (self.facebook_index + 1) % len(self.facebook_data)
        
        # Clean and prepare the message
        message = {
            'platform': 'facebook',
            'timestamp': int(time.time() * 1000),
            'data': {
                'post_id': record.get('post_id', ''),
                'user_name': record.get('user_name', ''),
                'comment_text': record.get('comment_text', ''),
                'date_created': record.get('date_created', ''),
                'num_likes': self._safe_int(record.get('num_likes', 0)),
                'num_replies': self._safe_int(record.get('num_replies', 0)),
                'source_type': record.get('source_type', ''),
                'url': record.get('url', '')
            }
        }
        
        try:
            topic = self.config['kafka']['facebook_topic']
            timeout = self.config.get('producer', {}).get('timeout_seconds', 30)
            
            future = self.producer.send(topic, key=record.get('post_id'), value=message)
            result = future.get(timeout=timeout)
            logger.info(f"Facebook message sent - Post ID: {record.get('post_id', 'Unknown')[:20]}...")
            return True
        except KafkaError as e:
            logger.error(f"Failed to send Facebook message: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending Facebook message: {e}")
            return False

    def send_twitter_message(self) -> bool:
        """Send a Twitter message to Kafka topic"""
        if not self.twitter_data:
            logger.warning("No Twitter data available")
            return False
            
        # Get current record and move to next (circular)
        record = self.twitter_data[self.twitter_index]
        self.twitter_index = (self.twitter_index + 1) % len(self.twitter_data)
        
        # Clean and prepare the message
        message = {
            'platform': 'twitter',
            'timestamp': int(time.time() * 1000),
            'data': {
                'id': record.get('id', ''),
                'user_posted': record.get('user_posted', ''),
                'name': record.get('name', ''),
                'description': record.get('description', ''),
                'date_posted': record.get('date_posted', ''),
                'likes': self._safe_int(record.get('likes', 0)),
                'reposts': self._safe_int(record.get('reposts', 0)),
                'replies': self._safe_int(record.get('replies', 0)),
                'views': self._safe_int(record.get('views', 0)),
                'followers': self._safe_int(record.get('followers', 0)),
                'hashtags': record.get('hashtags', ''),
                'url': record.get('url', '')
            }
        }
        
        try:
            topic = self.config['kafka']['twitter_topic']
            timeout = self.config.get('producer', {}).get('timeout_seconds', 30)
            
            future = self.producer.send(topic, key=record.get('id'), value=message)
            result = future.get(timeout=timeout)
            logger.info(f"Twitter message sent - Post ID: {record.get('id', 'Unknown')[:20]}...")
            return True
        except KafkaError as e:
            logger.error(f"Failed to send Twitter message: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending Twitter message: {e}")
            return False

    def _safe_int(self, value) -> int:
        """Safely convert value to int"""
        try:
            if value == '' or value is None:
                return 0
            return int(float(str(value)))
        except (ValueError, TypeError):
            return 0

    def print_config_summary(self):
        """Print configuration summary"""
        print("=" * 50)
        print("Social Media Producer Configuration")
        print("=" * 50)
        print(f"Kafka Server: {self.config['kafka']['servers']}")
        print(f"Twitter Topic: {self.config['kafka']['twitter_topic']}")
        print(f"Facebook Topic: {self.config['kafka']['facebook_topic']}")
        print(f"Data Folder: {self.config.get('producer', {}).get('data_folder', 'data')}")
        print(f"Streaming Interval: {self.config.get('producer', {}).get('streaming_interval_seconds', 5)} seconds")
        print(f"Timeout: {self.config.get('producer', {}).get('timeout_seconds', 30)} seconds")
        print("=" * 50)

    def start_streaming(self):
        """Start streaming data to Kafka topics using config settings"""
        interval = self.config.get('producer', {}).get('streaming_interval_seconds', 5)
        
        logger.info(f"Starting data streaming every {interval} seconds...")
        logger.info("Press Ctrl+C to stop")
        
        try:
            message_count = 0
            while True:
                # Send Facebook message
                fb_success = self.send_facebook_message()
                
                # Send Twitter message  
                tw_success = self.send_twitter_message()
                
                message_count += 1
                
                if fb_success and tw_success:
                    logger.info(f"Batch {message_count} completed successfully")
                else:
                    logger.warning(f"Batch {message_count} had errors")
                
                # Wait for specified interval
                logger.info(f"Waiting {interval} seconds before next batch...")
                time.sleep(interval)
                
        except KeyboardInterrupt:
            logger.info("Stopping producer...")
        except Exception as e:
            logger.error(f"Error during streaming: {e}")
        finally:
            if self.producer:
                self.producer.close()
                logger.info("Producer closed successfully")

def main():
    """Main function"""
    try:
        # Initialize producer with config
        logger.info("Starting Social Media Producer with config.yml...")
        producer = SocialMediaProducer()
        
        # Print configuration summary
        producer.print_config_summary()
        
        # Load data
        logger.info("Loading CSV data...")
        producer.load_facebook_data()
        producer.load_twitter_data()
        
        # Start streaming
        producer.start_streaming()
        
    except Exception as e:
        logger.error(f"Application error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())