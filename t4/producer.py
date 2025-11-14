#!/usr/bin/env python3
"""
Social Media Data Producer - VM Version
Configured specifically for Kafka running in VirtualBox VM
"""

import csv
import json
import time
import logging
from typing import List, Dict, Any
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SocialMediaProducer:
    def __init__(self, bootstrap_servers: str = 'dinesh-virtualbox:9092'):
        """Initialize the Kafka producer for VM setup"""
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.facebook_data = []
        self.twitter_data = []
        self.facebook_index = 0
        self.twitter_index = 0
        
        # Initialize Kafka producer
        self._init_producer()
        
    def _init_producer(self):
        """Initialize Kafka producer with VM-specific configuration"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=5,  # Increased retries for VM network
                retry_backoff_ms=500,  # Increased backoff
                request_timeout_ms=60000,  # 60 seconds timeout
                max_block_ms=60000,  # 60 seconds block time
                # Force using the correct broker address
                security_protocol='PLAINTEXT',
                # Add connection config for VM
                connections_max_idle_ms=30000,
                metadata_max_age_ms=30000
            )
            logger.info(f"Kafka producer initialized successfully for {self.bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            raise

    def load_facebook_data(self, csv_file: str):
        """Load Facebook data from CSV file"""
        try:
            with open(csv_file, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                self.facebook_data = [row for row in reader if any(row.values())]  # Skip empty rows
            logger.info(f"Loaded {len(self.facebook_data)} Facebook records")
        except Exception as e:
            logger.error(f"Failed to load Facebook data: {e}")
            raise

    def load_twitter_data(self, csv_file: str):
        """Load Twitter data from CSV file"""
        try:
            with open(csv_file, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                self.twitter_data = [row for row in reader if any(row.values())]  # Skip empty rows
            logger.info(f"Loaded {len(self.twitter_data)} Twitter records")
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
            'timestamp': int(time.time() * 1000),  # Current timestamp in milliseconds
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
            # Send with longer timeout for VM
            future = self.producer.send('facebook-posts', key=record.get('post_id'), value=message)
            result = future.get(timeout=30)  # 30 second timeout
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
            'timestamp': int(time.time() * 1000),  # Current timestamp in milliseconds
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
            # Send with longer timeout for VM
            future = self.producer.send('twitter-posts', key=record.get('id'), value=message)
            result = future.get(timeout=30)  # 30 second timeout
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

    def test_connection(self) -> bool:
        """Test Kafka connection before starting"""
        try:
            # Get cluster metadata to test connection
            metadata = self.producer._metadata
            if metadata:
                logger.info("Kafka connection test successful")
                return True
            else:
                logger.error("Kafka connection test failed - no metadata")
                return False
        except Exception as e:
            logger.error(f"Kafka connection test failed: {e}")
            return False

    def start_streaming(self, interval: int = 5):
        """Start streaming data to Kafka topics"""
        logger.info(f"Starting data streaming every {interval} seconds...")
        logger.info("Press Ctrl+C to stop")
        
        # Test connection first
        logger.info("Testing Kafka connection...")
        time.sleep(2)  # Give producer time to initialize
        
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
    # File paths for data folder
    facebook_csv = 'data/Facebook-datasets.csv'
    twitter_csv = 'data/Twitter-datasets.csv'
    
    try:
        # Initialize producer
        logger.info("Starting Social Media Producer for VM...")
        producer = SocialMediaProducer()
        
        # Load data
        logger.info("Loading CSV data...")
        producer.load_facebook_data(facebook_csv)
        producer.load_twitter_data(twitter_csv)
        
        # Start streaming (every 5 seconds)
        producer.start_streaming(interval=5)
        
    except Exception as e:
        logger.error(f"Application error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())