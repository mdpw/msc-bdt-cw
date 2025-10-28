import json
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class KafkaTrafficProducer:    
    def __init__(self, bootstrap_servers='localhost:9092', topic='traffic-detectors'):        
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = None
        self.message_count = 0
        self.error_count = 0
    
    def connect(self):        
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8') if k else None,
                acks='all',  # Wait for all replicas to acknowledge
                retries=3,
                max_in_flight_requests_per_connection=1,
                compression_type='gzip',
                linger_ms=10,  # Batch messages for 10ms
                batch_size=16384  # Batch size in bytes
            )
            logger.info(f"Connected to Kafka at {self.bootstrap_servers}")
            logger.info(f"Will publish to topic: {self.topic}")
            return True
        except KafkaError as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error connecting to Kafka: {e}")
            return False
    
    def send_record(self, record, key=None):
        try:            
            future = self.producer.send(
                self.topic,
                key=key,
                value=record
            )
            
            # Wait for send to complete
            metadata = future.get(timeout=10)
            
            self.message_count += 1
            logger.info(
                f"Sent record {record.get('record_id', 'N/A')} | "
                f"Detector: {record.get('detector_id', 'N/A')} | "
                f"Status: {record.get('detector_status', 'N/A')} | "
                f"Partition: {metadata.partition} | "
                f"Offset: {metadata.offset}"
            )
            return True
            
        except KafkaError as e:
            self.error_count += 1
            logger.error(f"Kafka error sending record: {e}")
            return False
        except Exception as e:
            self.error_count += 1
            logger.error(f"Error sending record: {e}")
            return False
    
    def stream_data(self, records, interval_seconds=5, loop=False, max_messages=None):
        """
        Stream records to Kafka with specified interval
        
        Args:
            records: List of record dictionaries
            interval_seconds: Time between messages (default 5 seconds)
            loop: If True, continuously loop through records
            max_messages: Maximum number of messages to send (None for unlimited)
        """
        if not self.producer:
            if not self.connect():
                logger.error("Cannot start streaming - failed to connect to Kafka")
                return
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Starting data stream")
        logger.info(f"Total records: {len(records)}")
        logger.info(f"Interval: {interval_seconds} seconds")
        logger.info(f"Loop mode: {loop}")
        logger.info(f"Max messages: {max_messages if max_messages else 'unlimited'}")
        logger.info(f"{'='*60}\n")
        
        try:
            iteration = 1
            while True:
                if loop:
                    logger.info(f"\n--- Iteration {iteration} ---")
                
                for idx, record in enumerate(records, 1):
                    # Check if we've reached max messages
                    if max_messages and self.message_count >= max_messages:
                        logger.info(f"\nReached max messages limit: {max_messages}")
                        return
                    
                    # Use detector_id as the key for partitioning
                    key = str(record.get('detector_id', ''))
                    
                    # Send record
                    self.send_record(record, key=key)
                    
                    # Progress indicator
                    if idx % 10 == 0 or idx == len(records):
                        logger.info(f"Progress: {idx}/{len(records)} records sent in this iteration")
                    
                    # Wait before sending next message
                    time.sleep(interval_seconds)
                
                if not loop:
                    logger.info(f"\n{'='*60}")
                    logger.info("Finished streaming all records")
                    logger.info(f"Total messages sent: {self.message_count}")
                    logger.info(f"Total errors: {self.error_count}")
                    logger.info(f"{'='*60}")
                    break
                
                iteration += 1
                logger.info(f"Completed iteration {iteration - 1}, looping back to start...\n")
        
        except KeyboardInterrupt:
            logger.info("\n\nStreaming interrupted by user (Ctrl+C)")
            logger.info(f"Total messages sent: {self.message_count}")
            logger.info(f"Total errors: {self.error_count}")
        except Exception as e:
            logger.error(f"Unexpected error during streaming: {e}")
        finally:
            self.close()
    
    def send_batch(self, records, keys=None):
        """
        Send multiple records as a batch
        
        Args:
            records: List of record dictionaries
            keys: Optional list of keys corresponding to records
            
        Returns:
            int: Number of successfully sent records
        """
        if not self.producer:
            if not self.connect():
                return 0
        
        success_count = 0
        for idx, record in enumerate(records):
            key = keys[idx] if keys and idx < len(keys) else None
            if self.send_record(record, key):
                success_count += 1
        
        return success_count
    
    def get_statistics(self):
        """Get producer statistics"""
        return {
            'messages_sent': self.message_count,
            'errors': self.error_count,
            'success_rate': (self.message_count / (self.message_count + self.error_count) * 100) 
                           if (self.message_count + self.error_count) > 0 else 0
        }
    
    def close(self):
        """Close Kafka producer and cleanup"""
        if self.producer:
            logger.info("\nFlushing remaining messages...")
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka producer closed successfully")
            
            # Print final statistics
            stats = self.get_statistics()
            logger.info(f"\n{'='*60}")
            logger.info("Final Statistics:")
            logger.info(f"  Messages sent: {stats['messages_sent']}")
            logger.info(f"  Errors: {stats['errors']}")
            logger.info(f"  Success rate: {stats['success_rate']:.2f}%")
            logger.info(f"{'='*60}\n")