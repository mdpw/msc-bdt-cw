import json
import argparse
import logging
from preprocessing import TrafficDataPreprocessor
from producer import KafkaTrafficProducer
import yaml

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_config():
    with open('config.yml', 'r') as f:
        return yaml.safe_load(f)

def preprocess_data(csv_file):    
    logger.info("STEP 1: DATA PREPROCESSING")
    
    try:
        preprocessor = TrafficDataPreprocessor(csv_file)
        df = preprocessor.preprocess()
        
        # Convert to JSON records
        records = preprocessor.to_json_records()
        logger.info(f"\nSuccessfully preprocessed {len(records)} records")
        
        return df, records
        
    except Exception as e:
        logger.error(f"Error during preprocessing: {e}")
        raise


def save_preprocessed_data(df, records, csv_output, json_output):    
    logger.info("STEP 2: SAVING PREPROCESSED DATA")
    
    try:
        # Save CSV
        df.to_csv(csv_output, index=False)
        logger.info(f"Saved preprocessed CSV to: {csv_output}")
        
        # Save JSON
        with open(json_output, 'w') as f:
            json.dump(records, f, indent=2)
        logger.info(f"Saved preprocessed JSON to: {json_output}")
        
        logger.info("\nData saved successfully")
        
    except Exception as e:
        logger.error(f"Error saving data: {e}")
        raise


def stream_to_kafka(records, config):
    logger.info("STEP 3: KAFKA STREAMING")
    kafka = config['kafka']
    streaming = config['streaming']
    
    try:
        producer = KafkaTrafficProducer(
            bootstrap_servers = kafka['bootstrap_servers'],
            topic = kafka['topic']
        )
        
        logger.info(f"\nStreaming configuration:")
        logger.info(f"  Kafka servers: {kafka['bootstrap_servers']}")
        logger.info(f"  Topic: {kafka['topic']}")
        logger.info(f"  Interval: {streaming['interval_seconds']} seconds")
        logger.info(f"  Loop mode: {streaming['loop_streaming']}")
        
        # Start streaming
        producer.stream_data(
            records=records,
            interval_seconds=streaming['interval_seconds'],
            loop=streaming['loop_streaming'],
            max_messages=streaming['max_messages']
        )
        
    except Exception as e:
        logger.error(f"Error during Kafka streaming: {e}")
        raise


def main():
    config = load_config()
    files = config['files']    

    logger.info("TRAFFIC DETECTOR DATA PROCESSING PIPELINE")
    logger.info(f"Input file: {files['csv_input']}")
    logger.info("="*80 + "\n")
    
    try:
        # Step 1: Preprocess data
        df, records = preprocess_data(files['csv_input'])
        
        # Step 2: Save preprocessed data
        save_preprocessed_data(
            df,
            records,
            files['csv_output'],
            files['json_output']
        )
        
        # Step 3: Stream to Kafka
        stream_to_kafka(records, config)                
        
        logger.info("PIPELINE COMPLETED SUCCESSFULLY")
        
    except KeyboardInterrupt:
        logger.info("\n\nPipeline interrupted by user")
    except Exception as e:
        logger.error(f"\n\nPipeline failed: {e}")
        raise


if __name__ == "__main__":
    main()