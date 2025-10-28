import pandas as pd
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TrafficDataPreprocessor:   
    def __init__(self, csv_file_path):
        self.csv_file_path = csv_file_path
        self.df = None
    
    def load_data(self):        
        try:
            self.df = pd.read_csv(self.csv_file_path)
            logger.info(f"Loaded {len(self.df)} records from {self.csv_file_path}")
            return self.df
        except Exception as e:
            logger.error(f"Error loading CSV file: {e}")
            raise
    
    def handle_missing_values(self):        
        # Fill missing string columns with 'UNKNOWN'
        string_columns = ['detector_type', 'detector_status', 'detector_direction', 
                         'detector_movement', 'location_name', 'atd_location_id', 'signal_id', 'ip_comm_status']
        for col in string_columns:
            if col in self.df.columns:
                self.df[col] = self.df[col].fillna('UNKNOWN')
        
        # Fill missing numeric columns with appropriate defaults
        if 'detector_id' in self.df.columns:
            self.df['detector_id'] = self.df['detector_id'].fillna(0)        
        if 'atd_location_id' in self.df.columns:
            self.df['atd_location_id'] = self.df['atd_location_id'].fillna('')
        
        # Fill missing coordinates with 0
        if 'location_latitude' in self.df.columns:
            self.df['location_latitude'] = self.df['location_latitude'].fillna(0.0)
        if 'location_longitude' in self.df.columns:
            self.df['location_longitude'] = self.df['location_longitude'].fillna(0.0)
        
        logger.info("Missing values handled")
    
    def convert_timestamps(self):
        """Convert timestamp columns to ISO format"""
        timestamp_columns = ['created_date', 'modified_date', 'comm_status_datetime_utc']
        
        for col in timestamp_columns:
            if col in self.df.columns:
                try:
                    # Try parsing the datetime
                    self.df[col] = pd.to_datetime(self.df[col], errors='coerce')
                    # Convert to ISO format string
                    self.df[col] = self.df[col].dt.strftime('%Y-%m-%dT%H:%M:%S')
                    # Replace NaT with None
                    self.df[col] = self.df[col].replace('NaT', None)
                except Exception as e:
                    logger.warning(f"Could not convert {col}: {e}")
        
        logger.info("Timestamps converted to ISO format")
    
    def parse_location(self):
        """Parse POINT coordinates from LOCATION column"""
        if 'LOCATION' in self.df.columns:
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
            if 'location_longitude' not in self.df.columns or 'location_latitude' not in self.df.columns:
                self.df[['location_longitude', 'location_latitude']] = \
                    self.df['LOCATION'].apply(lambda x: pd.Series(extract_coordinates(x)))
            else:
                # Fill missing coordinates from LOCATION column
                mask = self.df['location_longitude'].isna() | self.df['location_latitude'].isna()
                if mask.any():
                    self.df.loc[mask, ['location_longitude', 'location_latitude']] = \
                        self.df.loc[mask, 'LOCATION'].apply(lambda x: pd.Series(extract_coordinates(x)))
            
            logger.info("Location coordinates parsed")
    
    def clean_string_columns(self):
        """Clean and standardize string columns"""
        string_columns = self.df.select_dtypes(include=['object']).columns
        
        for col in string_columns:
            # Strip whitespace
            self.df[col] = self.df[col].astype(str).str.strip()
            # Replace empty strings with None
            self.df[col] = self.df[col].replace('', None)
            self.df[col] = self.df[col].replace('nan', None)
        
        logger.info("String columns cleaned")    
    
    def preprocess(self):
        """Execute full preprocessing pipeline"""
        self.load_data()
        self.handle_missing_values()
        self.parse_location()
        self.convert_timestamps()
        self.clean_string_columns()        
        logger.info("Preprocessing completed")
        return self.df
    
    def to_json_records(self):
        """Convert DataFrame to JSON records"""
        return self.df.to_dict(orient='records')   