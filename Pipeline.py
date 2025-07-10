import requests
import json
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import sys
import time
import logging

load_dotenv()
cwd = os.getcwd()
sys.path.append(cwd)

from utils.weather_api_utils import WeatherAPI

# Create logs directory if it doesn't exist
logs_dir = "logs"
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)

# Simple logging to file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(logs_dir, 'pipeline.txt')),
        logging.StreamHandler()  # Also print to console
    ]
)
logger = logging.getLogger(__name__)

# Global settings - easy to modify
STATIONS = ["0579W", "KSFO"]
FAILURE_WAIT_TIME = 7
NEW_REQUEST_WAIT_TIME = 0.1
RECURRENT_DOWNLOAD_WAIT_TIME = 10
MAX_RETRIES = 2

weather_api = WeatherAPI()

def safe_api_call(api_method, *args, **kwargs):
    """Safely call API methods with basic error handling"""
    try:
        result = api_method(*args, **kwargs)
        time.sleep(NEW_REQUEST_WAIT_TIME)
        return result
    except Exception as e:
        logger.error(f"API call failed: {e}")
        raise e

def process_station_data(station_id, data_processor, *args, **kwargs):
    """Process data for a single station with retry logic"""
    for attempt in range(MAX_RETRIES + 1):
        try:
            return safe_api_call(data_processor, station_id, *args, **kwargs)
        except Exception as e:
            if attempt < MAX_RETRIES:
                logger.warning(f"Attempt {attempt + 1} failed for {station_id}: {e}")
                time.sleep(FAILURE_WAIT_TIME)
            else:
                logger.error(f"All retries failed for station {station_id}: {e}")
                return None
    return None

def upsert_data_safely(df, upsert_method):
    """Safely upsert data with error handling"""
    if df is None or df.empty:
        logger.warning("No data to upsert")
        return False
    
    try:
        upsert_method(df)
        logger.info(f"Successfully upserted {len(df)} records")
        return True
    except Exception as e:
        logger.error(f"Failed to upsert data: {e}")
        return False

def download_station_metadata():
    """Download metadata for all stations"""
    logger.info("Starting station metadata download")
    
    successful_data = []
    failed_stations = []
    
    for station_id in STATIONS:
        df = process_station_data(station_id, weather_api.get_station_metadata)
        if df is not None:
            successful_data.append(pd.DataFrame([df]))
        else:
            failed_stations.append(station_id)
    
    if successful_data:
        combined_df = pd.concat(successful_data, ignore_index=True)
        upsert_data_safely(combined_df, weather_api.upsert_station_metadata)
    else:
        logger.error("No station metadata could be downloaded")
        raise RuntimeError("No station metadata available")
    
    return failed_stations

def download_historical_data(start_date, end_date):
    """Download historical measurements data"""
    logger.info("Starting historical data download")
    
    successful_data = []
    failed_stations = []
    
    for station_id in STATIONS:
        df = process_station_data(
            station_id,
            weather_api.get_measurements_data,
            start=start_date.strftime("%Y-%m-%d"),
            end=end_date.strftime("%Y-%m-%d")
        )
        if df is not None and not df.empty:
            successful_data.append(df)
        else:
            failed_stations.append(station_id)
    
    if successful_data:
        combined_df = pd.concat(successful_data, ignore_index=True)
        upsert_data_safely(combined_df, weather_api.upsert_measurements_data)
    else:
        logger.warning("No historical data could be downloaded")
    
    return failed_stations

def download_latest_data():
    """Download latest measurements data"""
    logger.info("Starting latest data download")
    
    successful_data = []
    failed_stations = []
    
    for station_id in STATIONS:
        df = process_station_data(station_id, weather_api.get_latest_measurement_data)
        if df is not None and not df.empty:
            successful_data.append(df)
        else:
            failed_stations.append(station_id)
    
    if successful_data:
        combined_df = pd.concat(successful_data, ignore_index=True)
        upsert_data_safely(combined_df, weather_api.upsert_measurements_data)
    else:
        logger.warning("No latest data could be downloaded")
    
    return failed_stations

def run_pipeline(end_date=None, start_date=None, max_run_time=60):
    """Run the complete pipeline with simple parameters"""
    if end_date is None:
        end_date = datetime.now()
    if start_date is None:
        start_date = end_date - timedelta(days=7)
    
    start_time = time.time()
    logger.info("\n\nStarting weather data pipeline")
    
    # Track all failed stations
    all_failed_stations = set()
    
    # Initial data download
    try:
        metadata_failures = download_station_metadata()
        all_failed_stations.update(metadata_failures)
    except RuntimeError as e:
        logger.error(f"Failed to download metadata: {e}")
        return {'error': str(e)}
    
    # Download historical data
    historical_failures = download_historical_data(start_date, end_date)
    all_failed_stations.update(historical_failures)
    
    # Continuous monitoring loop
    iteration_count = 0
    while time.time() - start_time < max_run_time:
        iteration_count += 1
        logger.info(f"Starting iteration {iteration_count}")
        
        latest_failures = download_latest_data()
        all_failed_stations.update(latest_failures)
        
        time.sleep(RECURRENT_DOWNLOAD_WAIT_TIME)
    
    runtime = time.time() - start_time
    logger.info(f"Pipeline completed after {runtime:.2f} seconds")
    
    return {
        'runtime_seconds': runtime,
        'iterations': iteration_count,
        'failed_stations': list(all_failed_stations),
        'successful_stations': [s for s in STATIONS if s not in all_failed_stations]
    }

if __name__ == "__main__":
    print("\n\nStarting pipeline...")
    result = run_pipeline()
    print(f"Pipeline completed with result: {result}")