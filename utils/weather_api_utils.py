import requests
import json
from typing import Dict, List, Optional, Tuple
import pandas as pd
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import sys
import numpy as np

load_dotenv()
cwd = os.getcwd()
sys.path.append(cwd)

from utils.bd_utils import get_db_engine, update_insert_data, get_data_bd_query_generic

class WeatherAPI:
    """
    Client for the National Weather Service API
    Documentation: https://www.weather.gov/documentation/services-web-api
    """
    def __init__(self, user_agent: str = "WeatherApp/1.0 (contact@example.com)"):
        """
        Initialize the Weather API client
        
        Args:
            user_agent: User agent string for API identification
        """
        self.base_url = "https://api.weather.gov"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': user_agent,
            'Accept': 'application/geo+json'
        })
        self.engine = get_db_engine()

    def create_time_str_filters(self, date_input):
        """
        Create a time range for a given period.
        Accepts various input formats: string, datetime, date, or timestamp.
        """
        
        def parse_time(val):
            if isinstance(val, pd.Timestamp):
                return val
            elif isinstance(val, str):
                return pd.to_datetime(val)
            elif hasattr(val, "isoformat"):
                # Handles datetime.date, datetime.datetime, etc.
                return pd.to_datetime(val.isoformat())
            else:
                raise ValueError(f"Unsupported input type for time: {type(val)}")
        
        date_obj = parse_time(date_input)
        
        # Ensure timezone awareness - convert to UTC if needed
        if date_obj.tz is None:
            date_obj = date_obj.tz_localize('UTC')
        else:
            date_obj = date_obj.tz_convert('UTC')
        
        # Convert to ISO format with Z timezone
        date_str_out = date_obj.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        return date_str_out

    def get_station_metadata(self, station_id: str):
        """
        Get the name of a weather station by its ID
        
        Args:
            station_id: Station identifier (e.g., 'KSFO', '0579W')
            
        Returns:
            Station name as string
            
        Raises:
            requests.exceptions.RequestException: If API request fails
            KeyError: If station not found or name not available
        """
        url = f"{self.base_url}/stations/{station_id}"
        response = self.session.get(url)
        response.raise_for_status()
        data = response.json()
        
        data["properties"]
        output_data = {}
        output_data['station_id'] = station_id
        output_data['name'] = data['properties']['name']
        output_data['timezone'] = data['properties']['timeZone']
        output_data['latitude'] = data['geometry']['coordinates'][1]
        output_data['longitude'] = data['geometry']['coordinates'][0]
        return output_data

    def get_measurements_data(self, station_id: str, 
                            start=None, 
                            end=None,
                            round_decimals=2):
        """
        Get temperature and humidity measurements for a station in a given period.
        
        Args:
            station_id: Station identifier (e.g., 'KSFO', '0579W')
            start: Start datetime (string, datetime, or date object)
            end: End datetime (string, datetime, or date object)
        
        Returns:
            DataFrame with columns: ['timestamp', 'temperature', 'humidity']
        """
        url = f"{self.base_url}/stations/{station_id}/observations"
        
        params = {}
        if start:
            start_date = self.create_time_str_filters(start)
            params['start'] = start_date
        if end:
            end_date = self.create_time_str_filters(end)
            params['end'] = end_date
        
        response = self.session.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        records = []
        features = data.get('features', [])
        
        if len(features) == 0:
            print(f"No data found for station {station_id} between {start} and {end}")
            return pd.DataFrame()
        for feature in data.get('features', []):
            #feature = data['features'][0]
            props = feature.get('properties', {})
            timestamp = props.get('timestamp')
            temp = props.get('temperature', {}).get('value')
            humidity = props.get('relativeHumidity', {}).get('value')
            wind_speed = props.get('windSpeed', {}).get('value')
            if timestamp is not None and (temp is not None or humidity is not None):
                records.append({
                    'measurement_timestamp': timestamp,
                    'temperature': temp,
                    'humidity': humidity,
                    'wind_speed': wind_speed
                })
        df = pd.DataFrame(records)
        if not df.empty:
            df['measurement_timestamp'] = pd.to_datetime(df['measurement_timestamp'])
            df['station_id'] = station_id
        # Round temperature, humidity, and wind_speed columns to 2 decimals if they exist
            df[['temperature', 'humidity', 'wind_speed']] = df[['temperature', 'humidity', 'wind_speed']].round(round_decimals)
        return df 

    def upsert_station_metadata(self, df, table_name = "stations", schema = "public"):
        """
        Upsert station metadata into a table
        """
        update_insert_data(df, table_name, self.engine, schema)
        
    def upsert_measurements_data(self, df, table_name = "stations_measurements", schema = "public"):
        """
        Upsert measurements data into a table
        """
        update_insert_data(df, table_name, self.engine, schema)

    def get_current_stations_ids(self, table_name = "stations", schema = "public"):
        """
        Get the current stations ids from a table
        """
        df = get_data_bd_query_generic(self.engine, schema, table_name, columns_to_get=['station_id'])
        return df['station_id'].unique()

    def get_latest_measurement_data(self, station_id: str, round_decimals=2):
        """
        Get the latest temperature and humidity measurement for a station.
        
        Args:
            station_id: Station identifier (e.g., 'KSFO', '0579W')
            round_decimals: Number of decimal places to round numeric values
        
        Returns:
            DataFrame with one row containing columns: ['measurement_timestamp', 'temperature', 'humidity', 'wind_speed', 'station_id']
        """
        url = f"{self.base_url}/stations/{station_id}/observations/latest"
        
        response = self.session.get(url)
        response.raise_for_status()
        data = response.json()
        
        # Extract properties from the latest observation
        props = data.get('properties', {})
        timestamp = props.get('timestamp')
        temp = props.get('temperature', {}).get('value')
        humidity = props.get('relativeHumidity', {}).get('value')
        wind_speed = props.get('windSpeed', {}).get('value')
        
        # Create a single record
        record = {
            'measurement_timestamp': timestamp,
            'temperature': temp,
            'humidity': humidity,
            'wind_speed': wind_speed,
            'station_id': station_id
        }
        
        df = pd.DataFrame([record])
        
        if not df.empty:
            df['measurement_timestamp'] = pd.to_datetime(df['measurement_timestamp'])
            # Round temperature, humidity, and wind_speed columns to specified decimals if they exist
            numeric_columns = ['temperature', 'humidity', 'wind_speed']
            existing_numeric_columns = [col for col in numeric_columns if col in df.columns]
            if existing_numeric_columns:
                df[existing_numeric_columns] = df[existing_numeric_columns].round(round_decimals)
            #df = df.where(pd.notnull(df), np.nan)
        
        return df

# weather_api = WeatherAPI()

# station_id = '0579W' #KSFO

# df_latest_measurements = weather_api.get_latest_measurement_data(station_id)


# df_stations = weather_api.get_station_metadata(station_id)
# print(df_stations)
# df_measurements = weather_api.get_measurements_data(station_id, start='2025-07-01', end='2025-07-02')
# print(df_measurements)
# weather_api.upsert_station_metadata(df_stations)
# weather_api.upsert_measurements_data(df_measurements)


if __name__ == "__main__":
    weather_api = WeatherAPI()
    station_id = 'KSFO' #KSFO
    station_data = weather_api.get_station_metadata(station_id)
    station_data_df = pd.DataFrame([station_data])
    print(df_stations)
    df_measurements = weather_api.get_measurements_data(station_id, start='2025-07-01', end='2025-07-02')
    print(df_measurements)
    weather_api.upsert_station_metadata(df_stations)
    #weather_api.upsert_measurements_data(df_measurements)
    
    
