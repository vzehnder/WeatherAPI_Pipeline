import requests
import json
from typing import Dict, List, Optional, Tuple
import pandas as pd
from datetime import datetime, timedelta



base_url = "https://api.weather.gov"
session = requests.Session()
session.headers.update({
    'User-Agent': 'WeatherApp/1.0 (contact@example.com)',
    'Accept': 'application/geo+json'
})

def create_time_str_filters(date_input):
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

def get_station_metadata(station_id: str):
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
    url = f"{base_url}/stations/{station_id}"
    response = session.get(url)
    response.raise_for_status()
    data = response.json()
    
    data["properties"]
    output_data = {}
    output_data['station_id'] = station_id
    output_data['station_name'] = data['properties']['name']
    output_data['station_timezone'] = data['properties']['timeZone']
    output_data['station_latitude'] = data['geometry']['coordinates'][1]
    output_data['station_longitude'] = data['geometry']['coordinates'][0]
    return output_data

def get_measurements_data(station_id: str, 
                          start=None, 
                          end=None,
                          ):
    """
    Get temperature and humidity measurements for a station in a given period.
    
    Args:
        station_id: Station identifier (e.g., 'KSFO', '0579W')
        start: Start datetime (string, datetime, or date object)
        end: End datetime (string, datetime, or date object)
    
    Returns:
        DataFrame with columns: ['timestamp', 'temperature', 'humidity']
    """
    url = f"{base_url}/stations/{station_id}/observations"
    
    params = {}
    if start:
        start_date = create_time_str_filters(start)
        params['start'] = start_date
    if end:
        end_date = create_time_str_filters(end)
        params['end'] = end_date
    
    response = session.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    
    records = []
    for feature in data.get('features', []):
        #feature = data['features'][0]
        props = feature.get('properties', {})
        timestamp = props.get('timestamp')
        temp = props.get('temperature', {}).get('value')
        humidity = props.get('relativeHumidity', {}).get('value')
        wind_speed = props.get('windSpeed', {}).get('value')
        if timestamp is not None and (temp is not None or humidity is not None):
            records.append({
                'timestamp': timestamp,
                'temperature': temp,
                'humidity': humidity,
                'wind_speed': wind_speed
            })
    df = pd.DataFrame(records)
    if not df.empty:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df 

start = "2025-07-01"
end = "2025-07-02"

df_data = get_measurements_data("0579W", start, end)
station_metadata = get_station_metadata("0579W")

df_data.sort_values(by='timestamp', ascending=True)


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
