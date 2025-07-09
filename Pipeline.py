import requests
import json
from typing import Dict, List, Optional, Tuple
import pandas as pd
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import sys
import time

load_dotenv()
cwd = os.getcwd()
sys.path.append(cwd)

from utils.weather_api_utils import WeatherAPI


weather_api = WeatherAPI()

# stations_to_get = ["0579W", "KSFO"]
# failure_wait_time = 7
# new_request_wait_time = 0.1
# recurrent_download_wait_time = 10
# end_date = datetime.now() 
# start_date = end_date - timedelta(days=7)
# end_date = end_date.strftime("%Y-%m-%d")
# start_date = start_date.strftime("%Y-%m-%d")
# max_run_time = 3600


# failed_stations = []

def pipeline_start_downloads(end_date, start_date, stations_to_get,
                             failure_wait_time = 7, new_request_wait_time = 0.1, recurrent_download_wait_time = 10):
    failed_stations = []
    
    df_metadata_list = []
    for station_id in stations_to_get:
        try:
            station_data = weather_api.get_station_metadata(station_id)
            df_metadata = pd.DataFrame([station_data])
            df_metadata_list.append(df_metadata)
            time.sleep(new_request_wait_time)
        except Exception as e:
            print(f"Error getting station data for {station_id}: {e}")
            print(f"Waiting {failure_wait_time} seconds for next try")
            time.sleep(failure_wait_time)
            try:
                station_data = weather_api.get_station_metadata(station_id)
                df_metadata = pd.DataFrame([station_data])
                df_metadata_list.append(df_metadata)
                time.sleep(new_request_wait_time)
            except Exception as e:
                print(f"Second try failed, error getting station data for {station_id}: {e}")
                print(f"No more retries for this station.")
                print(f"Waiting {recurrent_download_wait_time} seconds for next station.")
                time.sleep(recurrent_download_wait_time)
                failed_stations.append(station_id)
                continue

    if len(df_metadata_list) > 0:
        df_metadata = pd.concat(df_metadata_list)
        if not df_metadata.empty:
            weather_api.upsert_station_metadata(df_metadata)
        else:
            print("No data found for all stations, exiting.")
            exit()
    else:
        print("No data found for any station, exiting.")
        exit()

    df_measurements_list = []
    for station_id in stations_to_get:
        if station_id in failed_stations:
            print(f"Skipping {station_id} because it failed to get metadata.")
            continue
        try:
            df_measurements = weather_api.get_measurements_data(station_id, start=start_date, end=end_date)
            time.sleep(new_request_wait_time)
            if not df_measurements.empty:
                df_measurements_list.append(df_measurements)
        except Exception as e:
            print(f"Error getting measurements data for {station_id}: {e}")
            print(f"Waiting {recurrent_download_wait_time} seconds for next station.")
            time.sleep(recurrent_download_wait_time)
            try:
                df_measurements = weather_api.get_measurements_data(station_id, start=start_date, end=end_date)
                time.sleep(new_request_wait_time)
                if not df_measurements.empty:
                    df_measurements_list.append(df_measurements)
            except Exception as e:
                print(f"Second try failed, error getting measurements data for {station_id}: {e}")
                print(f"No more retries for this station.")
                print(f"Waiting {recurrent_download_wait_time} seconds for next station.")
                time.sleep(recurrent_download_wait_time)
                failed_stations.append(station_id)
            continue

    if len(df_measurements_list) > 0:
        df_measurements = pd.concat(df_measurements_list)
        if not df_measurements.empty:
            weather_api.upsert_measurements_data(df_measurements)
        else:
            print("No data found for all stations, exiting.")
            exit()
    else:
        print("No data found for any station, exiting.")
        exit()
        
    return failed_stations

def pipeline_recurrent_downloads(end_date, start_date, stations_to_get, failed_stations = [],
                                 failure_wait_time = 7,
                                 new_request_wait_time = 0.1,
                                 recurrent_download_wait_time = 10):
    df_latest_measurements_list = []
    for station_id in stations_to_get:
        if station_id in failed_stations:
            print(f"Skipping {station_id} because it failed to get metadata.")
            continue
        try:
            df_latest_measurements = weather_api.get_latest_measurement_data(station_id)
            time.sleep(new_request_wait_time)
            if not df_latest_measurements.empty:
                df_latest_measurements_list.append(df_latest_measurements)
        except Exception as e:
            print(f"Error getting latest measurements data for {station_id}: {e}")
            print(f"Waiting {failure_wait_time} seconds for next station.")
            time.sleep(failure_wait_time)
            try:
                df_latest_measurements = weather_api.get_latest_measurement_data(station_id)
                time.sleep(new_request_wait_time)
                if not df_latest_measurements.empty:
                    df_latest_measurements_list.append(df_latest_measurements)
            except Exception as e:
                print(f"Second try failed, error getting latest measurements data for {station_id}: {e}")
                print(f"No more retries for this station.")
                print(f"Waiting {failure_wait_time} seconds for next station.")
                time.sleep(failure_wait_time)
            failed_stations.append(station_id)
            continue

    if len(df_latest_measurements_list) > 0:
        df_latest_measurements = pd.concat(df_latest_measurements_list)
        if not df_latest_measurements.empty:
            weather_api.upsert_measurements_data(df_latest_measurements)
        else:
            print("No data found for all stations, exiting.")
            exit()
    else:
        print("No data found for any station, exiting.")
        exit()

def run_pipeline(end_date=datetime.now(),
                 start_date=datetime.now() - timedelta(days=7),
                 stations_to_get=["0579W", "KSFO"],
                 max_run_time=60,
                 failure_wait_time=7,
                 new_request_wait_time=0.1,
                 recurrent_download_wait_time=10,
                 failed_stations = []):
    start_time = time.time()
    failed_stations = pipeline_start_downloads(end_date, start_date, stations_to_get,
                                                failure_wait_time, new_request_wait_time, recurrent_download_wait_time)
    while time.time() - start_time < max_run_time:
        failed_stations_new = pipeline_recurrent_downloads(end_date, start_date, stations_to_get, failed_stations,
                                                            failure_wait_time, new_request_wait_time, recurrent_download_wait_time)
        failed_stations = list(set(failed_stations + failed_stations_new))
        time.sleep(recurrent_download_wait_time)
    return failed_stations


if __name__ == "__main__":
    run_pipeline()