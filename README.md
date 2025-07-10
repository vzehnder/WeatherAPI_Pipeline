# Weather Data Pipeline

A robust Python pipeline for collecting and storing weather station data from the National Weather Service (NWS) API. This project continuously monitors weather stations, downloads historical and real-time measurements, and stores them in a PostgreSQL database.

## Features

- 🌤️ **Real-time Data Collection**: Continuously monitors weather stations for latest measurements
- 📊 **Historical Data**: Downloads historical weather data for specified date ranges
- 🗄️ **Database Storage**: Stores data in PostgreSQL with automatic upsert functionality
- 🔄 **Robust Error Handling**: Built-in retry logic and graceful failure handling
- 📝 **Structured Logging**: Comprehensive logging for monitoring and debugging
- ⚡ **Configurable**: Easy-to-modify settings for different environments

## Project Structure

```
vz_front_task/
├── Pipeline.py                 # Main pipeline execution script
├── utils/
│   ├── weather_api_utils.py   # NWS API client and data processing
│   └── bd_utils.py           # Database utilities and operations
├── logs/                      # Pipeline execution logs
├── requirements.txt           # Python dependencies
└── README.md                 # This file
```

## Prerequisites

### Database Setup

Before running the pipeline, you need to set up a PostgreSQL database with the required tables and view.

#### 1. Create Database Tables

Execute the following SQL commands in your PostgreSQL database:

```sql
-- Create stations table
CREATE TABLE public.stations (
    station_id text NOT NULL,
    "name" text NOT NULL,
    timezone text NULL,
    latitude float8 NULL,
    longitude float8 NULL,
    created_at timestamptz NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamptz NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT stations_pkey PRIMARY KEY (station_id)
);

-- Create stations_measurements table
CREATE TABLE public.stations_measurements (
    station_id text NOT NULL,
    measurement_timestamp timestamptz NOT NULL,
    humidity float8 NULL,
    temperature float8 NULL,
    wind_speed float8 NULL,
    created_at timestamptz NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamptz NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT stations_measurements_pkey PRIMARY KEY (station_id, measurement_timestamp),
    CONSTRAINT stations_measurements_station_id_fkey FOREIGN KEY (station_id) REFERENCES public.stations(station_id) ON DELETE CASCADE
);

-- Create triggers for automatic updated_at column updates
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_stations_updated_at 
    BEFORE UPDATE ON public.stations 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_stations_measurements_updated_at 
    BEFORE UPDATE ON public.stations_measurements 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
```

#### 2. Create Summary View

Create a view for aggregated weather data analysis:

```sql
-- Drop existing view if it exists
DROP VIEW IF EXISTS view_measurements_summary;

-- Create measurements summary view
CREATE VIEW view_measurements_summary AS
WITH
-- CTE for calculating the average temperature for the last full week (Monday to Sunday)
last_week_temp AS (
    SELECT
        station_id,
        ROUND(AVG(temperature)::numeric, 2) AS average_temperature
    FROM
        stations_measurements
    WHERE
        measurement_timestamp >= date_trunc('week', NOW()) - INTERVAL '7 days'
        AND measurement_timestamp < date_trunc('week', NOW())
    GROUP BY
        station_id
),
-- CTE for calculating the maximum wind speed change in the last 7 rolling days
last_7_days_wind AS (
    WITH consecutive_measurements AS (
        SELECT
            station_id,
            ABS(wind_speed - LAG(wind_speed, 1) OVER (PARTITION BY station_id ORDER BY measurement_timestamp)) AS wind_speed_change
        FROM
            stations_measurements
        WHERE
            measurement_timestamp >= NOW() - INTERVAL '7 days'
    )
    SELECT
        station_id,
        MAX(wind_speed_change) AS max_wind_speed_change
    FROM
        consecutive_measurements
    GROUP BY
        station_id
)
-- Final SELECT joining the results from the two CTEs
SELECT
    COALESCE(temp.station_id, wind.station_id) AS station_id,
    temp.average_temperature,
    wind.max_wind_speed_change
FROM
    last_week_temp temp
FULL OUTER JOIN
    last_7_days_wind wind ON temp.station_id = wind.station_id;
```

### Environment Configuration

Create a `.env` file in the project root with your database credentials:

```env
NEON_DB_NAME=your_database_name
NEON_DB_PORT=5432
NEON_DB_HOST=your_database_host
NEON_DB_USER=your_database_user
NEON_DB_PASSWORD=your_database_password
```

## Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd vz_front_task
   ```

2. **Create a virtual environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up your environment variables** (see Environment Configuration above)

## Usage

### Basic Pipeline Execution

Run the pipeline with default settings:

```python
from Pipeline import run_pipeline

# Run with default settings (60 seconds runtime)
result = run_pipeline()
print(f"Pipeline completed: {result}")
```

### Custom Configuration

```python
from datetime import datetime, timedelta

# Run with custom parameters
result = run_pipeline(
    end_date=datetime.now(),
    start_date=datetime.now() - timedelta(days=30),
    max_run_time=3600  # 1 hour
)
```

### Direct Script Execution

```bash
python Pipeline.py
```

## Configuration

The pipeline uses global settings that can be easily modified in `Pipeline.py`:

```python
# Weather stations to monitor
STATIONS = ["0579W", "KSFO"]

# Timing settings
FAILURE_WAIT_TIME = 7          # Seconds to wait after API failures
NEW_REQUEST_WAIT_TIME = 0.1    # Seconds between API requests
RECURRENT_DOWNLOAD_WAIT_TIME = 10  # Seconds between continuous downloads
MAX_RETRIES = 2                # Maximum retry attempts for failed requests
```

## Data Flow

1. **Station Metadata Download**: Fetches basic information about weather stations
2. **Historical Data Download**: Downloads historical measurements for the specified date range
3. **Continuous Monitoring**: Repeatedly downloads latest measurements until max runtime is reached
4. **Database Storage**: All data is automatically upserted into PostgreSQL tables

## Database Schema

### Tables

- **`stations`**: Weather station metadata (ID, name, location, timezone)
- **`stations_measurements`**: Weather measurements (temperature, humidity, wind speed)

### View

- **`view_measurements_summary`**: Aggregated data showing:
  - Average temperature for the last full week
  - Maximum wind speed change over the last 7 days

## Monitoring and Logging

The pipeline automatically logs all activities to `logs/pipeline.txt` with structured logging:

```
2025-07-10 18:43:42,002 - INFO - Starting weather data pipeline
2025-07-10 18:43:49,090 - INFO - Successfully upserted 2 records
2025-07-10 18:43:52,913 - INFO - Successfully upserted 224 records
```

## Error Handling

The pipeline includes robust error handling:

- **Retry Logic**: Failed API calls are retried up to `MAX_RETRIES` times
- **Graceful Degradation**: Failed stations are tracked and skipped in subsequent iterations
- **Structured Logging**: All errors are logged with appropriate levels

## API Integration

This project integrates with the [National Weather Service API](https://www.weather.gov/documentation/services-web-api):

- **No API Key Required**: Public service provided by the US Government
- **Rate Limiting**: Built-in delays to respect API limits
- **User Agent**: Proper identification for security monitoring

## Example Output

```python
result = run_pipeline(max_run_time=60)

# Returns:
{
    'runtime_seconds': 62.35,
    'iterations': 4,
    'failed_stations': [],
    'successful_stations': ['0579W', 'KSFO']
}
```

## Resources

- [NWS API Documentation](https://www.weather.gov/documentation/services-web-api)
- [API Base URL](https://api.weather.gov)
- [OpenAPI Specification](https://api.weather.gov/openapi.json)