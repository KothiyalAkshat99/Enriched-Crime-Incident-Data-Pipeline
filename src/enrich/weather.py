import logging
from datetime import datetime
import openmeteo_requests
import requests_cache
from retry_requests import retry
from psycopg2.extensions import connection

_cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
_retry_session = retry(_cache_session, retries = 5, backoff_factor = 0.2)
_openmeteo_client = openmeteo_requests.Client(session = _retry_session)

logger = logging.getLogger(__name__)

def get_weather(db: connection) -> None:
    """Fetch weather data for each location in the database."""
    url = "https://archive-api.open-meteo.com/v1/archive"
    with db.cursor() as cur:
        cur.execute("SELECT DISTINCT datetime, location, latitude, longitude FROM incidents JOIN location ON incidents.location = location.loc")
        locations = cur.fetchall()

    for datetime_str, location, latitude, longitude in locations:

        date = datetime.strptime(datetime_str, '%m/%d/%Y %H:%M').strftime('%Y-%m-%d')
        hour = datetime.strptime(datetime_str, '%m/%d/%Y %H:%M').hour

        if latitude is None or longitude is None:
            logger.warning(f"Latitude or longitude is None for {location} on {date} at hour {hour}")
            continue
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": date,
            "end_date": date,
            "hourly": "weather_code",
            "timezone": "auto"
        }
        try:
            responses = _openmeteo_client.weather_api(url, params=params)
            response = responses[0]

            hourly_weather_code = response.Hourly().Variables(0).ValuesAsNumpy()

            if hour < len(hourly_weather_code):
                weather_code = int(hourly_weather_code[hour])
                with db.cursor() as cur:
                    cur.execute("UPDATE incidents SET weather = %s WHERE datetime = %s AND location = %s", (weather_code, datetime_str, location))
            else:
                logger.warning(f"No weather data found for {location} on {date} at hour {hour}")
                continue
        except Exception as e:
            logger.exception(f"Error fetching weather data for {location} on {date} at hour {hour}: {e}")
            continue
    db.commit()
