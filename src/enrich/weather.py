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
        cur.execute("SELECT DISTINCT incident_ts, location, latitude, longitude FROM incidents JOIN location ON incidents.location = location.loc")
        locations = cur.fetchall()

    for incident_ts, location, latitude, longitude in locations:
        date_str = incident_ts.strftime("%Y-%m-%d") if hasattr(incident_ts, "strftime") else str(incident_ts)[:10]
        hour = incident_ts.hour if hasattr(incident_ts, "hour") else 0

        if latitude is None or longitude is None:
            logger.warning("Latitude or longitude is None for %s on %s at hour %s", location, date_str, hour)
            continue
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": date_str,
            "end_date": date_str,
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
                    cur.execute("UPDATE incidents SET weather = %s WHERE incident_ts = %s AND location = %s", (weather_code, incident_ts, location))
            else:
                logger.warning("No weather data found for %s on %s at hour %s", location, date_str, hour)
                continue
        except Exception as e:
            logger.exception("Error fetching weather data for %s on %s at hour %s: %s", location, date_str, hour, e)
            continue
    db.commit()
