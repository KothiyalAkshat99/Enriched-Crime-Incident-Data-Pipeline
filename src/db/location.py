import sqlite3
import logging
from geopy.geocoders import Nominatim
import geopy.extra.rate_limiter as rl
from typing import Tuple

geolocator = Nominatim(user_agent="normanpd")
rate_limiter = rl.RateLimiter(geolocator.geocode, min_delay_seconds=1)

logger = logging.getLogger(__name__)

def cache_geocode(address: str, db: sqlite3.Connection) -> None:
    """Cache the latitude and longitude for a given address in the database."""
    try:
        con = db.cursor()
        result = con.execute(f"SELECT loc, latitude, longitude FROM location WHERE loc = ?", (address,)).fetchone()
        
        if result:
            logger.info(f"Cache hit for {address}")
            return
        else:
            loc = rate_limiter(address)
            if loc:
                latitude = loc.latitude
                longitude = loc.longitude
                con.execute("INSERT INTO location (loc, latitude, longitude) VALUES (?, ?, ?)", (address, latitude, longitude))
                db.commit()
                logger.info(f"Location {address} cached for {latitude}, {longitude}")
            else:
                logger.warning(f"No location found for {address}")
    except Exception as e:
        logger.exception(f"Error in geocoding {address}: {e}")

def get_location(db: sqlite3.Connection) -> sqlite3.Connection:
    """Get the latitude and longitude for the locations in the database."""
    logger.info("Fetching latitude and longitude for locations")
    try:
        cur = db.cursor()
        res = cur.execute("SELECT DISTINCT location FROM incidents")
        addresses = [row[0] for row in res.fetchall()]

        for address in addresses:
            cache_geocode(address, db)
    except Exception as e:
        logger.exception(f"Error in getting location: {e}")
        raise Exception(f"Error in getting location: {e}")
    finally:
        return db