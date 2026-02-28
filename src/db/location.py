import logging
from geopy.geocoders import Nominatim
import geopy.extra.rate_limiter as rl
from psycopg2.extensions import connection

geolocator = Nominatim(user_agent="normanpd")
rate_limiter = rl.RateLimiter(geolocator.geocode, min_delay_seconds=1)

logger = logging.getLogger(__name__)

def cache_geocode(address: str, db: connection) -> None:
    """Cache the latitude and longitude for a given address in the database."""
    try:
        with db.cursor() as con:
            con.execute("SELECT loc, latitude, longitude FROM location WHERE loc = %s", (address,))
            result = con.fetchone()

            if result:
                logger.debug("Cache hit for %s", address)
                return
            loc = rate_limiter(address)
            if loc:
                latitude = loc.latitude
                longitude = loc.longitude
                con.execute(
                    "INSERT INTO location (loc, latitude, longitude) VALUES (%s, %s, %s) ON CONFLICT (loc) DO NOTHING",
                    (address, latitude, longitude),
                )
                db.commit()
                logger.info("Location %s cached: lat=%s lon=%s", address, latitude, longitude)
            else:
                logger.warning("No location found for %s", address)
    except Exception as e:
        logger.exception("Error in geocoding %s: %s", address, e)

def get_location(db: connection) -> connection:
    """Get the latitude and longitude for the locations in the database."""
    try:
        with db.cursor() as cur:
            cur.execute("SELECT DISTINCT location FROM incidents")
            addresses = [row[0] for row in cur.fetchall()]
        logger.info("Geocoding %d distinct incident locations", len(addresses))
        for address in addresses:
            cache_geocode(address, db)
    except Exception as e:
        logger.exception("Error in getting location: %s", e)
        raise Exception(f"Error in getting location: {e}") from e
    return db
