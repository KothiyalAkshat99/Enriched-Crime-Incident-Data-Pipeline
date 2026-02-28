import logging
from geopy.geocoders import Nominatim
import geopy.extra.rate_limiter as rl
from psycopg2.extensions import connection

# Nominatim can be slow or rate-limited; use a longer timeout to avoid ReadTimeoutError (geopy default is 1s)
GEOCODE_TIMEOUT = 10
geolocator = Nominatim(user_agent="normanpd-incident-pipeline-geocoder", timeout=GEOCODE_TIMEOUT)
rate_limiter = rl.RateLimiter(geolocator.geocode, min_delay_seconds=1)

# For intersection-style addresses (e.g. "VINE ST / S BERRY RD"), geocoding each side with locality often works
LOCALITY_SUFFIX = ", Norman, OK, USA"
INTERSECTION_SEP = " / "

logger = logging.getLogger(__name__)


def _geocode_with_intersection_fallback(address: str):
    """Try full address, then each side of ' / ' with locality if that fails."""
    loc = rate_limiter(address)
    if loc:
        return loc
    if INTERSECTION_SEP not in address:
        return None
    parts = [p.strip() for p in address.split(INTERSECTION_SEP, 1) if p.strip()]
    for part in parts:
        query = part + LOCALITY_SUFFIX
        loc = rate_limiter(query)
        if loc:
            logger.debug("Geocoded intersection '%s' via part '%s'", address, part)
            return loc
    return None


def cache_geocode(address: str, db: connection) -> None:
    """Cache the latitude and longitude for a given address in the database."""
    try:
        with db.cursor() as con:
            con.execute("SELECT loc, latitude, longitude FROM location WHERE loc = %s", (address,))
            result = con.fetchone()

            if result:
                logger.debug("Cache hit for %s", address)
                return
            loc = _geocode_with_intersection_fallback(address)
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
