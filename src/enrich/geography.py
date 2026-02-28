import logging
from math import radians, cos, sin, atan2, degrees
from psycopg2.extensions import connection

from src.config import TOWN_CENTER

logger = logging.getLogger(__name__)

def side_of_town(db: connection) -> None:
    """Calculate the side of town for each location in the database."""
    town_center = TOWN_CENTER
    if not town_center:
        logger.error("TOWN_CENTER is not set")
        return

    with db.cursor() as cur:
        cur.execute("SELECT DISTINCT loc, latitude, longitude FROM location")
        locs = cur.fetchall()

    directions = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']

    for loc, latitude, longitude in locs:
        if latitude is None or longitude is None:
            logger.warning("Latitude or longitude is None for %s", loc)
            continue
        point = (latitude, longitude)
        lat1, lon1 = map(radians, town_center)
        lat2, lon2 = map(radians, point)

        dLon = lon2 - lon1
        x = cos(lat2) * sin(dLon)
        y = cos(lat1) * sin(lat2) - (sin(lat1) * cos(lat2) * cos(dLon))
        initial_bearing = atan2(x, y)

        bearing = (degrees(initial_bearing) + 360) % 360
        direction = directions[round(bearing / 45) % 8]

        with db.cursor() as cur:
            cur.execute("UPDATE incidents SET side_of_town = %s WHERE location = %s", (direction, loc))

    db.commit()
