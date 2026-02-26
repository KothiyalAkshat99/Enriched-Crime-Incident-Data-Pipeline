import sqlite3
import logging
from math import radians, cos, sin, atan2, degrees

from src.config import TOWN_CENTER

logger = logging.getLogger(__name__)

def side_of_town(db: sqlite3.Connection) -> None:
    """Calculate the side of town for each location in the database."""
    #Calculate the bearing between two points using the Haversine formula
    town_center = TOWN_CENTER
    if not town_center:
        logger.error("TOWN_CENTER is not set")
        return

    cur = db.cursor()
    locs = cur.execute("SELECT DISTINCT loc, latitude, longitude FROM location").fetchall()

    directions = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']

    for loc, latitude, longitude in locs:
        point = (latitude, longitude)
        if latitude is None or longitude is None:
            logger.warning(f"Latitude or longitude is None for {loc}")
            continue
        lat1, lon1 = map(radians, town_center)
        lat2, lon2 = map(radians, point)

        dLon = lon2 - lon1
        x = cos(lat2) * sin(dLon)
        y = cos(lat1) * sin(lat2) - (sin(lat1) * cos(lat2) * cos(dLon))
        initial_bearing = atan2(x, y)

        bearing = (degrees(initial_bearing) + 360) % 360

        #Map a bearing to a compass direction
        direction = directions[round(bearing / 45) % 8]

        cur.execute("UPDATE incidents SET side_of_town = ? WHERE location = ?", (direction, loc))
    
    db.commit()