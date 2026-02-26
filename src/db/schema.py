import sqlite3
import logging

logger = logging.getLogger(__name__)

def create_incident_table(conn: sqlite3.Connection) -> None:
    """Create the incident table."""
    cur = conn.cursor()
    try:
        cur.execute("CREATE TABLE IF NOT EXISTS incidents (incident_num int PRIMARY KEY, datetime TEXT, day_of_week int, time_of_day int, weather int, location TEXT, location_rank int, side_of_town TEXT, incident_rank int, nature TEXT, emsstat int);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_incidents_incident_num ON incidents (incident_num);")
        conn.commit()
    except sqlite3.Error as e:
        logger.exception(f"Error creating incident table: {e}")
        raise Exception(f"Error creating incident table: {e}")

def create_location_table(conn: sqlite3.Connection) -> None:
    """Create the location table."""
    cur = conn.cursor()
    try:
        cur.execute("CREATE TABLE IF NOT EXISTS location (loc TEXT, latitude FLOAT, longitude FLOAT, weather int);")
        conn.commit()
    except sqlite3.Error as e:
        logger.exception(f"Error creating location table: {e}")
        raise Exception(f"Error creating location table: {e}")
