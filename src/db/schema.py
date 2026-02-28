import logging
from psycopg2.extensions import connection

logger = logging.getLogger(__name__)

def create_incident_table(conn: connection) -> None:
    """Create the incident table."""
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS incidents (
                incident_num TEXT PRIMARY KEY,
                datetime TEXT,
                day_of_week INTEGER,
                time_of_day INTEGER,
                weather INTEGER,
                location TEXT,
                location_rank INTEGER,
                side_of_town TEXT,
                incident_rank INTEGER,
                nature TEXT,
                emsstat INTEGER
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_incidents_incident_num ON incidents (incident_num)")
        conn.commit()
    except Exception as e:
        logger.exception(f"Error creating incident table: {e}")
        raise Exception(f"Error creating incident table: {e}") from e

def create_location_table(conn: connection) -> None:
    """Create the location table."""
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS location (
                loc TEXT PRIMARY KEY,
                latitude REAL,
                longitude REAL,
                weather INTEGER
            )
        """)
        conn.commit()
    except Exception as e:
        logger.exception(f"Error creating location table: {e}")
        raise Exception(f"Error creating location table: {e}") from e
