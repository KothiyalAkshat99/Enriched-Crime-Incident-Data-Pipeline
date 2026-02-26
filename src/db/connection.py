import sqlite3
import logging
import os

logger = logging.getLogger(__name__)

def create_connection(db_file: str) -> sqlite3.Connection:
    """Create a connection to the database."""
    os.makedirs("resources", exist_ok=True)
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except sqlite3.Error as e:
        logger.exception(f"Error creating database connection: {e}")
        raise Exception(f"Error creating database connection: {e}")
    return conn

def terminate_connection(conn: sqlite3.Connection) -> None:
    """Terminate a connection to the database."""
    try:
        conn.close()
    except sqlite3.Error as e:
        logger.exception(f"Error terminating database connection: {e}")
        raise Exception(f"Error terminating database connection: {e}")
