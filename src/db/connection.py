import logging
import os
from dotenv import load_dotenv
import psycopg2
from psycopg2.extensions import connection

load_dotenv()

logger = logging.getLogger(__name__)

def create_connection() -> connection:
    """Create a connection to the database."""
    DATABASE_URL = os.getenv("DATABASE_URL")
    if not DATABASE_URL:
        raise Exception("DATABASE_URL is not set")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        logger.debug("Database connection established")
    except psycopg2.Error as e:
        logger.exception("Error creating database connection: %s", e)
        raise Exception(f"Error creating database connection: {e}")
    return conn

def terminate_connection(conn: connection) -> None:
    """Terminate a connection to the database."""
    try:
        conn.close()
    except psycopg2.Error as e:
        logger.exception("Error terminating database connection: %s", e)
        raise Exception(f"Error terminating database connection: {e}")
