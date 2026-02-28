import logging
import csv

from src.logging_config import setup_logging
from src.db.connection import create_connection, terminate_connection

logger = logging.getLogger(__name__)

def run() -> None:
    setup_logging()

    conn = create_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM incidents")
    incidents = cur.fetchall()

    if not incidents:
        logger.warning("No incidents to export; incidents table is empty")
        terminate_connection(conn)
        return

    headers = [col[0] for col in cur.description]
    with open("incidents.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(incidents)
    logger.info("Exported %d incidents to incidents.csv", len(incidents))
    terminate_connection(conn)


if __name__ == "__main__":
    run()