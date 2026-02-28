import logging
from typing import Sequence

from src.logging_config import setup_logging
from src.scrape.normanpd import scrape_normanpd_pdf_urls
from src.pdf.fetch_incidents import fetchincidents
from src.pdf.parse_incidents import extract_incidents
from src.db.connection import create_connection, terminate_connection
from src.db.schema import create_incident_table, create_location_table
from src.db.incidents import populate_incidents, update_ranks_incidents
from src.db.location import get_location
from src.enrich.weather import get_weather
from src.enrich.geography import side_of_town


logger = logging.getLogger(__name__)


def _output_incidents(db) -> None:
    """Print the final augmented incidents table to stdout."""
    cur = db.cursor()
    res = cur.execute(
        "SELECT day_of_week, time_of_day, weather, location, location_rank, "
        "side_of_town, incident_rank, nature, emsstat FROM incidents;"
    )

    print(
        "Day of the Week\tTime of Day\tWeather\tLocation\tLocation Rank\t"
        "Side of Town\tIncident Rank\tNature\tEMS Status"
    )
    for (
        day_of_week,
        time_of_day,
        weather,
        location,
        location_rank,
        side_of_town,
        incident_rank,
        nature,
        emsstat,
    ) in res.fetchall():
        print(
            f"{day_of_week}\t{time_of_day}\t{weather}\t{location}\t"
            f"{location_rank}\t{side_of_town}\t{incident_rank}\t{nature}\t{emsstat}"
        )


def run() -> None:
    """
    Orchestrate the full Norman PD incident pipeline.

    Scrape the Norman PD activity reports page.
    Fetch, parse, and load incidents.
    Post-process the incidents.
    Output the incidents.
    """
    setup_logging()

    conn = create_connection()
    try:
        # Ensure schema exists
        create_incident_table(conn)
        create_location_table(conn)

        # Scrape the Norman PD activity reports page
        incident_urls, case_urls, arrest_urls = scrape_normanpd_pdf_urls(conn)
        logger.info(
            "Processing %d incident PDFs (cases/arrests not yet handled)",
            len(incident_urls),
        )
        
        # Fetch, parse, and load incidents
        inserted_incidents = 0
        for url in incident_urls:
            logger.info("Fetching incidents from %s", url)
            incident_pdf = fetchincidents(url)
            incidents = extract_incidents(incident_pdf)
            inserted_incidents_this_url = populate_incidents(conn, incidents)
            logger.info(f"Inserted {inserted_incidents_this_url} incidents from {url} into the database")
            inserted_incidents += inserted_incidents_this_url
        logger.info(f"Inserted {inserted_incidents} incidents into the database")

        # Post-processing
        update_ranks_incidents(conn)
        get_location(conn)
        get_weather(conn)
        side_of_town(conn)

        # Final output
        #_output_incidents(conn)
    finally:
        terminate_connection(conn)


if __name__ == "__main__":
    run()

