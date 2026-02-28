from psycopg2.extensions import connection
import logging
from src.pdf.parse_incidents import get_day_of_week
from datetime import datetime

logger = logging.getLogger(__name__)

def populate_incidents(db: connection, incidents: list[list[list[str]]]) -> int:
    """Populate the database with the incidents."""
    try:

        dttime = incidents[0]
        inc_no = incidents[1]
        loc = incidents[2]
        nature = incidents[3]
        inc_ori = incidents[4]
        temp = []

        # Splitting date and time into separate lists for each page
        dates = [[j.split(' ')[0] for j in i]for i in dttime]
        times = [[j.split(' ')[1] for j in i]for i in dttime]

        # Extracting day of week and hour of day from date and time
        days_of_week = [[get_day_of_week(j) for j in i] for i in dates]
        hours_of_day = [[int(j.split(':')[0]) for j in i] for i in times]

        emsstat = [[1 if j=='EMSSTAT' else 0 for j in i] for i in inc_ori]
        
        # Splitting and re-arranging data into tuple format for easy insertion using executemany
        #incident_num INT, datetime TEXT, day_of_week int, time_of_day int, weather int, location TEXT, location_rank int, side_of_town TEXT, incident_rank int, nature TEXT, emsstat int
        for i in range(len(dttime)): # Total pages in the PDF
            for j in range(len(dttime[i])): # Entries per page
                dt_str = dttime[i][j]  # e.g. "1/2/2026 0:03"
                incident_ts = datetime.strptime(dt_str, '%m/%d/%Y %H:%M')
                temp.append((inc_no[i][j], incident_ts, days_of_week[i][j], hours_of_day[i][j], loc[i][j], nature[i][j], emsstat[i][j]))
        
        with db.cursor() as cur:
            # Data insertion (ON CONFLICT for idempotent runs)
            cur.executemany(
                """INSERT INTO incidents(incident_num, incident_ts, day_of_week, time_of_day, location, nature, emsstat)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (incident_num) DO NOTHING""",
                temp,
            )
            inserted_incidents = cur.rowcount

            # When multiple incidents with same time and location have different emsstat values, set emsstat to 1 for all of them
            cur.execute("""
                UPDATE incidents SET emsstat = 1
                WHERE incident_num IN (
                    SELECT i2.incident_num FROM incidents i1
                    JOIN incidents i2 ON i1.incident_ts = i2.incident_ts AND i1.location = i2.location AND i1.incident_num <> i2.incident_num
                    WHERE i1.emsstat = 1 AND i2.emsstat = 0
                )
            """)
        db.commit()
        return inserted_incidents

    except Exception as e:
        logger.exception("Error populating database: %s", e)
        raise Exception(f"Error populating database: {e}") from e


def update_ranks_incidents(db: connection) -> None:
    """Update the ranks of the incidents."""
    try:
        with db.cursor() as cur:
            # Updating location_rank and incident_rank
            cur.execute("WITH LocationFrequency AS (SELECT location, RANK() OVER (ORDER BY COUNT(*) DESC) AS Rank FROM incidents GROUP BY location) UPDATE incidents SET location_rank = LocationFrequency.Rank FROM LocationFrequency WHERE incidents.location = LocationFrequency.location;")
            cur.execute("WITH NatureFrequency AS (SELECT nature, RANK() OVER (ORDER BY COUNT(*) DESC) AS Rank FROM incidents GROUP BY nature) UPDATE incidents SET incident_rank = NatureFrequency.Rank FROM NatureFrequency WHERE incidents.nature = NatureFrequency.nature;")
        db.commit()
    except Exception as e:
        logger.exception("Error updating ranks: %s", e)
        raise Exception(f"Error updating ranks: {e}") from e