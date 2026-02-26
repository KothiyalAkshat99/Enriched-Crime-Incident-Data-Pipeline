import sqlite3
import logging
from src.pdf.parse_incidents import get_day_of_week

logger = logging.getLogger(__name__)

def populate_incidents(db: sqlite3.Connection, incidents: list[list[list[str]]]) -> None:
    """Populate the database with the incidents."""
    try:
        cur = db.cursor()

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
                temp.append((inc_no[i][j], dttime[i][j], days_of_week[i][j], hours_of_day[i][j], loc[i][j], nature[i][j], emsstat[i][j]))

        # Data insertion
        cur.executemany("INSERT INTO incidents(incident_num, datetime, day_of_week, time_of_day, location, nature, emsstat) VALUES (?, ?, ?, ?, ?, ?, ?)", temp)
        db.commit() # Committing to save changes to DB

        # When multiple incidents with same time and location have different emsstat values, set emsstat to 1 for all of them
        cur.execute("UPDATE incidents SET emsstat = 1 WHERE incident_num in (SELECT i2.incident_num FROM incidents i1 JOIN incidents i2 ON i1.datetime = i2.datetime WHERE (i1.emsstat = 1 AND i2.emsstat = 0) AND i1.location = i2.location AND i1.incident_num <> i2.incident_num);")
        db.commit()

    except Exception as e:
        logger.exception(f"Error populating database: {e}")
        raise Exception(f"Error populating database: {e}")


def update_ranks_incidents(db: sqlite3.Connection) -> None:
    """Update the ranks of the incidents."""
    try:
        cur = db.cursor()

        # Updating location_rank and incident_rank
        cur.execute("WITH LocationFrequency AS (SELECT location, RANK() OVER (ORDER BY COUNT(*) DESC) AS Rank FROM incidents GROUP BY location) UPDATE incidents SET location_rank = LocationFrequency.Rank FROM LocationFrequency WHERE incidents.location = LocationFrequency.location;")
        
        cur.execute("WITH NatureFrequency AS (SELECT nature, RANK() OVER (ORDER BY COUNT(*) DESC) AS Rank FROM incidents GROUP BY nature) UPDATE incidents SET incident_rank = NatureFrequency.Rank FROM NatureFrequency WHERE incidents.nature = NatureFrequency.nature;")

        db.commit()
    except Exception as e:
        logger.exception(f"Error updating ranks: {e}")
        raise Exception(f"Error updating ranks: {e}")