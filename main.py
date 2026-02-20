import sys
import argparse
import os
import fitz
from urllib.request import urlopen
import io
import sqlite3
from datetime import datetime
from geopy.geocoders import Nominatim # ignore: import-error
from geopy.adapters import AioHTTPAdapter # ignore: import-error
import asyncio
import aiohttp # ignore: import-error
from geopy.extra.rate_limiter import RateLimiter, AsyncRateLimiter # ignore: import-error
from opencage.geocoder import OpenCageGeocode # ignore: import-error
import pandas as pd
import requests
import openmeteo_requests # ignore: import-error
import requests_cache # ignore: import-error
from retry_requests import retry # ignore: import-error
from math import radians, cos, sin, asin, sqrt, atan2, degrees


def get_day_of_week(date_string):

    # Convert the date string to a datetime object
    date_obj = datetime.strptime(date_string, '%m/%d/%Y')
    #print(date_obj)
    
    # Get the day of the week (0 = Monday, 1 = Tuesday, ..., 6 = Sunday)
    day_of_week = date_obj.weekday()
    
    # Re-coding day of week to 1-7 (1 = Sunday and 7 = Saturday)
    day_of_week_number = ((day_of_week + 1) % 7) + 1
    
    return day_of_week_number


geocode_cache = {}
geolocator = Nominatim(user_agent="cis6930sp24assignment2")
#geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

loc_errors = 0
cache_hits = 0

def cached_geocode(address, db):
    global loc_errors
    global cache_hits
    con = db.cursor()
    result = con.execute(f"SELECT loc, latitude, longitude FROM location WHERE loc = ?", (address,)).fetchone()

    if result:
        cache_hits += 1
        return (result[1], result[2])
    else:
        try:
            loc = geolocator.geocode(address)
            #geores = RateLimiter(geolocator.geocode(address), min_delay_seconds=1)
        except Exception as e:
            #print(f'\nAddress \'{address}\' threw an exception. Latitude/Longitude could not be fetched.')
            loc_errors += 1
            return (None, None)
        if loc:
            latitude = loc.latitude
            longitude = loc.longitude
            con.execute("INSERT INTO location (loc, latitude, longitude) VALUES (?, ?, ?)", (address, latitude, longitude))
            db.commit()
            return (latitude, longitude)
        else:
            return (None, None)


def get_location(db):

    print("\nFetching latitude and longitude for locations")
    cur = db.cursor()

    # Pulling location from DB
    res = cur.execute("SELECT DISTINCT location FROM incidents")

    location = res.fetchall()

    location = [i[0] for i in location]

    df = pd.DataFrame({'location': location})
    try:
        df['point'] = df['location'].apply(lambda x: cached_geocode(x, db))

        df[['latitude', 'longitude']] = pd.DataFrame(df['point'].to_list(), index=df.index)
    except:
        #print("Error in geocoding")
        pass
    
    return db


# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)


def get_weather(db):
    url = "https://archive-api.open-meteo.com/v1/archive"
    cur = db.cursor()
    locations = cur.execute("SELECT DISTINCT datetime, loc, latitude, longitude FROM incidents JOIN location ON incidents.location = location.loc").fetchall()

    for datetime_str, loc, latitude, longitude in locations:
        
        date = datetime.strptime(datetime_str, '%m/%d/%Y %H:%M').strftime('%Y-%m-%d')
        hour = datetime.strptime(datetime_str, '%m/%d/%Y %H:%M').hour
        if latitude is None or longitude is None:
            continue
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": date,
            "end_date": date,
            "hourly": ["weathercode"]
        }
        try:
            responses = openmeteo.weather_api(url, params=params)

            if responses:
                response = responses[0]
                hourly_weather_code = response.Hourly().Variables(0).ValuesAsNumpy()
                
                if hour < len(hourly_weather_code):

                    weather_code = hourly_weather_code[hour]
                    
                    cur.execute("UPDATE incidents SET weather = ? WHERE datetime = ?", (int(weather_code), datetime_str))
                    db.commit()

        except Exception as e:
            print(f"Error fetching weather data for {loc} on {date} at hour {hour}: {e}")

    return db


def side_of_town(db):
    #Calculate the bearing between two points
    town_center = (35.2226, -97.4395)

    cur = db.cursor()
    locs = cur.execute("SELECT DISTINCT loc, latitude, longitude FROM location").fetchall()

    directions = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']

    for loc, latitude, longitude in locs:
        point = (latitude, longitude)
        if latitude is None or longitude is None:
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
        
    return db


def fetchincidents(url):
    #headers = {}
    #headers['User-Agent'] = "Mozilla/5.0 (X11; Linux i686) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.27 Safari/537.17"

    response = urlopen(url).read()
    response = io.BytesIO(response) # In-Memory Binary Stream, can be read like a file

    return response


def extractincidents(incident_data):

    doc = fitz.open(stream = incident_data, filetype="pdf") # Using PyMuPDF/ Fitz module for PDF data extraction

    global dttime, inc_no, loc, nature, inc_ori
    dttime = []
    inc_no = []
    loc = []
    nature = []
    inc_ori = []

    for page_number in range(len(doc)):

        global ls
        ls = []
        
        x = doc[page_number]
        text = x.get_text("blocks")

        if page_number == 0: # Removing extraneous info from first page
            text.pop(0)
            text.pop()
            text.pop()
        elif page_number == len(doc)-1: # Removing extraneous info from last page
            text.pop()
        
        for t in text: # Splitting text list into required columns
            
            temp = t[4].split('\n')
            temp.remove('')
            if(len(temp)<5): # Handling Blank Spaces for 'Nature' column
                temp.insert(2, ' ')
                temp.insert(3, ' ')
            elif(len(temp)>5): # Handling Multi-line 'Location' issues
                temp[2] = temp[2] + temp[3]
                temp.pop(3)
            ls.append(temp)

        dttime.append([sublist[0] for sublist in ls])
        inc_no.append([sublist[1] for sublist in ls])
        loc.append([sublist[2] for sublist in ls])
        nature.append([sublist[3] for sublist in ls])
        inc_ori.append([sublist[4] for sublist in ls])

    incidents = [dttime, inc_no, loc, nature, inc_ori]
    return incidents


def createdb():
    os.makedirs("resources", exist_ok=True)
    try:
        con = sqlite3.connect("resources/normanpd.db") # Creating Database connection
        cur = con.cursor() # Database Cursor
        
        #Creating incident table
        cur.execute("DROP TABLE IF EXISTS incidents;")
        cur.execute("CREATE TABLE incidents (idx TEXT, incident_num int, datetime TEXT, day_of_week int, time_of_day int, weather int, location TEXT, location_rank int, side_of_town TEXT, incident_rank int, nature TEXT, emsstat int);")

        #cur.execute("DROP TABLE IF EXISTS location;")
        cur.execute("CREATE TABLE IF NOT EXISTS location (loc TEXT, latitude FLOAT, longitude FLOAT, weather int);")
        return con
    except Exception as e:
        print(f"Error creating database: {e}")
        return None

pdfnum = 1

def populatedb(db, incidents):
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
        
        k = 1

        global pdfnum

        # Splitting and re-arranging data into tuple format for easy insertion using executemany
        #incident_num INT, datetime TEXT, day_of_week int, time_of_day int, weather int, location TEXT, location_rank int, side_of_town TEXT, incident_rank int, nature TEXT, emsstat int
        for i in range(len(dttime)): # Total pages in the PDF
            for j in range(len(dttime[i])): # Entries per page
                idx = str(pdfnum) + "_" + str(k)
                temp.append((idx, inc_no[i][j], dttime[i][j], days_of_week[i][j], hours_of_day[i][j], loc[i][j], nature[i][j], emsstat[i][j]))
                k += 1

        pdfnum += 1

        # Data insertion
        cur.executemany("INSERT INTO incidents(idx, incident_num, datetime, day_of_week, time_of_day, location, nature, emsstat) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", temp)
        db.commit() # Committing to save changes to DB

        # When multiple incidents with same time and location have different emsstat values, set emsstat to 1 for all of them
        cur.execute("UPDATE incidents SET emsstat = 1 WHERE incident_num in (SELECT i2.incident_num FROM incidents i1 JOIN incidents i2 ON i1.datetime = i2.datetime WHERE (i1.emsstat = 1 AND i2.emsstat = 0) AND i1.location = i2.location AND i1.incident_num <> i2.incident_num);")
        db.commit()

        return db
    except Exception as e:
        print(f"Error populating database: {e}")
        return None


def updateranks(db):
    
    cur = db.cursor()

    # Updating location_rank and incident_rank
    cur.execute("WITH LocationFrequency AS (SELECT location, RANK() OVER (ORDER BY COUNT(*) DESC) AS Rank FROM incidents GROUP BY location) UPDATE incidents SET location_rank = LocationFrequency.Rank FROM LocationFrequency WHERE incidents.location = LocationFrequency.location;")
    
    cur.execute("WITH NatureFrequency AS (SELECT nature, RANK() OVER (ORDER BY COUNT(*) DESC) AS Rank FROM incidents GROUP BY nature) UPDATE incidents SET incident_rank = NatureFrequency.Rank FROM NatureFrequency WHERE incidents.nature = NatureFrequency.nature;")

    db.commit()

    return db


def output(db):

    cur = db.cursor()
    
    res = cur.execute("SELECT day_of_week, time_of_day, weather, location, location_rank, side_of_town, incident_rank, nature, emsstat FROM incidents;")

    print("Day of the Week\tTime of Day\tWeather\tLocation\tLocation Rank\tSide of Town\tIncident Rank\tNature\tEMS Status")

    for t in res.fetchall():
        print(f'{t[0]}\t{t[1]}\t{t[2]}\t{t[3]}\t{t[4]}\t{t[5]}\t{t[6]}\t{t[7]}\t{t[8]}')
    
    db.close() # Closing DB connection


def main():
    try:
        # Argument Parser
        parser = argparse.ArgumentParser(description="Data Augmentation")

        # Arguments
        parser.add_argument('--urls', required=True, help='files.csv')

        args = parser.parse_args()

        urlFile = args.urls
    except:
        print("Error while parsing arguments", file=sys.stderr)
        sys.exit(1)

    db = createdb()
    if not db:
        print("Error while creating database", file=sys.stderr)
        sys.exit(1)

    with open(urlFile) as f:
        filedata = f.read()
        for i in filedata.splitlines():
            url = i.strip()
            if url:
                print("\nFetching URL: " + url)
                urlIncidentData = fetchincidents(url)
                print("Data Fetched")
                incidents = extractincidents(urlIncidentData)
                print("Incidents Extracted")
                db = populatedb(db, incidents)
                if not db:
                    print("Error while populating database", file=sys.stderr)
                    sys.exit(1)
    
    db = updateranks(db)
    if not db:
        print("Error while updating ranks", file=sys.stderr)
        sys.exit(1)

    db = get_location(db)
    if not db:
        print("Error while getting location", file=sys.stderr)
        sys.exit(1)

    print(f"\nError while fetching latitude, longitude for {loc_errors} locations.")
    print(f"\nCache hits: {cache_hits}\n")

    db = get_weather(db)
    if not db:
        print("Error while getting weather", file=sys.stderr)
        sys.exit(1)

    db = side_of_town(db)
    if not db:
        print("Error while getting side of town", file=sys.stderr)
        sys.exit(1)

    output(db)


if __name__ == "__main__":
    main()