import pytest
import main.py as pkg_main
import sys
import warnings

warnings.filterwarnings("ignore")

url = "https://www.normanok.gov/sites/default/files/documents/2024-04/2024-04-03_daily_incident_summary.pdf"

global response, incidents
response = []
incidents = []

# Test for Fetching Data from test URLs
def test_fetchincidents():
    try:
        tresponse = pkg_main.fetchincidents(url)
        response.append(tresponse)
        assert True
        
    except Exception as e:
        print("Error while fetching data from URL "+ str(e), file=sys.stderr)
        assert False


# Test for Extracting Incident Data from test URLs
def test_extractincidents():
    for i in response:
        try:
            tincidents = pkg_main.extractincidents(i)
            incidents.append(tincidents)
            assert True

        except Exception as e:
            print("Error while extracting data "+ str(e), file=sys.stderr)
            assert False


# Test for Creating DB
def test_createdb():
    
    try:
        db = pkg_main.createdb()
        assert db

    except Exception as e:
        print("Error while creating database "+ str(e), file=sys.stderr)
        assert False


# Test for Populating DB
def test_populatedb():
    # Also tests for get_day_of_week() within the same function call
    db = pkg_main.createdb()

    for i in incidents:
        try:
            db = pkg_main.populatedb(db, i)
            assert db
        
        except Exception as e:
            print("Error while inserting into database "+ str(e),file=sys.stderr)
            assert False


# Test for Updating Location and IncidentRanks
def test_updateranks():

    db = pkg_main.createdb()

    try:
        pkg_main.updateranks(db)
        assert True
    
    except Exception as e:
        print("Error while updating ranks "+ str(e),file=sys.stderr)
        assert False


# Test for getting Location and Weather
def test_get_location():
    # Also tests for cached_geocode() within same function call
    db = pkg_main.createdb()

    try:
        pkg_main.get_location(db)
        assert True
    
    except Exception as e:
        print("Error while getting location "+ str(e),file=sys.stderr)
        assert False


# Test for getting Weather
def test_get_weather():
    db = pkg_main.createdb()

    try:
        pkg_main.get_weather(db)
        assert True
    
    except Exception as e:
        print("Error while getting weather "+ str(e),file=sys.stderr)
        assert False


# Test for side of town
def test_side_of_town():
    db = pkg_main.createdb()

    try:
        pkg_main.side_of_town(db)
        assert True
    
    except Exception as e:
        print("Error while getting side of town "+ str(e),file=sys.stderr)
        assert False


# Test for Final Status Check/ Output
def test_output():

    db = pkg_main.createdb()

    try:
        pkg_main.output(db)
        assert True
    
    except Exception as e:
        print("Error while printing to stdout"+ str(e),file=sys.stderr)
        assert False