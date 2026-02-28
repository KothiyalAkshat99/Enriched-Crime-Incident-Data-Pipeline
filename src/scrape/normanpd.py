import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import re
import logging
from datetime import datetime

from psycopg2.extensions import connection

logger = logging.getLogger(__name__)

def scrape_normanpd_pdf_urls(db: connection) -> tuple[list[str], list[str], list[str]]:
    """Scrape Norman PD PDF URLs from the department activity reports page."""
    
    url = "https://www.normanok.gov/public-safety/police-department/crime-prevention-data/department-activity-reports"
    
    response = requests.get(url)
    incident_pdf_urls = set()
    case_pdf_urls = set()
    arrest_pdf_urls = set()
    
    # Check if the database has the latest PDF URLs
    with db.cursor() as cur:
        cur.execute("SELECT MAX(incident_ts)::date FROM incidents")
        latest_datetime = cur.fetchone()
    
    latest_date_in_db = latest_datetime[0] if latest_datetime else None
    
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        base_url = "https://www.normanok.gov"
        
        daily_incident_pattern = r'/sites/default/files/documents/\d{4}-\d{2}/\d{4}-\d{2}-\d{2}_daily_incident_summary.pdf'
        daily_case_pattern = r'/sites/default/files/documents/\d{4}-\d{2}/\d{4}-\d{2}-\d{2}_daily_case_summary.pdf'
        daily_arrest_pattern = r'/sites/default/files/documents/\d{4}-\d{2}/\d{4}-\d{2}-\d{2}_daily_arrest_summary.pdf'
        
        for link in soup.find_all('a', href=True):
            href = link.get('href', '').strip()
            
            if re.search(daily_incident_pattern, href):
                report_date = re.search(r'\d{4}-\d{2}-\d{2}', href).group(0)
                report_date = datetime.strptime(report_date, '%Y-%m-%d').date()
                if not latest_date_in_db or report_date > latest_date_in_db:
                    incident_pdf_urls.add(urljoin(base_url, href))
                else:
                    logger.info(f"Skipping {href} because it is before {latest_date_in_db}")
            
            if re.search(daily_case_pattern, href):
                report_date = re.search(r'\d{4}-\d{2}-\d{2}', href).group(0)
                report_date = datetime.strptime(report_date, '%Y-%m-%d').date()
                if not latest_date_in_db or report_date > latest_date_in_db:
                    case_pdf_urls.add(urljoin(base_url, href))
                else:
                    logger.info(f"Skipping {href} because it is before {latest_date_in_db}")
            
            if re.search(daily_arrest_pattern, href):
                report_date = re.search(r'\d{4}-\d{2}-\d{2}', href).group(0)
                report_date = datetime.strptime(report_date, '%Y-%m-%d').date()
                if not latest_date_in_db or report_date > latest_date_in_db:
                    arrest_pdf_urls.add(urljoin(base_url, href))
                else:
                    logger.info(f"Skipping {href} because it is before {latest_date_in_db}")
    else:
        logger.exception(f"Error while scraping Norman PD PDF URLs: {response.status_code}")
        raise Exception(f"Error while scraping Norman PD PDF URLs: {response.status_code}")

    logger.info(f"Found {len(incident_pdf_urls)} incident PDF URLs")
    logger.info(f"Found {len(case_pdf_urls)} case PDF URLs")
    logger.info(f"Found {len(arrest_pdf_urls)} arrest PDF URLs")
    
    return list(incident_pdf_urls), list(case_pdf_urls), list(arrest_pdf_urls)