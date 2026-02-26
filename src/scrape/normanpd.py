import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import re
import logging

logger = logging.getLogger(__name__)

def scrape_normanpd_pdf_urls() -> tuple[list[str], list[str], list[str]]:
    """Scrape Norman PD PDF URLs from the department activity reports page."""
    
    url = "https://www.normanok.gov/public-safety/police-department/crime-prevention-data/department-activity-reports"
    
    response = requests.get(url)
    incident_pdf_urls = set()
    case_pdf_urls = set()
    arrest_pdf_urls = set()
    
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        base_url = "https://www.normanok.gov"
        
        daily_incident_pattern = r'/sites/default/files/documents/\d{4}-\d{2}/\d{4}-\d{2}-\d{2}_daily_incident_summary.pdf'
        daily_case_pattern = r'/sites/default/files/documents/\d{4}-\d{2}/\d{4}-\d{2}-\d{2}_daily_case_summary.pdf'
        daily_arrest_pattern = r'/sites/default/files/documents/\d{4}-\d{2}/\d{4}-\d{2}-\d{2}_daily_arrest_summary.pdf'
        
        for link in soup.find_all('a', href=True):
            href = link.get('href', '').strip()
            if re.search(daily_incident_pattern, href):
                incident_pdf_urls.add(urljoin(base_url, href))
            if re.search(daily_case_pattern, href):
                case_pdf_urls.add(urljoin(base_url, href))
            if re.search(daily_arrest_pattern, href):
                arrest_pdf_urls.add(urljoin(base_url, href))
    else:
        logger.exception(f"Error while scraping Norman PD PDF URLs: {response.status_code}")
        raise Exception(f"Error while scraping Norman PD PDF URLs: {response.status_code}")

    logger.info(f"Found {len(incident_pdf_urls)} incident PDF URLs")
    logger.info(f"Found {len(case_pdf_urls)} case PDF URLs")
    logger.info(f"Found {len(arrest_pdf_urls)} arrest PDF URLs")
    
    return list(incident_pdf_urls), list(case_pdf_urls), list(arrest_pdf_urls)