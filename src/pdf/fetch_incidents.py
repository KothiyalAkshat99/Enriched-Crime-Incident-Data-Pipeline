import io
from urllib.request import urlopen
import logging

logger = logging.getLogger(__name__)

def fetchincidents(url: str) -> io.BytesIO:
    
    try:
        response = urlopen(url).read()
    except Exception as e:
        logger.exception("Error fetching incidents from %s: %s", url, e)
        raise Exception(f"Error fetching incidents from {url}: {e}")

    response = io.BytesIO(response) # In-Memory Binary Stream, can be read like a file

    return response