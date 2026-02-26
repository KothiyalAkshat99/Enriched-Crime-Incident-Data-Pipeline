import io
import fitz
import logging
from datetime import datetime
from typing import List, Tuple

logger = logging.getLogger(__name__)

def get_day_of_week(date_string: str) -> int:

    # Convert the date string to a datetime object
    date_obj = datetime.strptime(date_string, '%m/%d/%Y')
    
    # Get the day of the week (0 = Monday, 1 = Tuesday, ..., 6 = Sunday)
    day_of_week = date_obj.weekday()
    
    # Re-coding day of week to 1-7 (1 = Sunday and 7 = Saturday)
    day_of_week_number = ((day_of_week + 1) % 7) + 1
    
    return day_of_week_number


def extract_incidents(incident_data: io.BytesIO) -> Tuple[List[List[str]], List[List[str]], List[List[str]], List[List[str]], List[List[str]]]:
    """Extract incidents from a PDF file."""
    doc = fitz.open(stream = incident_data, filetype="pdf") # Using PyMuPDF/ Fitz module for PDF data extraction

    dttime: List[List[str]] = []
    inc_no: List[List[str]] = []
    loc: List[List[str]] = []
    nature: List[List[str]] = []
    inc_ori: List[List[str]] = []

    for page_number in range(len(doc)):

        ls: List[List[str]] = []
        
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

    return dttime, inc_no, loc, nature, inc_ori