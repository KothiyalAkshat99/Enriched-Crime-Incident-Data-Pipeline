# Norman PD Incident Data Pipeline

A data pipeline that fetches Norman Police Department daily incident PDFs, extracts structured data, and augments it with geocoding and weather.

**Author:** Akshat Kothiyal

---

## What it does

- **Fetch:** Reads PDF URLs from a CSV file and downloads each PDF in memory.
- **Extract:** Parses incident tables from PDFs (datetime, incident number, location, nature, incident ORI) using PyMuPDF.
- **Store:** Writes to SQLite (`resources/normanpd.db`) with an extended schema: day of week, time of day, location/incident ranks, EMSSTAT, etc.
- **Augment:** Geocodes locations (Nominatim, cached in DB), fetches historical weather (Open-Meteo), and computes “side of town” (compass direction from Norman center).
- **Output:** Prints the augmented dataset to stdout (tab-separated).

The pipeline processes **all URLs** in the given CSV, creates the `resources/` directory if missing, and skips weather/side-of-town for locations where geocoding failed (NULL lat/lon).

---

## Prerequisites

- **Python 3.8+** (tested on 3.12)
- Optional: [Pipenv](https://pipenv.pypa.io/) for environment management

---

## Install

**Option A — pip (recommended for new repo / CI):**

```bash
pip install -r requirements.txt
```

**Option B — Pipenv:**

```bash
pipenv install
```

---

## Run

Provide a CSV file with one PDF URL per line (no header required):

```bash
python main.py --urls files.csv
```

Example `files.csv`:

```
https://www.normanok.gov/sites/default/files/documents/2024-04/2024-04-01_daily_incident_summary.pdf
https://www.normanok.gov/sites/default/files/documents/2024-04/2024-04-02_daily_incident_summary.pdf
```

The database is created at `resources/normanpd.db`; the `resources/` folder is created automatically if it does not exist.

---

## Testing

```bash
pipenv run python -m pytest
# or
python -m pytest
```

Tests cover fetch, extract, DB create/populate, rank updates, geocoding, weather, side-of-town, and output. Geocoding and weather tests may hit live APIs or use cached data.

---

## Project layout

| Path | Purpose |
|------|--------|
| `main.py` | Main pipeline script |
| `files.csv` | Example list of incident PDF URLs |
| `requirements.txt` | Pip-installable dependencies |
| `Pipfile` | Pipenv dependencies (optional) |
| `setup.py` | Package metadata and pytest hook |
| `tests/test_main.py` | Pytest tests |
| `PLANNER.md` | Roadmap and future work (see below) |

---

## Configuration and environment

- **Database path:** `resources/normanpd.db`.
- **Town center** (for “side of town”): Norman, OK `(35.2226, -97.4395)`.
- **Geocoding:** Nominatim (no API key). Optional `.env` can be used for future OpenCage or other keys.

---
