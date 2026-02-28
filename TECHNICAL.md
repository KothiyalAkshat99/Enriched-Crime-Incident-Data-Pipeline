## Technical documentation

**Project:** Norman PD Incident Data Pipeline  
**Purpose:** Ingest Norman PD “Daily Incident Summary” PDFs, extract structured rows, enrich with geocoding + historical weather, and persist an augmented dataset in **PostgreSQL** for analysis and future dashboard/API use.

---

## Key technical decisions

| Decision | Rationale |
|----------|-----------|
| **PostgreSQL** | Primary DB; supports proper timestamps and ordering. Schema uses `incident_ts TIMESTAMP` for correct "latest date" and sorting. |
| **`incident_ts` (TIMESTAMP)** | Incident time stored as timestamp (not text) so "latest date in DB" and ordering are correct; PDF parsing once at insert. |
| **Skip by latest date** | No separate table: query `MAX(incident_ts)::date`, only process URLs whose report date is after that. |
| **Idempotent inserts** | `INSERT ... ON CONFLICT (incident_num) DO NOTHING` so re-runs do not duplicate or fail. |
| **Record tracking** | Log per-URL extracted/inserted and run summary; after enrichment, log NULL counts for weather, location_rank, side_of_town. |
| **Structured logging** | Root logger, `%(name)s`; `%s`-style messages; `LOG_LEVEL` / `LOG_FILE` from env. |

---

## System overview

This project is a **batch ETL/ELT-style pipeline**:

- **Extract**: Download incident PDF(s) and parse the report table into structured rows.
- **Load**: Insert into **PostgreSQL** (`incidents`). Connection via `DATABASE_URL`. Idempotent inserts (ON CONFLICT DO NOTHING).
- **Incremental discovery**: Scraper only returns PDF URLs whose report date (from URL) is after `MAX(incident_ts)::date` in the DB; empty table → all URLs.
- **Transform/Enrich**:
  - **Location caching**: Geocode distinct `incidents.location` strings and cache coordinates in `location`.
  - **Weather**: Fetch hourly historical weather codes from Open-Meteo for each distinct `(datetime, location)` and update `incidents.weather`.
  - **Geography**: Compute a compass “side of town” for each location based on a fixed Norman city center, then update `incidents.side_of_town`.
- **Output**: Print the final augmented dataset to stdout (tab-separated).

The “source of truth” is PostgreSQL; enrichment updates rows in place.

---

## Repository layout (current)

The codebase is modularized under `src/`:

- **`src/pipeline/main.py`**: orchestration entrypoint (recommended runner)
- **`src/scrape/normanpd.py`**: scrapes the Norman website for PDF URLs
- **`src/pdf/fetch_incidents.py`**: downloads PDF content into an in-memory stream
- **`src/pdf/parse_incidents.py`**: parses incident PDF(s) into lists of fields
- **`src/db/connection.py`**: opens/closes PostgreSQL connections (psycopg2, `DATABASE_URL`)
- **`src/db/schema.py`**: creates tables/indexes
- **`src/db/incidents.py`**: inserts incident rows and updates ranks
- **`src/db/location.py`**: geocodes and caches `(location string -> lat/lon)` into `location`
- **`src/enrich/weather.py`**: fetches weather and updates incidents
- **`src/enrich/geography.py`**: computes side-of-town and updates incidents
- **`src/config.py`**: basic configuration values from environment
- **`src/logging_config.py`**: logging setup (root logger)

Legacy/reference implementation:

- **`src/main_monolithic.py`**: earlier all-in-one script (kept for reference and legacy tests)

---

## How the pipeline runs (data flow)

The orchestrator (`src/pipeline/main.py`) runs these steps:

1. **Logging configuration**
   - `setup_logging()` configures the root logger using `LOG_LEVEL` and `LOG_FILE`.

2. **DB connection and schema**
   - `create_connection()` opens PostgreSQL via `DATABASE_URL`.
   - `create_incident_table(conn)` + `create_location_table(conn)` create tables and indexes.

3. **Discover incident PDFs**
   - `scrape_normanpd_pdf_urls(conn)` gets latest date from DB, then scrapes the “Department Activity Reports” page and returns three lists:
     - incident PDFs
     - case PDFs (future)
     - arrest PDFs (future)
   - Current pipeline processes **incident PDFs only**.

4. **Fetch and parse**
   - For each incident URL:
     - `fetchincidents(url)` downloads the PDF bytes.
     - `extract_incidents(pdf_bytes)` parses each PDF page’s blocks into table rows.

5. **Load into DB**
   - `populate_incidents(conn, incidents)` parses datetime to `incident_ts` (TIMESTAMP), derives day_of_week, time_of_day, emsstat; `INSERT ... ON CONFLICT (incident_num) DO NOTHING`; EMSSTAT update for same-time/location pairs. Returns inserted count.

6. **Run summary**
   - Logs inserted this run and total rows in `incidents`.

7. **Ranking transforms**
   - `update_ranks_incidents(conn)` updates:
     - `location_rank`: rank locations by frequency
     - `incident_rank`: rank natures by frequency

8. **Geocode and cache**
   - `get_location(conn)` → distinct locations; `cache_geocode()` checks `location`, on miss calls Nominatim, INSERT with ON CONFLICT DO NOTHING.

9. **Weather enrichment**
   - `get_weather(conn)` queries distinct `(incident_ts, location, latitude, longitude)`; Open-Meteo per row; `UPDATE incidents SET weather = %s WHERE incident_ts = %s AND location = %s`.

10. **Side-of-town enrichment**
    - `side_of_town(conn)` reads location coords, computes bearing from TOWN_CENTER, updates `incidents.side_of_town`.

11. **Enrichment health**
    - Logs counts of rows with NULL weather, location_rank, side_of_town.

12. **Output**
    - Optional stdout; CSV export in `src.pipeline.temp`.

---

## Database schema (PostgreSQL)

Connection string: `DATABASE_URL` (e.g. `postgresql://user:password@host:5432/normanpd`).

### `incidents` table

Created by `src/db/schema.py`:

- `incident_num` (TEXT, PRIMARY KEY) — from PDF; format like `2026-00000205`
- `incident_ts` (TIMESTAMP) — parsed from PDF datetime string at insert; used for "latest date" and ordering
- `day_of_week` (INTEGER)
- `time_of_day` (INTEGER, hour 0–23)
- `weather` (INTEGER; Open-Meteo weathercode)
- `location` (TEXT; raw location string from PDF)
- `location_rank` (INTEGER; frequency rank)
- `side_of_town` (TEXT; one of N/NE/E/SE/S/SW/W/NW)
- `incident_rank` (INTEGER; nature frequency rank)
- `nature` (TEXT)
- `emsstat` (INTEGER; 1/0 derived from ORI column)

Indexes: `idx_incidents_incident_num`, `idx_incidents_incident_ts` (for `MAX(incident_ts)::date` and ordering).

### `location` table

- `loc` (TEXT, PRIMARY KEY) — exact location string; join key with `incidents.location`
- `latitude` (REAL)
- `longitude` (REAL)
- `weather` (INTEGER; reserved)

Join: `incidents.location = location.loc`.

---

## Geocoding (Nominatim) and caching

Implementation: `src/db/location.py`

- A location string is looked up in `location` first (cache hit).
- On cache miss:
  - calls Nominatim geocode (rate-limited)
  - INSERTs coordinates into `location`
  - commits

Notes:

- Nominatim requires courteous usage. Keep rate limiting enabled (currently 1 request/second).
- Use a descriptive `user_agent` string (and optionally contact info) for Nominatim.

---

## Weather enrichment (Open-Meteo) with cache + retries

Implementation: `src/enrich/weather.py`

- Uses `requests_cache.CachedSession('.cache', expire_after=-1)`:
  - HTTP responses are cached on disk in `.cache/`
  - `expire_after=-1` means “never expire” (cache survives across runs if you mount `.cache/`)
- Wraps the cached session with retries via `retry_requests.retry(...)`:
  - transient failures get retried with exponential backoff

Important correctness detail:

- Weather updates must be per `(incident_ts, location)`:
  - `UPDATE incidents SET weather = %s WHERE incident_ts = %s AND location = %s`
  - Using only `incident_ts` could overwrite weather across different locations.

---

## Side of town computation

Implementation: `src/enrich/geography.py`

- Uses a fixed `TOWN_CENTER` (lat/lon) from `src/config.py`.
- Computes bearing and maps it to 8 compass directions:
  - N, NE, E, SE, S, SW, W, NW

---

## Logging

Implementation: `src/logging_config.py`

- `setup_logging()` configures the **root logger**:
  - rotating file handler (`LOG_FILE`, default `app.log`)
  - console handler (stderr/stdout)
  - level via `LOG_LEVEL` (default INFO)

In every module:

- `logger = logging.getLogger(__name__)`
- Use `logger.info(...)`, `logger.warning(...)`, `logger.exception(...)`

Because the root logger is configured, module loggers automatically emit to the same handlers.

---

## Configuration

Implementation: `src/config.py` and `.env` (loaded by `python-dotenv` in `connection.py`).

Environment variables:

- **`DATABASE_URL`** — PostgreSQL connection string (e.g. `postgresql://user:password@host:5432/normanpd`). Required for the pipeline. For Docker Compose, the pipeline service gets this from env with `POSTGRES_HOST=db`.
- **`LOG_LEVEL`** (e.g. `INFO`, `DEBUG`)
- **`LOG_FILE`** (e.g. `app.log`)
- **`TOWN_CENTER`** — optional; default in code is `(35.2226, -97.4395)` (Norman, OK).

---

## Running locally

Recommended:

```bash
python -m src.pipeline.main
```

Artifacts:

- DB: `resources/normanpd.db`
- Weather cache: `.cache/` (optional)
- Logs: `app.log` (or `LOG_FILE`)

---

## Running with Docker

Docker runs the modular pipeline entrypoint:

```bash
docker build -t normanpd-pipeline .
docker run --rm -v "%cd%\resources:/app/resources" -v "%cd%\.cache:/app/.cache" normanpd-pipeline
```

Compose:

```bash
docker compose run --rm normanpd-pipeline
```

---

## Extending the project

### Adding cases and arrests

The scraper already returns case and arrest PDF URLs. To add full support:

- Add parsers:
  - `src/pdf/parse_cases.py`
  - `src/pdf/parse_arrests.py`
- Add DB schema:
  - `cases` and `arrests` tables in `src/db/schema.py`
- Add loaders:
  - `src/db/cases.py`, `src/db/arrests.py`
- Extend the orchestrator to run those flows (or generalize into “report type” plugins).

### Backend + dashboard

- **Simple dashboard (Streamlit)** or **API (FastAPI/Flask)** reading from PostgreSQL. The pipeline is the single writer; APIs read via `DATABASE_URL` or a read-only user.
- Legacy **SQLite** path (`src.main_monolithic`) remains for reference; for new work, use the Postgres pipeline.

---

## Troubleshooting

- **No PDFs found:** Norman site content can change; check `src/scrape/normanpd.py` regex patterns.
- **Nominatim failures / 429:** reduce request rate, cache results, and ensure User-Agent is descriptive.
- **Weather too slow:** ensure `.cache/` is mounted/persisted; cache should make re-runs much faster.
- **DB is empty:** confirm `DATABASE_URL` is correct and tables exist; confirm PDF parsing is extracting rows. Inspect logs (`app.log`) for connection, parsing, geocode, or weather errors.
- **No new PDFs processed:** scraper only includes URLs after `MAX(incident_ts)::date`; if the DB already has the latest report date, no URLs are returned. Check logs for "Skipping ... (report date before ...)".

