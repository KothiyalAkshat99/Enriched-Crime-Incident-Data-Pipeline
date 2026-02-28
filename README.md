# Norman PD Incident Data Pipeline

A data pipeline that fetches Norman Police Department daily incident PDFs, extracts structured data, and augments it with geocoding and weather. Supports incremental runs and PostgreSQL.

**Author:** Akshat Kothiyal

---

## What it does

- **Fetch:** Scrapes the Norman PD reports page for incident PDF URLs and downloads each PDF in memory.
- **Extract:** Parses incident tables from PDFs (datetime, incident number, location, nature, ORI) using PyMuPDF.
- **Store:** Writes to **PostgreSQL** with an enriched schema. Re-runs skip duplicates and only process new reports (by latest date in the DB).
- **Augment:** Geocodes locations (Nominatim, cached in DB), fetches historical weather (Open-Meteo), and computes “side of town” (compass direction from Norman center).
- **Output:** Prints the augmented dataset to stdout. Optional CSV export via `python -m src.pipeline.temp`.

For implementation details, schema, and technical decisions, see **TECHNICAL.md**.

---

## Prerequisites

- Python 3.10+
- PostgreSQL (local or Docker)

---

## Install

```bash
pip install -r requirements.txt
```

---

## Configuration

Create a `.env` file with at least:

```env
DATABASE_URL=postgresql://user:password@host:5432/normanpd
```

Optional: `LOG_LEVEL`, `LOG_FILE`, `TOWN_CENTER`. For Docker Compose, also set `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `POSTGRES_PORT`, `POSTGRES_HOST=db`.

---

## Run

**Pipeline (Postgres):**

```bash
python -m src.pipeline.main
```

**With Docker (pipeline + Postgres):**

```bash
docker compose up --build
```

**CSV export:**

```bash
python -m src.pipeline.temp
```

**Legacy (monolithic, SQLite):** `python -m src.main_monolithic --urls files.csv` — uses `resources/normanpd.db`.

---

## Testing

```bash
python -m pytest tests/test_pipeline_minimal.py -v
```

Minimal suite: no DB or network; needs pytest and PyMuPDF. Legacy tests: `python -m pytest tests/test_main.py -v` (monolithic + SQLite).

---

## Project layout

| Path | Purpose |
|------|---------|
| `src/pipeline/` | Orchestrator and CSV export |
| `src/scrape/` | PDF URL scraping |
| `src/pdf/` | Fetch and parse PDFs |
| `src/db/` | Postgres connection, schema, incidents, location cache |
| `src/enrich/` | Weather and side-of-town |
| `tests/test_pipeline_minimal.py` | Minimal tests |
| `tests/test_main.py` | Legacy (monolithic) tests |
| `TECHNICAL.md` | Schema, data flow, and technical decisions |
