"""
Minimal test suite for the Norman PD pipeline (no DB, no live network).
Run from repo root: python -m pytest tests/test_pipeline_minimal.py -v
Requires: pip install -r requirements.txt (PyMuPDF, pytest, etc.)
"""
import io
import re
from unittest.mock import patch, MagicMock

import pytest

# Require project deps so src.pdf.parse_incidents (fitz) can be imported
pytest.importorskip("fitz", reason="PyMuPDF required; install from requirements.txt")


from src.pdf.parse_incidents import get_day_of_week
from src.pdf.fetch_incidents import fetchincidents


# --- get_day_of_week (pure function) ---

def test_get_day_of_week_monday():
    """1/1/2024 is a Monday; encoding 1-7 (1=Sunday, 7=Saturday) -> Monday = 2."""
    assert get_day_of_week("1/1/2024") == 2


def test_get_day_of_week_sunday():
    """12/31/2023 is a Sunday -> 1 in 1-7 encoding."""
    assert get_day_of_week("12/31/2023") == 1


def test_get_day_of_week_saturday():
    """6/1/2024 is a Saturday -> 7."""
    assert get_day_of_week("6/1/2024") == 7


def test_get_day_of_week_invalid_raises():
    """Invalid date string should raise ValueError."""
    with pytest.raises(ValueError):
        get_day_of_week("not-a-date")


# --- Report date from URL (regex used in scraper) ---

def test_report_date_regex_from_incident_url():
    """Date YYYY-MM-DD is extracted from incident PDF URL."""
    url = "https://example.com/documents/2024-01/2024-01-15_daily_incident_summary.pdf"
    match = re.search(r"\d{4}-\d{2}-\d{2}", url)
    assert match is not None
    assert match.group(0) == "2024-01-15"


def test_report_date_regex_malformed_url():
    """URL without YYYY-MM-DD has no match."""
    url = "https://example.com/no-date-here.pdf"
    match = re.search(r"\d{4}-\d{2}-\d{2}", url)
    assert match is None


# --- fetchincidents (mocked network) ---

def test_fetchincidents_returns_bytes_io():
    """fetchincidents returns a BytesIO containing the response body."""
    fake_pdf = b"%PDF-1.4 fake content"
    with patch("src.pdf.fetch_incidents.urlopen") as mock_urlopen:
        mock_resp = MagicMock()
        mock_resp.read.return_value = fake_pdf
        mock_urlopen.return_value = mock_resp

        result = fetchincidents("https://example.com/test.pdf")

    assert isinstance(result, io.BytesIO)
    assert result.read() == fake_pdf


def test_fetchincidents_raises_on_network_error():
    """fetchincidents raises when urlopen fails."""
    with patch("src.pdf.fetch_incidents.urlopen") as mock_urlopen:
        mock_urlopen.side_effect = OSError("Connection refused")

        with pytest.raises(Exception) as exc_info:
            fetchincidents("https://example.com/test.pdf")

        assert "Connection refused" in str(exc_info.value) or "Error fetching" in str(exc_info.value)


# --- Smoke: pipeline importable when full deps (psycopg2, geopy, etc.) are installed ---

def test_pipeline_main_importable():
    """Pipeline main module can be imported (skipped if deps missing)."""
    try:
        from src.pipeline import main
    except ImportError as e:
        pytest.skip(f"Pipeline deps not installed: {e}")
    assert hasattr(main, "run")


def test_pipeline_run_callable():
    """run is callable (skipped if deps missing)."""
    try:
        from src.pipeline.main import run
    except ImportError as e:
        pytest.skip(f"Pipeline deps not installed: {e}")
    assert callable(run)


def test_logging_config_importable():
    """Logging config can be imported and setup_logging is callable."""
    from src.logging_config import setup_logging
    assert callable(setup_logging)
