import logging
import os
from logging.handlers import RotatingFileHandler
from src.config import LOG_LEVEL, LOG_FILE

def setup_logging():
    """Configure logging for the pipeline: file + console, level from env."""
    log_level = getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO)
    log_file = os.environ.get("LOG_FILE", "app.log")

    logger = logging.getLogger(__name__)
    logger.setLevel(log_level)
    # Avoid duplicate handlers when main is re-run (e.g. in tests)
    if logger.handlers:
        return

    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # File: append, one log file for the pipeline
    fh = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5)
    fh.setLevel(log_level)
    fh.setFormatter(formatter)
    logger.addHandler(fh)