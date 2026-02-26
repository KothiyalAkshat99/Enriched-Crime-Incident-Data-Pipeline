import logging
import os
from logging.handlers import RotatingFileHandler
from typing import Any

def setup_logging() -> None:
    """Configure logging once for the whole process (file + console)."""
    if getattr(logging, "_pipeline_logging_configured", False):
        return

    log_level = getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO)
    log_file = os.environ.get("LOG_FILE", "app.log")

    root = logging.getLogger()
    root.setLevel(log_level)

    # Avoid duplicate handlers if something configured logging earlier.
    for h in list(root.handlers):
        root.removeHandler(h)

    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    fh = RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8")
    fh.setLevel(log_level)
    fh.setFormatter(formatter)
    root.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setLevel(log_level)
    ch.setFormatter(formatter)
    root.addHandler(ch)

    setattr(logging, "_pipeline_logging_configured", True)