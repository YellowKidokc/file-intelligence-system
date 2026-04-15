"""Centralized logging configuration for FIS."""

import logging
import sys


def get_logger(name: str) -> logging.Logger:
    """Get a logger with consistent formatting for FIS modules."""
    logger = logging.getLogger(f"fis.{name}")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(
            "[%(levelname).1s] %(name)s: %(message)s"
        ))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger
