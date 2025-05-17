"""Logging utilities for Zowatari."""

import os
import sys
from datetime import datetime
from pathlib import Path

from loguru import logger


def setup_logger(log_dir=None):
    """Set up the logger for Zowatari.
    
    Args:
        log_dir: Directory to store log files. Defaults to ~/.zowatari/logs.
    """
    # Clear existing handlers
    logger.remove()
    
    # Set up console logging
    logger.add(
        sys.stderr,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
        level="INFO",
    )
    
    # Set up file logging
    if log_dir is None:
        log_dir = Path.home() / ".zowatari" / "logs"
    
    # Create log directory if it doesn't exist
    os.makedirs(log_dir, exist_ok=True)
    
    # Log file path with timestamp
    log_file = log_dir / f"zowatari_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    logger.add(
        log_file,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
        level="DEBUG",
        rotation="10 MB",
        retention="1 week",
    )
    
    return logger


# Set up the logger for Zowatari
logger = setup_logger()


def get_logger():
    """Get the Zowatari logger."""
    return logger