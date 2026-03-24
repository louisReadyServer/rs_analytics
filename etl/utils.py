"""
Shared Utilities for rs_analytics ETL

This module provides common utility functions used across all ETL components:
- Date range calculation and validation
- Path resolution for configs and files
- Logging setup utilities
- Data transformation helpers

Usage:
    from etl.utils import (
        get_date_range,
        resolve_path,
        get_project_root,
        setup_extractor_logging,
    )
"""

import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple

from dotenv import load_dotenv


# ============================================
# Path Utilities
# ============================================

def get_project_root() -> Path:
    """
    Get the project root directory.
    
    Returns:
        Path to the project root (directory containing .env or etl/)
    """
    # Start from this file's location and go up to find project root
    current = Path(__file__).parent.parent.resolve()
    return current


def resolve_path(
    path_str: Optional[str],
    default: str,
    project_root: Optional[Path] = None
) -> Path:
    """
    Resolve a path string to an absolute Path.
    
    If the path is relative, it's resolved relative to the project root.
    If the path is None or empty, the default is used.
    
    Args:
        path_str: Path string from environment variable or config
        default: Default path if path_str is None/empty
        project_root: Project root directory (defaults to get_project_root())
        
    Returns:
        Resolved absolute Path
        
    Example:
        >>> resolve_path("./data/warehouse.duckdb", "./data/warehouse.duckdb")
        Path('/full/path/to/project/data/warehouse.duckdb')
    """
    if project_root is None:
        project_root = get_project_root()
    
    path_str = path_str or default
    path = Path(path_str)
    
    if not path.is_absolute():
        path = (project_root / path).resolve()
    
    return path


def ensure_directory_exists(path: Path) -> Path:
    """
    Ensure a directory exists, creating it if necessary.
    
    Args:
        path: Path to the directory
        
    Returns:
        The path (for chaining)
        
    Raises:
        PermissionError: If directory cannot be created
    """
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)
    return path


# ============================================
# Date Range Utilities
# ============================================

def get_date_range(
    days: Optional[int] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    lifetime: bool = False,
    lifetime_start: str = "2020-01-01",
    default_days: int = 30
) -> Tuple[str, str]:
    """
    Calculate a date range based on various input options.
    
    This function provides a unified way to handle date range parameters
    across all ETL scripts. It supports:
    - Specific start/end dates
    - Number of days to look back
    - Lifetime data extraction
    - Default fallback
    
    Priority order:
    1. If lifetime=True, use lifetime_start to yesterday
    2. If start_date provided, use start_date to end_date (or yesterday)
    3. If days provided, use (today - days) to yesterday
    4. Otherwise, use default_days lookback
    
    Args:
        days: Number of days to look back
        start_date: Explicit start date (YYYY-MM-DD)
        end_date: Explicit end date (YYYY-MM-DD), defaults to yesterday
        lifetime: If True, extract all available data
        lifetime_start: Start date for lifetime extraction
        default_days: Default lookback if nothing specified
        
    Returns:
        Tuple of (start_date, end_date) in YYYY-MM-DD format
        
    Example:
        >>> get_date_range(days=7)
        ('2025-01-26', '2025-02-01')  # Last 7 days ending yesterday
        
        >>> get_date_range(lifetime=True)
        ('2020-01-01', '2025-02-01')  # All data since 2020
    """
    # Yesterday is the typical end date (today's data is usually incomplete)
    yesterday = datetime.now() - timedelta(days=1)
    yesterday_str = yesterday.strftime('%Y-%m-%d')
    
    # Priority 1: Lifetime mode
    if lifetime:
        return lifetime_start, yesterday_str
    
    # Priority 2: Explicit start date
    if start_date:
        effective_end = end_date or yesterday_str
        return start_date, effective_end
    
    # Priority 3: Days lookback
    effective_days = days if days is not None else default_days
    start = yesterday - timedelta(days=effective_days - 1)
    return start.strftime('%Y-%m-%d'), yesterday_str


def validate_date_format(date_str: str) -> bool:
    """
    Validate that a string is in YYYY-MM-DD format.
    
    Args:
        date_str: Date string to validate
        
    Returns:
        True if valid, False otherwise
    """
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        return False


def days_between(start_date: str, end_date: str) -> int:
    """
    Calculate the number of days between two dates.
    
    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        
    Returns:
        Number of days (inclusive)
    """
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    return (end - start).days + 1


# ============================================
# Environment and Configuration
# ============================================

def load_env_file(env_path: Optional[Path] = None) -> bool:
    """
    Load environment variables from .env file.
    
    Searches for .env in multiple locations:
    1. Provided env_path
    2. Current working directory
    3. Project root
    
    Args:
        env_path: Optional explicit path to .env file
        
    Returns:
        True if .env was loaded, False otherwise
    """
    search_paths = []
    
    if env_path:
        search_paths.append(env_path)
    
    search_paths.extend([
        Path.cwd() / ".env",
        get_project_root() / ".env",
    ])
    
    for path in search_paths:
        if path.exists():
            load_dotenv(path)
            return True
    
    return False


def get_env_or_default(
    key: str,
    default: str,
    required: bool = False
) -> str:
    """
    Get an environment variable with a default value.
    
    Args:
        key: Environment variable name
        default: Default value if not set
        required: If True, raise ValueError if not set
        
    Returns:
        Environment variable value or default
        
    Raises:
        ValueError: If required=True and variable not set
    """
    value = os.getenv(key, default)
    
    if required and (not value or value == default):
        raise ValueError(f"Required environment variable {key} is not set")
    
    return value


# ============================================
# Logging Utilities
# ============================================

def setup_extractor_logging(
    name: str,
    log_dir: Optional[Path] = None,
    log_level: str = "INFO",
    add_file_handler: bool = True
) -> logging.Logger:
    """
    Set up logging for an extractor module.
    
    Creates a logger with:
    - Console handler (always)
    - File handler (optional, rotated daily)
    
    Args:
        name: Logger name (typically module or class name)
        log_dir: Directory for log files (required if add_file_handler=True)
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        add_file_handler: Whether to add a file handler
        
    Returns:
        Configured logger instance
    """
    from logging.handlers import TimedRotatingFileHandler
    
    # Get or create logger
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Clear existing handlers to avoid duplicates
    logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, log_level.upper()))
    console_format = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)
    
    # File handler (optional)
    if add_file_handler and log_dir:
        ensure_directory_exists(log_dir)
        
        log_file = log_dir / f"{name}_{datetime.now().strftime('%Y%m%d')}.log"
        file_handler = TimedRotatingFileHandler(
            log_file,
            when="midnight",
            interval=1,
            backupCount=30,
            encoding="utf-8"
        )
        file_handler.setLevel(getattr(logging, log_level.upper()))
        file_format = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s"
        )
        file_handler.setFormatter(file_format)
        logger.addHandler(file_handler)
    
    return logger


# ============================================
# Data Transformation Utilities
# ============================================

def flatten_dict(
    d: dict,
    parent_key: str = '',
    sep: str = '_'
) -> dict:
    """
    Flatten a nested dictionary.
    
    Args:
        d: Dictionary to flatten
        parent_key: Prefix for keys (used in recursion)
        sep: Separator between nested key names
        
    Returns:
        Flattened dictionary
        
    Example:
        >>> flatten_dict({'a': {'b': 1, 'c': 2}})
        {'a_b': 1, 'a_c': 2}
    """
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


