"""
Command-Line Interface Utilities for rs_analytics ETL Scripts

This module provides standardized CLI argument parsing and setup:
- Common argument definitions (--lifetime, --start-date, etc.)
- Date range calculation from arguments
- Logging setup for scripts

Usage:
    from scripts.utils.cli import create_etl_parser, setup_script_logging
    
    parser = create_etl_parser("Google Ads ETL")
    parser.add_argument("--custom-arg", help="Custom argument")
    args = parser.parse_args()
    
    logger = setup_script_logging("gads_etl", log_dir, args.verbose)
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple

# Bootstrap: add project root to sys.path so etl.utils can be imported
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from etl.utils import get_project_root
project_root = get_project_root()


def create_etl_parser(
    description: str,
    default_lookback_days: int = 30
) -> argparse.ArgumentParser:
    """
    Create a standardized argument parser for ETL scripts.
    
    Includes common arguments:
    - --lifetime: Extract all available data
    - --start-date: Start date (YYYY-MM-DD)
    - --end-date: End date (YYYY-MM-DD)
    - --lookback-days: Number of days to look back
    - --verbose/-v: Enable verbose logging
    - --dry-run: Validate only, don't extract
    
    Args:
        description: Description for the argument parser
        default_lookback_days: Default lookback period
        
    Returns:
        Configured ArgumentParser instance
        
    Example:
        >>> parser = create_etl_parser("My ETL Pipeline", default_lookback_days=7)
        >>> args = parser.parse_args()
    """
    parser = argparse.ArgumentParser(
        description=description,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python {script}                           # Last {days} days (default)
  python {script} --lifetime                # All available data
  python {script} --lookback-days 90        # Last 90 days
  python {script} --start-date 2024-01-01   # From specific date
  python {script} --dry-run                 # Validate config only
        """.format(script="script.py", days=default_lookback_days)
    )
    
    # Date range options
    date_group = parser.add_argument_group('Date Range Options')
    
    date_group.add_argument(
        "--lifetime",
        action="store_true",
        help="Extract all available lifetime data"
    )
    
    date_group.add_argument(
        "--start-date",
        type=str,
        metavar="YYYY-MM-DD",
        help="Start date in YYYY-MM-DD format"
    )
    
    date_group.add_argument(
        "--end-date",
        type=str,
        metavar="YYYY-MM-DD",
        help="End date in YYYY-MM-DD format (defaults to yesterday)"
    )
    
    date_group.add_argument(
        "--lookback-days",
        type=int,
        default=default_lookback_days,
        metavar="N",
        help=f"Number of days to look back (default: {default_lookback_days})"
    )
    
    # Execution options
    exec_group = parser.add_argument_group('Execution Options')
    
    exec_group.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose (DEBUG) logging"
    )
    
    exec_group.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate configuration only, don't extract data"
    )
    
    return parser


def get_date_range_from_args(
    args: argparse.Namespace,
    lifetime_start: str = "2020-01-01"
) -> Tuple[str, str]:
    """
    Calculate date range from parsed command-line arguments.
    
    This function handles the priority logic for date range arguments:
    1. --lifetime takes precedence
    2. --start-date/--end-date explicit dates
    3. --lookback-days from yesterday
    
    Args:
        args: Parsed argument namespace (must have lifetime, start_date,
              end_date, lookback_days attributes)
        lifetime_start: Start date to use for lifetime extraction
        
    Returns:
        Tuple of (start_date, end_date) in YYYY-MM-DD format
        
    Example:
        >>> args = parser.parse_args(['--lookback-days', '7'])
        >>> start, end = get_date_range_from_args(args)
    """
    # Yesterday is typical end date (today's data may be incomplete)
    yesterday = datetime.now() - timedelta(days=1)
    yesterday_str = yesterday.strftime('%Y-%m-%d')
    
    # Priority 1: Lifetime mode
    if args.lifetime:
        return lifetime_start, yesterday_str
    
    # Priority 2: Explicit start date
    if args.start_date:
        end_date = args.end_date or yesterday_str
        return args.start_date, end_date
    
    # Priority 3: Lookback days
    start = yesterday - timedelta(days=args.lookback_days - 1)
    return start.strftime('%Y-%m-%d'), yesterday_str


def setup_script_logging(
    name: str,
    log_dir: Optional[Path] = None,
    verbose: bool = False
) -> logging.Logger:
    """
    Set up logging for an ETL script.
    
    Creates a logger with:
    - Console handler with appropriate level
    - File handler (if log_dir provided) with daily rotation
    
    Args:
        name: Logger name (typically script name without .py)
        log_dir: Directory for log files (optional)
        verbose: If True, use DEBUG level; otherwise INFO
        
    Returns:
        Configured logger instance
        
    Example:
        >>> logger = setup_script_logging("gads_etl", Path("./logs"), verbose=True)
        >>> logger.info("Starting ETL...")
    """
    from logging.handlers import TimedRotatingFileHandler
    
    log_level = logging.DEBUG if verbose else logging.INFO
    
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_format = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)
    
    # File handler (optional)
    if log_dir:
        log_dir.mkdir(parents=True, exist_ok=True)
        
        log_file = log_dir / f"{name}_{datetime.now().strftime('%Y%m%d')}.log"
        file_handler = TimedRotatingFileHandler(
            log_file,
            when="midnight",
            interval=1,
            backupCount=30,
            encoding="utf-8"
        )
        file_handler.setLevel(log_level)
        file_format = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s"
        )
        file_handler.setFormatter(file_format)
        logger.addHandler(file_handler)
    
    return logger


def print_banner(
    title: str,
    timestamp: bool = True
) -> None:
    """
    Print a formatted banner for script startup.
    
    Args:
        title: Title to display
        timestamp: Whether to include current timestamp
        
    Example:
        >>> print_banner("Google Ads ETL Pipeline")
        ============================================================
        rs_analytics - Google Ads ETL Pipeline
        Started at: 2025-02-02 10:30:00
        ============================================================
    """
    print("=" * 60)
    print(f"rs_analytics - {title}")
    if timestamp:
        print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)


def print_step(step_num: int, description: str) -> None:
    """
    Print a step indicator for script progress.
    
    Args:
        step_num: Step number
        description: Step description
        
    Example:
        >>> print_step(1, "Loading configuration")
        
        Step 1: Loading configuration...
    """
    print(f"\nStep {step_num}: {description}...")


def print_completion(
    success: bool,
    total_rows: int = 0,
    tables_created: Optional[list] = None
) -> None:
    """
    Print a completion summary.
    
    Args:
        success: Whether the operation was successful
        total_rows: Total rows processed
        tables_created: List of table names created
    """
    print("\n" + "=" * 60)
    
    if success:
        print("ETL PIPELINE COMPLETED SUCCESSFULLY")
        print("=" * 60)
        print(f"Total rows extracted: {total_rows:,}")
        
        if tables_created:
            print(f"Tables created: {len(tables_created)}")
            print("")
            print("Created tables:")
            for table in tables_created:
                print(f"  - {table}")
    else:
        print("ETL PIPELINE FAILED")
        print("=" * 60)
        print("Check the logs for error details.")
    
    print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
