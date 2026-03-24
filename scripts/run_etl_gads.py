#!/usr/bin/env python3
"""
Google Ads ETL Pipeline for rs_analytics

This script extracts all available data from Google Ads:
- Campaign performance
- Ad group metrics  
- Keyword performance
- Ad performance
- Device breakdowns
- Geographic data
- Hourly performance
- Conversion tracking
- Lifetime data support

Usage:
    # Pull all lifetime data
    python scripts/run_etl_gads.py --lifetime
    
    # Pull last 30 days (default)
    python scripts/run_etl_gads.py
    
    # Pull specific date range
    python scripts/run_etl_gads.py --start-date 2024-01-01 --end-date 2025-12-31
    
    # Pull last N days
    python scripts/run_etl_gads.py --lookback-days 90

Created Tables:
    - gads_daily_summary: Account-level daily totals
    - gads_campaigns: Campaign performance by date
    - gads_ad_groups: Ad group performance
    - gads_keywords: Keyword performance
    - gads_ads: Individual ad performance
    - gads_devices: Performance by device
    - gads_geographic: Geographic performance
    - gads_hourly: Hourly performance
    - gads_conversions: Conversion action data
"""

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Bootstrap: add project root to sys.path so etl.utils can be imported
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from etl.utils import get_project_root
project_root = get_project_root()

from scripts.utils.cli import create_etl_parser, get_date_range_from_args, setup_script_logging, print_completion
from scripts.utils.db import load_to_duckdb


def main() -> int:
    """Main ETL pipeline execution."""
    parser = create_etl_parser("Google Ads ETL")
    args = parser.parse_args()
    
    print("=" * 60)
    print("rs_analytics - Google Ads ETL Pipeline")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # ========================================
    # Step 1: Load Configuration
    # ========================================
    print("\nStep 1: Loading Google Ads configuration...")
    
    try:
        from etl.gads_config import get_gads_config, get_gads_client
        config = get_gads_config()
        print(f"  [OK] Configuration loaded")
        print(f"  [i] Customer ID: {config.customer_id}")
        print(f"  [i] YAML Path: {config.yaml_path}")
        print(f"  [i] DuckDB: {config.duckdb_path}")
    except Exception as e:
        print(f"  [X] Configuration error: {e}")
        return 1
    
    # Set up logging with config
    logger = setup_script_logging("gads_etl", config.log_dir, args.verbose)
    
    # ========================================
    # Step 2: Determine Date Range
    # ========================================
    logger.info("=" * 50)
    logger.info("Step 2: Determining date range")
    logger.info("=" * 50)
    
    # Calculate dates
    yesterday = datetime.now() - timedelta(days=1)
    
    if args.lifetime:
        # Google Ads can have data going back years - start from 2020
        start_date = "2020-01-01"
        end_date = yesterday.strftime('%Y-%m-%d')
        logger.info("LIFETIME MODE: Extracting all available data from 2020")
    elif args.start_date:
        start_date = args.start_date
        end_date = args.end_date or yesterday.strftime('%Y-%m-%d')
    else:
        start_date = (yesterday - timedelta(days=args.lookback_days - 1)).strftime('%Y-%m-%d')
        end_date = yesterday.strftime('%Y-%m-%d')
    
    logger.info(f"Date range: {start_date} to {end_date}")
    
    # ========================================
    # Step 3: Initialize Client
    # ========================================
    logger.info("=" * 50)
    logger.info("Step 3: Initializing Google Ads client")
    logger.info("=" * 50)
    
    try:
        client = get_gads_client()
        logger.info("  [OK] Google Ads client initialized")
    except Exception as e:
        logger.error(f"  [X] Failed to create Google Ads client: {e}")
        return 2
    
    # ========================================
    # Step 4: Test Connection
    # ========================================
    logger.info("=" * 50)
    logger.info("Step 4: Testing Google Ads connection")
    logger.info("=" * 50)
    
    try:
        from etl.gads_extractor import GAdsExtractor
        
        extractor = GAdsExtractor(
            client=client,
            customer_id=config.customer_id,
            login_customer_id=config.login_customer_id,
            logger=logger
        )
        
        success, message = extractor.test_connection()
        
        if success:
            logger.info(f"  [OK] {message}")
        else:
            logger.error(f"  [X] {message}")
            return 2
            
    except Exception as e:
        logger.error(f"  [X] Failed to initialize extractor: {e}")
        return 2
    
    # ========================================
    # Dry Run Check
    # ========================================
    if args.dry_run:
        logger.info("=" * 50)
        logger.info("DRY RUN - Configuration validated, no data extracted")
        logger.info("=" * 50)
        return 0
    
    # ========================================
    # Step 5: Extract All Data
    # ========================================
    logger.info("=" * 50)
    logger.info("Step 5: Extracting Google Ads data")
    logger.info("=" * 50)
    
    try:
        all_data = extractor.extract_all_data(
            start_date=start_date,
            end_date=end_date
        )
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        return 2
    
    if not all_data:
        logger.warning("No data extracted from Google Ads")
        return 0
    
    # ========================================
    # Step 6: Load to DuckDB
    # ========================================
    logger.info("=" * 50)
    logger.info("Step 6: Loading data to DuckDB")
    logger.info("=" * 50)
    
    duckdb_path = str(config.duckdb_path)
    total_rows = 0
    tables_created = []
    
    for dataset_name, data in all_data.items():
        table_name = f"gads_{dataset_name}"
        
        if load_to_duckdb(duckdb_path, data, table_name, logger):
            total_rows += len(data)
            tables_created.append(table_name)
        else:
            logger.warning(f"Failed to load {table_name}")
    
    # ========================================
    # Summary
    # ========================================
    logger.info("\n" + "=" * 60)
    logger.info("GOOGLE ADS ETL PIPELINE COMPLETED SUCCESSFULLY")
    logger.info("=" * 60)
    logger.info(f"Date range: {start_date} to {end_date}")
    logger.info(f"Total rows extracted: {total_rows:,}")
    logger.info(f"Tables created: {len(tables_created)}")
    logger.info(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("")
    logger.info("Created tables:")
    for table in tables_created:
        logger.info(f"  - {table}")
    logger.info("=" * 60)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
