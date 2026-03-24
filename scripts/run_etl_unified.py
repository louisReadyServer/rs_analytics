#!/usr/bin/env python3
"""
Unified ETL Pipeline for rs_analytics

This script provides a single entry point for all ETL operations:
- Google Analytics 4 (GA4)
- Google Search Console (GSC)
- Google Ads
- Meta (Facebook) Ads
- Twitter/X

It consolidates the functionality of individual ETL scripts into a unified
interface while maintaining full compatibility with existing workflows.

Usage:
    # Run specific source
    python scripts/run_etl_unified.py --source ga4 --lookback-days 30
    python scripts/run_etl_unified.py --source gads --lifetime
    python scripts/run_etl_unified.py --source gsc --start-date 2024-01-01
    python scripts/run_etl_unified.py --source meta --lookback-days 90
    python scripts/run_etl_unified.py --source twitter --lookback-days 7
    
    # Run all sources
    python scripts/run_etl_unified.py --source all --lookback-days 30
    
    # Dry run (validate only)
    python scripts/run_etl_unified.py --source gads --dry-run

Exit Codes:
    0 - Success
    1 - Configuration error
    2 - Extraction error
    3 - Loading error
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Bootstrap: add project root to sys.path so etl.utils can be imported
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from etl.utils import get_project_root
project_root = get_project_root()

# Import shared utilities
from scripts.utils.cli import (
    create_etl_parser,
    get_date_range_from_args,
    setup_script_logging,
    print_banner,
    print_step,
    print_completion,
)
from scripts.utils.db import load_to_duckdb, load_dataframe_to_duckdb, upsert_to_duckdb, get_table_keys


# ============================================
# Available Data Sources
# ============================================

AVAILABLE_SOURCES = {
    'ga4': {
        'name': 'Google Analytics 4',
        'config_module': 'etl.config',
        'config_function': 'get_config',
        'lifetime_start': '2020-01-01',
    },
    'gsc': {
        'name': 'Google Search Console',
        'config_module': 'etl.gsc_config',
        'config_function': 'get_gsc_config',
        'lifetime_start': None,  # Dynamic based on data availability
    },
    'gads': {
        'name': 'Google Ads',
        'config_module': 'etl.gads_config',
        'config_function': 'get_gads_config',
        'lifetime_start': '2020-01-01',
    },
    'meta': {
        'name': 'Meta (Facebook) Ads',
        'config_module': 'etl.meta_config',
        'config_function': 'get_meta_config',
        'lifetime_start': None,  # Uses API limit (~13 months)
    },
    'twitter': {
        'name': 'Twitter/X',
        'config_module': 'etl.twitter_config',
        'config_function': 'get_twitter_config',
        'lifetime_start': None,  # Limited by API
    },
}


def parse_args() -> argparse.Namespace:
    """Parse command line arguments with unified ETL options."""
    parser = create_etl_parser(
        description="Unified ETL Pipeline for rs_analytics",
        default_lookback_days=30
    )
    
    # Add source selection
    parser.add_argument(
        "--source", "-s",
        type=str,
        required=True,
        choices=list(AVAILABLE_SOURCES.keys()) + ['all'],
        help="Data source to extract (ga4, gsc, gads, meta, twitter, or 'all')"
    )
    
    # Add comprehensive mode for GA4
    parser.add_argument(
        "--comprehensive",
        action="store_true",
        help="For GA4: Use comprehensive extraction (more dimensions/metrics)"
    )
    
    return parser.parse_args()


# ============================================
# GA4 ETL
# ============================================

def run_ga4_etl(
    start_date: str,
    end_date: str,
    comprehensive: bool,
    duckdb_path: str,
    logger: logging.Logger,
    is_lifetime: bool = False
) -> Tuple[bool, int, List[str]]:
    """
    Run Google Analytics 4 ETL.
    
    Returns:
        Tuple of (success, total_rows, tables_created)
    """
    logger.info("=" * 50)
    logger.info("Running Google Analytics 4 ETL")
    logger.info("=" * 50)
    
    try:
        from etl.config import get_config
        from google.analytics.data_v1beta import BetaAnalyticsDataClient
        from google.analytics.data_v1beta.types import (
            DateRange, Dimension, Metric, RunReportRequest
        )
        
        config = get_config()
        client = BetaAnalyticsDataClient()
        
        # Define extraction configurations based on mode
        if comprehensive:
            # Comprehensive extraction with multiple dimension sets
            extractions = [
                {
                    'name': 'traffic_overview',
                    'dimensions': ['date', 'sessionSource', 'sessionMedium', 'sessionCampaignName'],
                    'metrics': ['sessions', 'totalUsers', 'newUsers', 'bounceRate', 'screenPageViews']
                },
                {
                    'name': 'page_performance',
                    'dimensions': ['date', 'pagePath', 'pageTitle'],
                    'metrics': ['screenPageViews', 'sessions', 'bounceRate', 'averageSessionDuration']
                },
                {
                    'name': 'geographic_data',
                    'dimensions': ['date', 'country', 'region', 'city'],
                    'metrics': ['sessions', 'totalUsers', 'newUsers']
                },
                {
                    'name': 'technology_data',
                    'dimensions': ['date', 'deviceCategory', 'operatingSystem', 'browser'],
                    'metrics': ['sessions', 'totalUsers', 'screenPageViews']
                },
                {
                    'name': 'event_data',
                    'dimensions': ['date', 'eventName'],
                    'metrics': ['eventCount', 'totalUsers']
                },
            ]
        else:
            # Basic extraction
            extractions = [
                {
                    'name': 'sessions',
                    'dimensions': ['date'],
                    'metrics': ['sessions', 'totalUsers', 'newUsers', 'bounceRate']
                },
            ]
        
        total_rows = 0
        tables_created = []
        
        for extraction in extractions:
            table_name = f"ga4_{extraction['name']}"
            logger.info(f"Extracting: {table_name}")
            
            request = RunReportRequest(
                property=f"properties/{config.ga4_property_id}",
                dimensions=[Dimension(name=d) for d in extraction['dimensions']],
                metrics=[Metric(name=m) for m in extraction['metrics']],
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            )
            
            response = client.run_report(request)
            
            # Convert to list of dicts
            data = []
            for row in response.rows:
                record = {}
                for i, dim in enumerate(extraction['dimensions']):
                    record[dim] = row.dimension_values[i].value
                for i, met in enumerate(extraction['metrics']):
                    record[met] = row.metric_values[i].value
                data.append(record)
            
            # Use replace for lifetime, upsert for incremental
            if is_lifetime:
                success = load_to_duckdb(duckdb_path, data, table_name, logger, replace=True)
            else:
                success = upsert_to_duckdb(duckdb_path, data, table_name, logger=logger)
            
            if success:
                total_rows += len(data)
                tables_created.append(table_name)
        
        return True, total_rows, tables_created
        
    except Exception as e:
        logger.error(f"GA4 ETL failed: {e}")
        return False, 0, []


# ============================================
# GSC ETL
# ============================================

def run_gsc_etl(
    start_date: str,
    end_date: str,
    duckdb_path: str,
    logger: logging.Logger,
    is_lifetime: bool = False
) -> Tuple[bool, int, List[str]]:
    """
    Run Google Search Console ETL.
    
    Returns:
        Tuple of (success, total_rows, tables_created)
    """
    logger.info("=" * 50)
    logger.info("Running Google Search Console ETL")
    logger.info("=" * 50)
    
    try:
        from etl.gsc_config import get_gsc_config
        from etl.gsc_extractor import GSCExtractor
        
        config = get_gsc_config()
        
        extractor = GSCExtractor(
            credentials_path=str(config.credentials_path),
            site_url=config.site_url,
            logger=logger
        )
        
        # Test connection
        success, message = extractor.test_connection()
        if not success:
            logger.error(f"GSC connection failed: {message}")
            return False, 0, []
        
        logger.info(f"GSC connection: {message}")
        
        # Extract all data
        all_data = extractor.extract_all_data(start_date, end_date)
        
        if not all_data:
            logger.warning("No data extracted from GSC")
            return True, 0, []
        
        # Load to DuckDB
        total_rows = 0
        tables_created = []
        
        for dataset_name, data in all_data.items():
            table_name = f"gsc_{dataset_name}"
            # Use replace for lifetime, upsert for incremental
            if is_lifetime:
                success = load_to_duckdb(duckdb_path, data, table_name, logger, replace=True)
            else:
                success = upsert_to_duckdb(duckdb_path, data, table_name, logger=logger)
            
            if success:
                total_rows += len(data)
                tables_created.append(table_name)
        
        return True, total_rows, tables_created
        
    except Exception as e:
        logger.error(f"GSC ETL failed: {e}")
        return False, 0, []


# ============================================
# Google Ads ETL
# ============================================

def run_gads_etl(
    start_date: str,
    end_date: str,
    duckdb_path: str,
    logger: logging.Logger,
    is_lifetime: bool = False
) -> Tuple[bool, int, List[str]]:
    """
    Run Google Ads ETL.
    
    Returns:
        Tuple of (success, total_rows, tables_created)
    """
    logger.info("=" * 50)
    logger.info("Running Google Ads ETL")
    logger.info("=" * 50)
    
    try:
        from etl.gads_config import get_gads_config, get_gads_client
        from etl.gads_extractor import GAdsExtractor
        
        config = get_gads_config()
        client = get_gads_client()
        
        extractor = GAdsExtractor(
            client=client,
            customer_id=config.customer_id,
            login_customer_id=config.login_customer_id,
            logger=logger
        )
        
        # Test connection
        success, message = extractor.test_connection()
        if not success:
            logger.error(f"Google Ads connection failed: {message}")
            return False, 0, []
        
        logger.info(f"Google Ads connection: OK")
        
        # Extract all data
        all_data = extractor.extract_all_data(start_date, end_date)
        
        if not all_data:
            logger.warning("No data extracted from Google Ads")
            return True, 0, []
        
        # Load to DuckDB
        total_rows = 0
        tables_created = []
        
        for dataset_name, data in all_data.items():
            table_name = f"gads_{dataset_name}"
            # Use replace for lifetime, upsert for incremental
            if is_lifetime:
                success = load_to_duckdb(duckdb_path, data, table_name, logger, replace=True)
            else:
                success = upsert_to_duckdb(duckdb_path, data, table_name, logger=logger)
            
            if success:
                total_rows += len(data)
                tables_created.append(table_name)
        
        return True, total_rows, tables_created
        
    except Exception as e:
        logger.error(f"Google Ads ETL failed: {e}")
        return False, 0, []


# ============================================
# Meta Ads ETL
# ============================================

def run_meta_etl(
    start_date: str,
    end_date: str,
    duckdb_path: str,
    logger: logging.Logger,
    lifetime: bool = False,
    is_lifetime: bool = False
) -> Tuple[bool, int, List[str]]:
    """
    Run Meta (Facebook) Ads ETL.
    
    Returns:
        Tuple of (success, total_rows, tables_created)
    """
    logger.info("=" * 50)
    logger.info("Running Meta Ads ETL")
    logger.info("=" * 50)
    
    try:
        from etl.meta_config import get_meta_config
        from etl.meta_extractor import MetaExtractor
        
        config = get_meta_config()
        
        total_rows = 0
        tables_created = []
        
        # Process each account
        for account_id in config.ad_account_ids:
            logger.info(f"Processing Meta account: {account_id}")
            
            extractor = MetaExtractor(
                access_token=config.access_token,
                ad_account_id=account_id
            )
            
            # Test connection
            success, message = extractor.test_connection()
            if not success:
                logger.error(f"Meta connection failed for {account_id}: {message}")
                continue
            
            logger.info(f"Meta connection: {message}")
            
            # Extract all data
            all_data = extractor.extract_all(
                start_date=start_date,
                end_date=end_date,
                lifetime=lifetime
            )
            
            # Load to DuckDB
            for dataset_name, df in all_data.items():
                table_name = f"meta_{dataset_name}"
                # Use replace for lifetime, upsert for incremental
                if is_lifetime:
                    success = load_dataframe_to_duckdb(duckdb_path, df, table_name, logger, replace=True)
                else:
                    success = upsert_to_duckdb(duckdb_path, df, table_name, logger=logger)
                
                if success:
                    total_rows += len(df)
                    if table_name not in tables_created:
                        tables_created.append(table_name)
        
        return True, total_rows, tables_created
        
    except Exception as e:
        logger.error(f"Meta ETL failed: {e}")
        return False, 0, []


# ============================================
# Twitter ETL
# ============================================

def run_twitter_etl(
    start_date: str,
    end_date: str,
    duckdb_path: str,
    logger: logging.Logger,
    is_lifetime: bool = False
) -> Tuple[bool, int, List[str]]:
    """
    Run Twitter/X ETL.
    
    Returns:
        Tuple of (success, total_rows, tables_created)
    """
    logger.info("=" * 50)
    logger.info("Running Twitter/X ETL")
    logger.info("=" * 50)
    
    try:
        from etl.twitter_config import get_twitter_config
        from etl.twitter_extractor import TwitterExtractor
        
        config = get_twitter_config()
        
        extractor = TwitterExtractor(
            bearer_token=config.bearer_token,
            consumer_key=config.consumer_key,
            consumer_secret=config.consumer_secret,
            access_token=config.access_token,
            access_token_secret=config.access_token_secret,
            username=config.username
        )
        
        # Test connection
        success, message = extractor.test_connection()
        if not success:
            logger.error(f"Twitter connection failed: {message}")
            return False, 0, []
        
        logger.info(f"Twitter connection: {message}")
        
        # Extract all data
        all_data = extractor.extract_all(
            start_date=start_date,
            end_date=end_date
        )
        
        if not all_data:
            logger.warning("No data extracted from Twitter")
            return True, 0, []
        
        # Load to DuckDB
        total_rows = 0
        tables_created = []
        
        for dataset_name, df in all_data.items():
            table_name = f"twitter_{dataset_name}"
            # Use replace for lifetime, upsert for incremental
            if is_lifetime:
                success = load_dataframe_to_duckdb(duckdb_path, df, table_name, logger, replace=True)
            else:
                success = upsert_to_duckdb(duckdb_path, df, table_name, logger=logger)
            
            if success:
                total_rows += len(df)
                tables_created.append(table_name)
        
        return True, total_rows, tables_created
        
    except Exception as e:
        logger.error(f"Twitter ETL failed: {e}")
        return False, 0, []


# ============================================
# Main Execution
# ============================================

def main() -> int:
    """Main ETL pipeline execution."""
    args = parse_args()
    
    # Print startup banner
    print_banner("Unified ETL Pipeline")
    
    # Initial logging setup
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # ========================================
    # Step 1: Determine sources to run
    # ========================================
    print_step(1, "Determining data sources")
    
    if args.source == 'all':
        sources = list(AVAILABLE_SOURCES.keys())
        print(f"  [i] Running all sources: {', '.join(sources)}")
    else:
        sources = [args.source]
        print(f"  [i] Running source: {args.source}")
    
    # ========================================
    # Step 2: Calculate date range
    # ========================================
    print_step(2, "Calculating date range")
    
    # Use appropriate lifetime start based on source
    lifetime_start = "2020-01-01"
    if len(sources) == 1 and sources[0] in AVAILABLE_SOURCES:
        source_info = AVAILABLE_SOURCES[sources[0]]
        if source_info.get('lifetime_start'):
            lifetime_start = source_info['lifetime_start']
    
    start_date, end_date = get_date_range_from_args(args, lifetime_start)
    
    print(f"  [i] Date range: {start_date} to {end_date}")
    
    if args.lifetime:
        print(f"  [i] LIFETIME MODE: Extracting all available data")
    
    # ========================================
    # Step 3: Load configurations
    # ========================================
    print_step(3, "Loading configurations")
    
    # Get DuckDB path from first available config
    duckdb_path = None
    log_dir = None
    
    for source in sources:
        try:
            source_info = AVAILABLE_SOURCES[source]
            config_module = __import__(source_info['config_module'], fromlist=[''])
            config_func = getattr(config_module, source_info['config_function'])
            config = config_func()
            
            if hasattr(config, 'duckdb_path') and duckdb_path is None:
                duckdb_path = str(config.duckdb_path)
            if hasattr(config, 'log_dir') and log_dir is None:
                log_dir = config.log_dir
            
            print(f"  [OK] {source_info['name']} configuration loaded")
        except Exception as e:
            print(f"  [X] {AVAILABLE_SOURCES[source]['name']} configuration failed: {e}")
            if len(sources) == 1:
                return 1
    
    if not duckdb_path:
        duckdb_path = str(project_root / "data" / "warehouse.duckdb")
    
    if not log_dir:
        log_dir = project_root / "logs"
    
    print(f"  [i] DuckDB: {duckdb_path}")
    
    # Set up logging
    logger = setup_script_logging("unified_etl", log_dir, args.verbose)
    
    # ========================================
    # Dry Run Check
    # ========================================
    if args.dry_run:
        print("\n" + "=" * 60)
        print("DRY RUN - Configuration validated, no data extracted")
        print("=" * 60)
        return 0
    
    # ========================================
    # Step 4: Run ETL for each source
    # ========================================
    print_step(4, "Running ETL pipelines")
    
    all_tables = []
    total_rows = 0
    failed_sources = []
    
    # Determine if this is a lifetime pull (affects data loading strategy)
    is_lifetime = args.lifetime
    
    for source in sources:
        logger.info(f"\n{'#'*60}")
        logger.info(f"# Processing: {AVAILABLE_SOURCES[source]['name']}")
        logger.info(f"# Mode: {'LIFETIME (full replace)' if is_lifetime else 'INCREMENTAL (upsert)'}")
        logger.info(f"{'#'*60}")
        
        try:
            if source == 'ga4':
                success, rows, tables = run_ga4_etl(
                    start_date, end_date, args.comprehensive,
                    duckdb_path, logger, is_lifetime=is_lifetime
                )
            elif source == 'gsc':
                success, rows, tables = run_gsc_etl(
                    start_date, end_date, duckdb_path, logger,
                    is_lifetime=is_lifetime
                )
            elif source == 'gads':
                success, rows, tables = run_gads_etl(
                    start_date, end_date, duckdb_path, logger,
                    is_lifetime=is_lifetime
                )
            elif source == 'meta':
                success, rows, tables = run_meta_etl(
                    start_date, end_date, duckdb_path, logger,
                    lifetime=args.lifetime, is_lifetime=is_lifetime
                )
            elif source == 'twitter':
                success, rows, tables = run_twitter_etl(
                    start_date, end_date, duckdb_path, logger,
                    is_lifetime=is_lifetime
                )
            else:
                logger.warning(f"Unknown source: {source}")
                continue
            
            if success:
                total_rows += rows
                all_tables.extend(tables)
                logger.info(f"Completed {source}: {rows:,} rows, {len(tables)} tables")
            else:
                failed_sources.append(source)
                
        except Exception as e:
            logger.error(f"Failed to run {source} ETL: {e}")
            failed_sources.append(source)
    
    # ========================================
    # Summary
    # ========================================
    overall_success = len(failed_sources) == 0
    print_completion(overall_success, total_rows, all_tables)
    
    if failed_sources:
        print(f"\nFailed sources: {', '.join(failed_sources)}")
        return 2
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
