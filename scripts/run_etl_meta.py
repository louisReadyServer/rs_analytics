"""
Meta (Facebook) Ads ETL Pipeline

Extracts lifetime advertising data from Meta Ads API and loads into DuckDB.

Features:
- Multi-account support
- Lifetime data extraction
- Incremental updates
- Comprehensive metrics at all levels (account, campaign, ad set, ad)
- Geographic, device, and demographic breakdowns

Usage:
    # Lifetime extraction (all historical data)
    python scripts/run_etl_meta.py --lifetime
    
    # Last N days
    python scripts/run_etl_meta.py --lookback-days 90
    
    # Custom date range
    python scripts/run_etl_meta.py --start-date 2024-01-01 --end-date 2024-12-31

Tables Created:
    - meta_daily_account: Daily account-level metrics
    - meta_campaigns: Campaign metadata
    - meta_campaign_insights: Daily campaign metrics
    - meta_adsets: Ad set metadata
    - meta_adset_insights: Daily ad set metrics
    - meta_ads: Ad metadata
    - meta_ad_insights: Daily ad metrics
    - meta_geographic: Geographic breakdown
    - meta_devices: Device/platform breakdown
    - meta_demographics: Age/gender breakdown
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

# Fix Windows console encoding
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Bootstrap: add project root to sys.path so etl.utils can be imported
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from etl.utils import get_project_root
project_root = get_project_root()

import duckdb
import pandas as pd

from etl.meta_config import get_meta_config, MetaConfigurationError
from etl.meta_extractor import MetaExtractor
from scripts.utils.db import upsert_to_duckdb

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(project_root / 'logs' / 'meta_etl.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


# Table definitions for DuckDB
TABLE_SCHEMAS = {
    'meta_daily_account': """
        CREATE TABLE IF NOT EXISTS meta_daily_account (
            date DATE,
            ad_account_id VARCHAR,
            impressions BIGINT,
            reach BIGINT,
            clicks BIGINT,
            unique_clicks BIGINT,
            spend DOUBLE,
            ctr DOUBLE,
            cpc DOUBLE,
            cpm DOUBLE,
            frequency DOUBLE,
            cost_per_unique_click DOUBLE,
            link_clicks BIGINT,
            page_engagement BIGINT,
            post_engagement BIGINT,
            app_installs BIGINT,
            purchases BIGINT,
            leads BIGINT,
            purchase_value DOUBLE,
            video_p25 BIGINT,
            video_p50 BIGINT,
            video_p75 BIGINT,
            video_p100 BIGINT,
            extracted_at TIMESTAMP,
            PRIMARY KEY (date, ad_account_id)
        )
    """,
    'meta_campaigns': """
        CREATE TABLE IF NOT EXISTS meta_campaigns (
            campaign_id VARCHAR PRIMARY KEY,
            ad_account_id VARCHAR,
            campaign_name VARCHAR,
            status VARCHAR,
            effective_status VARCHAR,
            objective VARCHAR,
            buying_type VARCHAR,
            daily_budget DOUBLE,
            lifetime_budget DOUBLE,
            budget_remaining DOUBLE,
            created_time TIMESTAMP,
            start_time TIMESTAMP,
            stop_time TIMESTAMP,
            extracted_at TIMESTAMP
        )
    """,
    'meta_campaign_insights': """
        CREATE TABLE IF NOT EXISTS meta_campaign_insights (
            date DATE,
            ad_account_id VARCHAR,
            campaign_id VARCHAR,
            campaign_name VARCHAR,
            impressions BIGINT,
            reach BIGINT,
            clicks BIGINT,
            unique_clicks BIGINT,
            spend DOUBLE,
            ctr DOUBLE,
            cpc DOUBLE,
            cpm DOUBLE,
            frequency DOUBLE,
            link_clicks BIGINT,
            app_installs BIGINT,
            purchases BIGINT,
            leads BIGINT,
            purchase_value DOUBLE,
            extracted_at TIMESTAMP,
            PRIMARY KEY (date, campaign_id)
        )
    """,
    'meta_adsets': """
        CREATE TABLE IF NOT EXISTS meta_adsets (
            adset_id VARCHAR PRIMARY KEY,
            ad_account_id VARCHAR,
            campaign_id VARCHAR,
            adset_name VARCHAR,
            status VARCHAR,
            effective_status VARCHAR,
            optimization_goal VARCHAR,
            billing_event VARCHAR,
            bid_strategy VARCHAR,
            daily_budget DOUBLE,
            lifetime_budget DOUBLE,
            budget_remaining DOUBLE,
            target_countries VARCHAR,
            created_time TIMESTAMP,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            extracted_at TIMESTAMP
        )
    """,
    'meta_adset_insights': """
        CREATE TABLE IF NOT EXISTS meta_adset_insights (
            date DATE,
            ad_account_id VARCHAR,
            campaign_id VARCHAR,
            campaign_name VARCHAR,
            adset_id VARCHAR,
            adset_name VARCHAR,
            impressions BIGINT,
            reach BIGINT,
            clicks BIGINT,
            unique_clicks BIGINT,
            spend DOUBLE,
            ctr DOUBLE,
            cpc DOUBLE,
            cpm DOUBLE,
            frequency DOUBLE,
            link_clicks BIGINT,
            app_installs BIGINT,
            purchases BIGINT,
            leads BIGINT,
            purchase_value DOUBLE,
            extracted_at TIMESTAMP,
            PRIMARY KEY (date, adset_id)
        )
    """,
    'meta_ads': """
        CREATE TABLE IF NOT EXISTS meta_ads (
            ad_id VARCHAR PRIMARY KEY,
            ad_account_id VARCHAR,
            campaign_id VARCHAR,
            adset_id VARCHAR,
            ad_name VARCHAR,
            status VARCHAR,
            effective_status VARCHAR,
            creative_id VARCHAR,
            created_time TIMESTAMP,
            extracted_at TIMESTAMP
        )
    """,
    'meta_ad_insights': """
        CREATE TABLE IF NOT EXISTS meta_ad_insights (
            date DATE,
            ad_account_id VARCHAR,
            campaign_id VARCHAR,
            campaign_name VARCHAR,
            adset_id VARCHAR,
            adset_name VARCHAR,
            ad_id VARCHAR,
            ad_name VARCHAR,
            impressions BIGINT,
            reach BIGINT,
            clicks BIGINT,
            spend DOUBLE,
            ctr DOUBLE,
            cpc DOUBLE,
            cpm DOUBLE,
            link_clicks BIGINT,
            app_installs BIGINT,
            purchases BIGINT,
            purchase_value DOUBLE,
            extracted_at TIMESTAMP,
            PRIMARY KEY (date, ad_id)
        )
    """,
    'meta_geographic': """
        CREATE TABLE IF NOT EXISTS meta_geographic (
            date_start DATE,
            date_stop DATE,
            ad_account_id VARCHAR,
            country VARCHAR,
            impressions BIGINT,
            reach BIGINT,
            clicks BIGINT,
            spend DOUBLE,
            ctr DOUBLE,
            cpc DOUBLE,
            cpm DOUBLE,
            app_installs BIGINT,
            purchases BIGINT,
            purchase_value DOUBLE,
            extracted_at TIMESTAMP,
            PRIMARY KEY (date_start, ad_account_id, country)
        )
    """,
    'meta_devices': """
        CREATE TABLE IF NOT EXISTS meta_devices (
            date_start DATE,
            date_stop DATE,
            ad_account_id VARCHAR,
            device_platform VARCHAR,
            publisher_platform VARCHAR,
            impressions BIGINT,
            reach BIGINT,
            clicks BIGINT,
            spend DOUBLE,
            ctr DOUBLE,
            cpc DOUBLE,
            cpm DOUBLE,
            app_installs BIGINT,
            purchases BIGINT,
            extracted_at TIMESTAMP,
            PRIMARY KEY (date_start, ad_account_id, device_platform, publisher_platform)
        )
    """,
    'meta_demographics': """
        CREATE TABLE IF NOT EXISTS meta_demographics (
            date_start DATE,
            date_stop DATE,
            ad_account_id VARCHAR,
            age VARCHAR,
            gender VARCHAR,
            impressions BIGINT,
            reach BIGINT,
            clicks BIGINT,
            spend DOUBLE,
            ctr DOUBLE,
            cpc DOUBLE,
            cpm DOUBLE,
            app_installs BIGINT,
            purchases BIGINT,
            extracted_at TIMESTAMP,
            PRIMARY KEY (date_start, ad_account_id, age, gender)
        )
    """
}


def create_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create all required tables if they don't exist."""
    logger.info("Creating/verifying database tables...")
    
    for table_name, schema in TABLE_SCHEMAS.items():
        try:
            conn.execute(schema)
            logger.info(f"  ✅ Table {table_name} ready")
        except Exception as e:
            logger.error(f"  ❌ Error creating table {table_name}: {e}")
            raise



def run_etl(config, 
            days: int = None,
            start_date: str = None,
            end_date: str = None,
            lifetime: bool = False) -> dict:
    """
    Run the complete Meta Ads ETL pipeline.
    
    Args:
        config: MetaConfig object
        days: Number of days to look back
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        lifetime: If True, extract all historical data
        
    Returns:
        Dictionary with extraction statistics
    """
    stats = {
        'accounts_processed': 0,
        'tables_updated': 0,
        'total_rows': 0,
        'errors': [],
        'start_time': datetime.now(),
        'end_time': None
    }
    
    duckdb_path = str(config.duckdb_path)

    # Connect to DuckDB for table creation
    logger.info(f"Connecting to DuckDB: {config.duckdb_path}")
    conn = duckdb.connect(duckdb_path)
    create_tables(conn)
    conn.close()

    # Process each ad account
    for account_id in config.ad_account_ids:
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing account: {account_id}")
        logger.info(f"{'='*60}")
        
        try:
            # Initialize extractor
            extractor = MetaExtractor(config.access_token, account_id)
            
            # Test connection
            success, msg = extractor.test_connection()
            if not success:
                logger.error(f"Connection failed: {msg}")
                stats['errors'].append(f"{account_id}: {msg}")
                continue
            
            logger.info(f"Connected: {msg}")
            
            # Extract all data
            data = extractor.extract_all(
                days=days,
                start_date=start_date,
                end_date=end_date,
                lifetime=lifetime
            )
            
            # Mapping from extraction keys to table names and their primary keys
            table_mapping = {
                'daily_account': ('meta_daily_account', ['date', 'ad_account_id']),
                'campaigns': ('meta_campaigns', ['campaign_id']),
                'campaign_insights': ('meta_campaign_insights', ['date', 'campaign_id']),
                'adsets': ('meta_adsets', ['adset_id']),
                'adset_insights': ('meta_adset_insights', ['date', 'adset_id']),
                'ads': ('meta_ads', ['ad_id']),
                'ad_insights': ('meta_ad_insights', ['date', 'ad_id']),
                'geographic': ('meta_geographic', ['date_start', 'ad_account_id', 'country']),
                'devices': ('meta_devices', ['date_start', 'ad_account_id', 'device_platform', 'publisher_platform']),
                'demographics': ('meta_demographics', ['date_start', 'ad_account_id', 'age', 'gender']),
            }
            
            # Upsert each dataset
            for data_key, (table_name, pk_columns) in table_mapping.items():
                df = data.get(data_key, pd.DataFrame())
                if not df.empty:
                    if upsert_to_duckdb(duckdb_path, df, table_name, pk_columns, logger):
                        stats['total_rows'] += len(df)
                        stats['tables_updated'] += 1
            
            stats['accounts_processed'] += 1
            logger.info(f"✅ Account {account_id} processed successfully")
            
        except Exception as e:
            logger.error(f"❌ Error processing account {account_id}: {e}")
            stats['errors'].append(f"{account_id}: {str(e)}")
    
    stats['end_time'] = datetime.now()
    stats['duration_seconds'] = (stats['end_time'] - stats['start_time']).total_seconds()
    
    return stats


def print_summary(stats: dict) -> None:
    """Print ETL run summary."""
    print("\n" + "=" * 60)
    print("📊 META ADS ETL SUMMARY")
    print("=" * 60)
    print(f"Start Time:        {stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"End Time:          {stats['end_time'].strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Duration:          {stats['duration_seconds']:.1f} seconds")
    print("-" * 60)
    print(f"Accounts Processed: {stats['accounts_processed']}")
    print(f"Tables Updated:     {stats['tables_updated']}")
    print(f"Total Rows:         {stats['total_rows']:,}")
    
    if stats['errors']:
        print("-" * 60)
        print(f"⚠️  Errors ({len(stats['errors'])}):")
        for error in stats['errors']:
            print(f"   • {error}")
    
    print("=" * 60)
    
    if stats['accounts_processed'] > 0:
        print("✅ ETL completed successfully!")
    else:
        print("❌ ETL failed - no accounts processed")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Meta Ads ETL Pipeline - Extract advertising data to DuckDB'
    )
    
    parser.add_argument(
        '--lifetime',
        action='store_true',
        help='Extract all available historical data'
    )
    
    parser.add_argument(
        '--lookback-days',
        type=int,
        default=None,
        help='Number of days to look back (default: 30)'
    )
    
    parser.add_argument(
        '--start-date',
        type=str,
        default=None,
        help='Start date (YYYY-MM-DD)'
    )
    
    parser.add_argument(
        '--end-date',
        type=str,
        default=None,
        help='End date (YYYY-MM-DD)'
    )
    
    args = parser.parse_args()
    
    print("\n" + "█" * 60)
    print(" 🔵 META (FACEBOOK) ADS ETL PIPELINE")
    print("█" * 60)
    print(f" Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if args.lifetime:
        print(" Mode: LIFETIME (all historical data)")
    elif args.start_date and args.end_date:
        print(f" Mode: DATE RANGE ({args.start_date} to {args.end_date})")
    elif args.lookback_days:
        print(f" Mode: LOOKBACK ({args.lookback_days} days)")
    else:
        print(" Mode: DEFAULT (last 30 days)")
        args.lookback_days = 30
    
    print("█" * 60 + "\n")
    
    try:
        # Load configuration
        logger.info("Loading Meta Ads configuration...")
        config = get_meta_config()
        logger.info(f"  Ad accounts: {', '.join(config.ad_account_ids)}")
        logger.info(f"  DuckDB path: {config.duckdb_path}")
        
        # Run ETL
        stats = run_etl(
            config=config,
            days=args.lookback_days,
            start_date=args.start_date,
            end_date=args.end_date,
            lifetime=args.lifetime
        )
        
        # Print summary
        print_summary(stats)
        
        return 0 if stats['accounts_processed'] > 0 else 1
        
    except MetaConfigurationError as e:
        logger.error(f"Configuration error: {e}")
        print(f"\n❌ Configuration Error: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        print(f"\n❌ Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
