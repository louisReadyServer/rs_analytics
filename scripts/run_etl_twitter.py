#!/usr/bin/env python3
"""
Twitter/X ETL Pipeline

Extracts organic page analytics from Twitter and loads into DuckDB:
- User profile snapshots (daily follower/following counts)
- Tweet performance metrics (likes, retweets, replies, quotes, impressions)
- Daily aggregated engagement metrics

Usage:
    # Full extraction (default: last 100 tweets)
    python scripts/run_etl_twitter.py
    
    # Custom tweet limit
    python scripts/run_etl_twitter.py --max-tweets 500
    
    # Profile only (no tweets)
    python scripts/run_etl_twitter.py --profile-only
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

# Handle Windows console encoding for emojis
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Bootstrap: add project root to sys.path so etl.utils can be imported
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from etl.utils import get_project_root
project_root = get_project_root()

import duckdb
import pandas as pd

from etl.twitter_config import get_twitter_config, TwitterConfigurationError
from etl.twitter_extractor import TwitterExtractor
from scripts.utils.db import upsert_to_duckdb

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================
# Database Schema Definitions
# ============================================
TABLE_SCHEMAS = {
    'twitter_profile': """
        CREATE TABLE IF NOT EXISTS twitter_profile (
            user_id VARCHAR,
            username VARCHAR,
            name VARCHAR,
            description VARCHAR,
            location VARCHAR,
            verified BOOLEAN,
            verified_type VARCHAR,
            created_at VARCHAR,
            followers_count INTEGER,
            following_count INTEGER,
            tweet_count INTEGER,
            listed_count INTEGER,
            profile_image_url VARCHAR,
            snapshot_date DATE,
            extracted_at TIMESTAMP,
            PRIMARY KEY (user_id, snapshot_date)
        )
    """,
    
    'twitter_tweets': """
        CREATE TABLE IF NOT EXISTS twitter_tweets (
            tweet_id VARCHAR PRIMARY KEY,
            user_id VARCHAR,
            username VARCHAR,
            text VARCHAR,
            tweet_type VARCHAR,
            created_at VARCHAR,
            created_date DATE,
            impressions INTEGER,
            likes INTEGER,
            retweets INTEGER,
            replies INTEGER,
            quotes INTEGER,
            bookmarks INTEGER,
            language VARCHAR,
            source VARCHAR,
            conversation_id VARCHAR,
            in_reply_to_user_id VARCHAR,
            extracted_at TIMESTAMP
        )
    """,
    
    'twitter_daily_metrics': """
        CREATE TABLE IF NOT EXISTS twitter_daily_metrics (
            date DATE,
            username VARCHAR,
            tweet_count INTEGER,
            impressions INTEGER,
            likes INTEGER,
            retweets INTEGER,
            replies INTEGER,
            quotes INTEGER,
            bookmarks INTEGER,
            total_engagements INTEGER,
            engagement_rate DOUBLE,
            extracted_at TIMESTAMP,
            PRIMARY KEY (date, username)
        )
    """
}


def create_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create all required Twitter tables in DuckDB."""
    for table_name, schema in TABLE_SCHEMAS.items():
        try:
            conn.execute(schema)
            logger.info(f"  ✅ Table {table_name} ready")
        except Exception as e:
            logger.error(f"  ❌ Error creating {table_name}: {e}")
            raise



def run_etl(config, max_tweets: int = 100, profile_only: bool = False) -> dict:
    """
    Run the Twitter ETL pipeline.
    
    Args:
        config: TwitterConfig object
        max_tweets: Maximum tweets to fetch
        profile_only: If True, only fetch profile (no tweets)
        
    Returns:
        Dictionary with ETL statistics
    """
    stats = {
        'start_time': datetime.now(),
        'profile_rows': 0,
        'tweet_rows': 0,
        'daily_rows': 0,
        'errors': []
    }
    
    duckdb_path = str(config.duckdb_path)

    # Connect to DuckDB for table creation
    logger.info(f"Connecting to DuckDB: {config.duckdb_path}")
    conn = duckdb.connect(duckdb_path)
    
    # Create tables
    logger.info("Creating/verifying database tables...")
    create_tables(conn)
    conn.close()
    
    # Initialize extractor
    logger.info(f"\nExtracting data for @{config.username}...")
    extractor = TwitterExtractor(config)
    
    # Test connection
    success, message = extractor.test_connection()
    if not success:
        stats['errors'].append(f"Connection failed: {message}")
        logger.error(f"  ❌ {message}")
        return stats
    
    logger.info(f"  ✅ {message}")
    
    # Extract profile
    logger.info("\nExtracting profile...")
    try:
        profile_df = extractor.extract_user_profile()
        if not profile_df.empty:
            if upsert_to_duckdb(duckdb_path, profile_df, 'twitter_profile', ['user_id', 'snapshot_date'], logger):
                stats['profile_rows'] = len(profile_df)
            logger.info(f"  ✅ Upserted {stats['profile_rows']} profile row(s)")
    except Exception as e:
        stats['errors'].append(f"Profile extraction failed: {e}")
        logger.error(f"  ❌ Profile extraction failed: {e}")
    
    # Extract tweets (unless profile-only)
    if not profile_only:
        logger.info(f"\nExtracting tweets (max: {max_tweets})...")
        try:
            tweets_df = extractor.extract_recent_tweets(max_results=max_tweets)
            if not tweets_df.empty:
                if upsert_to_duckdb(duckdb_path, tweets_df, 'twitter_tweets', ['tweet_id'], logger):
                    stats['tweet_rows'] = len(tweets_df)
                logger.info(f"  ✅ Upserted {stats['tweet_rows']} tweet(s)")
                
                # Calculate daily metrics
                logger.info("\nCalculating daily metrics...")
                daily_df = extractor.extract_daily_metrics(tweets_df)
                if not daily_df.empty:
                    if upsert_to_duckdb(duckdb_path, daily_df, 'twitter_daily_metrics', ['date', 'username'], logger):
                        stats['daily_rows'] = len(daily_df)
                    logger.info(f"  ✅ Upserted {stats['daily_rows']} daily metric row(s)")
            else:
                logger.info("  ⚪ No tweets found")
                
        except Exception as e:
            stats['errors'].append(f"Tweet extraction failed: {e}")
            logger.error(f"  ❌ Tweet extraction failed: {e}")
    
    stats['end_time'] = datetime.now()
    
    return stats


def print_summary(stats: dict) -> None:
    """Print ETL run summary."""
    duration = (stats['end_time'] - stats['start_time']).total_seconds()
    
    print("\n" + "="*60)
    print(" 🐦 TWITTER/X ETL SUMMARY")
    print("="*60)
    print(f" Start Time:     {stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
    print(f" End Time:       {stats['end_time'].strftime('%Y-%m-%d %H:%M:%S')}")
    print(f" Duration:       {duration:.1f} seconds")
    print("-"*60)
    print(f" Profile Rows:   {stats['profile_rows']}")
    print(f" Tweet Rows:     {stats['tweet_rows']}")
    print(f" Daily Metrics:  {stats['daily_rows']}")
    print(f" Total Rows:     {stats['profile_rows'] + stats['tweet_rows'] + stats['daily_rows']}")
    print("-"*60)
    
    if stats['errors']:
        print(" ⚠️  Errors:")
        for error in stats['errors']:
            print(f"    - {error}")
    else:
        print(" ✅ ETL completed successfully!")
    
    print("="*60)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Twitter/X ETL Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        '--max-tweets', '-n',
        type=int,
        default=100,
        help='Maximum number of tweets to fetch (default: 100)'
    )
    parser.add_argument(
        '--profile-only',
        action='store_true',
        help='Only fetch profile data, skip tweets'
    )
    
    args = parser.parse_args()
    
    # Print banner
    print("\n" + "█"*60)
    print(" 🐦 TWITTER/X ETL PIPELINE")
    print("█"*60)
    print(f" Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f" Mode: {'Profile Only' if args.profile_only else f'Full (max {args.max_tweets} tweets)'}")
    print("█"*60 + "\n")
    
    # Load configuration
    try:
        logger.info("Loading Twitter configuration...")
        config = get_twitter_config()
        logger.info(f"  Username: @{config.username}")
        logger.info(f"  DuckDB path: {config.duckdb_path}")
    except TwitterConfigurationError as e:
        logger.error(f"Configuration error: {e}")
        print(f"\n❌ {e}")
        print("\nMake sure to set these in your .env file:")
        print("  ENABLE_TWITTER=1")
        print("  TWITTER_BEARER_TOKEN=...")
        print("  TWITTER_CONSUMER_KEY=...")
        print("  TWITTER_CONSUMER_SECRET=...")
        print("  TWITTER_ACCESS_TOKEN=...")
        print("  TWITTER_ACCESS_TOKEN_SECRET=...")
        print("  TWITTER_USERNAME=ReadyServer")
        sys.exit(1)
    
    # Run ETL
    stats = run_etl(config, max_tweets=args.max_tweets, profile_only=args.profile_only)
    
    # Print summary
    print_summary(stats)
    
    # Exit with error code if there were errors
    if stats['errors']:
        sys.exit(1)


if __name__ == "__main__":
    main()
