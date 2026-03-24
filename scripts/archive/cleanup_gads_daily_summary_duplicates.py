#!/usr/bin/env python3
"""
Cleanup script for gads_daily_summary duplicates.

This script removes duplicate rows from gads_daily_summary table,
keeping only the most recent record for each (date, campaign_id) combination.

Usage:
    python scripts/cleanup_gads_daily_summary_duplicates.py
"""

import logging
import os
import sys
from pathlib import Path

import duckdb
from dotenv import load_dotenv

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Load environment variables
load_dotenv(project_root / '.env')


def get_duckdb_path() -> str:
    """Get DuckDB path from environment or use default."""
    return os.getenv('DUCKDB_PATH', str(project_root / 'data' / 'warehouse.duckdb'))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def cleanup_duplicates(duckdb_path: str) -> tuple[int, int]:
    """
    Remove duplicate rows from gads_daily_summary.
    
    Keeps the most recent row (by extracted_at) for each (date, campaign_id) combination.
    
    Returns:
        Tuple of (rows_deleted, unique_combinations_remaining)
    """
    conn = duckdb.connect(str(duckdb_path))
    
    try:
        # Check if table exists
        table_exists = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'gads_daily_summary'"
        ).fetchone()[0] > 0
        
        if not table_exists:
            logger.warning("Table gads_daily_summary does not exist")
            return 0, 0
        
        # Count duplicates before cleanup
        duplicate_check = """
            SELECT 
                COUNT(*) as total_rows,
                COUNT(DISTINCT (date, campaign_id)) as unique_combinations,
                COUNT(*) - COUNT(DISTINCT (date, campaign_id)) as duplicate_rows
            FROM gads_daily_summary
        """
        before = conn.execute(duplicate_check).fetchone()
        total_before = before[0]
        unique_before = before[1]
        duplicates_before = before[2]
        
        logger.info(f"Before cleanup:")
        logger.info(f"  Total rows: {total_before:,}")
        logger.info(f"  Unique (date, campaign_id) combinations: {unique_before:,}")
        logger.info(f"  Duplicate rows: {duplicates_before:,}")
        
        if duplicates_before == 0:
            logger.info("No duplicates found. Nothing to clean up.")
            return 0, unique_before
        
        # Check if extracted_at column exists
        columns = conn.execute("DESCRIBE gads_daily_summary").fetchall()
        column_names = [col[0] for col in columns]
        has_extracted_at = 'extracted_at' in column_names
        
        if has_extracted_at:
            # Use extracted_at to determine which row to keep
            logger.info("Using extracted_at to determine which rows to keep...")
            
            # Create a temporary table with deduplicated data
            # Keep the row with the latest extracted_at for each (date, campaign_id)
            cleanup_query = """
                CREATE OR REPLACE TABLE gads_daily_summary_clean AS
                SELECT *
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (
                               PARTITION BY date, campaign_id 
                               ORDER BY extracted_at DESC NULLS LAST
                           ) as rn
                    FROM gads_daily_summary
                )
                WHERE rn = 1
            """
            conn.execute(cleanup_query)
            
            # Count rows in cleaned table
            clean_count = conn.execute("SELECT COUNT(*) FROM gads_daily_summary_clean").fetchone()[0]
            
            # Replace original table
            conn.execute("DROP TABLE gads_daily_summary")
            conn.execute("ALTER TABLE gads_daily_summary_clean RENAME TO gads_daily_summary")
            
            rows_deleted = total_before - clean_count
            
        else:
            # No extracted_at column - use rowid to keep arbitrary row
            logger.warning("No extracted_at column found. Keeping arbitrary row for each (date, campaign_id)...")
            
            cleanup_query = """
                CREATE OR REPLACE TABLE gads_daily_summary_clean AS
                SELECT *
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (
                               PARTITION BY date, campaign_id 
                               ORDER BY rowid DESC
                           ) as rn
                    FROM gads_daily_summary
                )
                WHERE rn = 1
            """
            conn.execute(cleanup_query)
            
            clean_count = conn.execute("SELECT COUNT(*) FROM gads_daily_summary_clean").fetchone()[0]
            
            conn.execute("DROP TABLE gads_daily_summary")
            conn.execute("ALTER TABLE gads_daily_summary_clean RENAME TO gads_daily_summary")
            
            rows_deleted = total_before - clean_count
        
        # Verify cleanup
        after = conn.execute(duplicate_check).fetchone()
        total_after = after[0]
        unique_after = after[1]
        duplicates_after = after[2]
        
        logger.info(f"\nAfter cleanup:")
        logger.info(f"  Total rows: {total_after:,}")
        logger.info(f"  Unique (date, campaign_id) combinations: {unique_after:,}")
        logger.info(f"  Duplicate rows: {duplicates_after:,}")
        logger.info(f"  Rows deleted: {rows_deleted:,}")
        
        if duplicates_after > 0:
            logger.warning(f"Warning: {duplicates_after} duplicates still remain!")
        
        return rows_deleted, unique_after
        
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        raise
    finally:
        conn.close()


def main() -> int:
    """Main entry point."""
    logger.info("=" * 60)
    logger.info("gads_daily_summary Duplicate Cleanup")
    logger.info("=" * 60)
    
    duckdb_path = get_duckdb_path()
    logger.info(f"Database: {duckdb_path}")
    
    try:
        rows_deleted, unique_combinations = cleanup_duplicates(duckdb_path)
        
        logger.info("\n" + "=" * 60)
        logger.info("Cleanup completed successfully!")
        logger.info(f"Rows deleted: {rows_deleted:,}")
        logger.info(f"Unique combinations remaining: {unique_combinations:,}")
        logger.info("=" * 60)
        
        return 0
        
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())
