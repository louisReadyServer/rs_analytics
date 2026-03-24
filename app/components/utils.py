"""
Shared Utility Functions for Dashboard Components

Centralizes common patterns used across multiple component files:
- DuckDB query execution (replaces 6+ duplicate _query() functions)
- Table existence checks (replaces 4 check_*_data_exists in main.py)
- Table metadata helpers

Usage:
    from app.components.utils import query_duckdb, check_tables_exist
"""

import logging
from typing import Optional, Tuple, List

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)


# ============================================
# DuckDB Query Helpers
# ============================================

def query_duckdb(duckdb_path: str, sql: str) -> Optional[pd.DataFrame]:
    """
    Execute a read-only SQL query against DuckDB and return a DataFrame.

    This is the single source of truth for read queries across all dashboard
    components. Previously duplicated as _query() in 6+ component files.

    Args:
        duckdb_path: Path to the DuckDB database file
        sql: SQL query to execute

    Returns:
        DataFrame with results, or None if the query fails or returns no rows
    """
    try:
        conn = duckdb.connect(duckdb_path, read_only=True)
        df = conn.execute(sql).fetchdf()
        conn.close()
        return df if not df.empty else None
    except Exception as exc:
        logger.warning("DuckDB query failed: %s", exc)
        return None


def load_duckdb_data(duckdb_path: str, query: str) -> Optional[pd.DataFrame]:
    """
    Load data from DuckDB — returns DataFrame even if empty (unlike query_duckdb).

    Mirrors the original load_duckdb_data from main.py. Useful when callers
    need to distinguish between an empty result and a failed query.

    Args:
        duckdb_path: Path to the DuckDB database file
        query: SQL query to execute

    Returns:
        DataFrame (possibly empty) on success, None on error
    """
    try:
        conn = duckdb.connect(duckdb_path, read_only=True)
        df = conn.execute(query).fetchdf()
        conn.close()
        return df
    except Exception:
        return None


def get_table_info(duckdb_path: str) -> dict:
    """
    Get row counts for every table in the database.

    Args:
        duckdb_path: Path to the DuckDB database file

    Returns:
        Dict mapping table_name -> row_count
    """
    try:
        conn = duckdb.connect(duckdb_path, read_only=True)
        tables_df = conn.execute("SHOW TABLES").fetchdf()

        table_info = {}
        for table in tables_df['name'].tolist():
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                table_info[table] = count
            except Exception:
                table_info[table] = 0

        conn.close()
        return table_info
    except Exception:
        return {}


# ============================================
# Table Existence Checks
# ============================================

# Canonical table lists per data source — single source of truth
GSC_TABLES = [
    'gsc_queries', 'gsc_pages', 'gsc_countries', 'gsc_devices',
    'gsc_search_appearance', 'gsc_query_page', 'gsc_daily_totals',
]

GADS_TABLES = [
    'gads_daily_summary', 'gads_campaigns', 'gads_ad_groups',
    'gads_keywords', 'gads_ads', 'gads_devices',
    'gads_geographic', 'gads_hourly', 'gads_conversions',
]

META_TABLES = [
    'meta_daily_account', 'meta_campaigns', 'meta_campaign_insights',
    'meta_adsets', 'meta_adset_insights', 'meta_ads', 'meta_ad_insights',
    'meta_geographic', 'meta_devices', 'meta_demographics',
]

TWITTER_TABLES = [
    'twitter_profile', 'twitter_tweets', 'twitter_daily_metrics',
]


def check_tables_exist(
    duckdb_path: str, table_names: List[str]
) -> Tuple[bool, int, list]:
    """
    Check which of the given tables exist and how many rows they hold.

    Replaces 4 near-identical check_*_data_exists functions from main.py.

    Args:
        duckdb_path: Path to the DuckDB database file
        table_names: List of table names to look for

    Returns:
        (has_data, total_rows, found_tables)
    """
    try:
        conn = duckdb.connect(duckdb_path, read_only=True)
        tables_df = conn.execute("SHOW TABLES").fetchdf()
        existing_tables = tables_df['name'].tolist()

        found_tables = [t for t in table_names if t in existing_tables]

        total_rows = 0
        for table in found_tables:
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                total_rows += count
            except Exception:
                pass

        conn.close()
        return len(found_tables) > 0, total_rows, found_tables
    except Exception:
        return False, 0, []


# Convenience wrappers matching the original function signatures
def check_gsc_data_exists(duckdb_path: str) -> Tuple[bool, int, list]:
    return check_tables_exist(duckdb_path, GSC_TABLES)


def check_gads_data_exists(duckdb_path: str) -> Tuple[bool, int, list]:
    return check_tables_exist(duckdb_path, GADS_TABLES)


def check_meta_data_exists(duckdb_path: str) -> Tuple[bool, int, list]:
    return check_tables_exist(duckdb_path, META_TABLES)


def check_twitter_data_exists(duckdb_path: str) -> Tuple[bool, int, list]:
    return check_tables_exist(duckdb_path, TWITTER_TABLES)
