"""
Database Utilities for rs_analytics ETL Scripts

This module provides DuckDB operations for ETL scripts:
- Loading data (list of dicts or DataFrames) to DuckDB tables
- Upsert operations with configurable key columns
- Table management (create, replace, check existence)
- Row count queries

Usage:
    from scripts.utils.db import load_to_duckdb, load_dataframe_to_duckdb, upsert_to_duckdb
    
    # Load list of dicts (replace mode)
    success = load_to_duckdb(
        duckdb_path="/path/to/db.duckdb",
        data=[{"col1": 1, "col2": "a"}, ...],
        table_name="my_table",
        logger=logger
    )
    
    # Load with upsert (delete matching keys, then insert)
    success = upsert_to_duckdb(
        duckdb_path="/path/to/db.duckdb",
        data=[{"date": "2024-01-01", "campaign_id": 123, "clicks": 100}, ...],
        table_name="gads_campaigns",
        key_columns=["date", "campaign_id"],
        logger=logger
    )
"""

import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import duckdb
import pandas as pd


# ============================================
# Table Key Definitions
# ============================================
# These define the unique key columns for each table type.
# Used for upsert operations to identify which rows to update.

TABLE_KEYS: Dict[str, List[str]] = {
    # Google Ads tables
    'gads_daily_summary': ['date', 'campaign_id'],
    'gads_campaigns': ['date', 'campaign_id'],
    'gads_ad_groups': ['date', 'campaign_id', 'ad_group_id'],
    'gads_keywords': ['date', 'campaign_id', 'ad_group_id', 'keyword_id'],
    'gads_ads': ['date', 'campaign_id', 'ad_group_id', 'ad_id'],
    'gads_devices': ['date', 'campaign_id', 'device'],
    'gads_geographic': ['date', 'campaign_id', 'country_criterion_id'],
    'gads_hourly': ['date', 'campaign_id', 'hour'],
    'gads_conversions': ['date', 'campaign_id', 'conversion_action'],
    
    # Google Search Console tables
    'gsc_queries': ['date', 'query'],
    'gsc_pages': ['date', 'page'],
    'gsc_countries': ['date', 'country'],
    'gsc_devices': ['date', 'device'],
    'gsc_search_appearance': ['date', 'searchAppearance'],
    'gsc_query_page': ['query', 'page'],
    'gsc_query_country': ['query', 'country'],
    'gsc_query_device': ['query', 'device'],
    'gsc_page_country': ['page', 'country'],
    'gsc_page_device': ['page', 'device'],
    'gsc_daily_totals': ['date'],
    
    # Meta Ads tables
    'meta_daily_account': ['date', 'ad_account_id'],
    'meta_campaigns': ['campaign_id'],
    'meta_campaign_insights': ['date', 'ad_account_id', 'campaign_id'],
    'meta_adsets': ['adset_id'],
    'meta_adset_insights': ['date', 'ad_account_id', 'adset_id'],
    'meta_ads': ['ad_id'],
    'meta_ad_insights': ['date', 'ad_account_id', 'ad_id'],
    'meta_geographic': ['ad_account_id', 'country', 'date_start'],
    'meta_devices': ['ad_account_id', 'device_platform', 'publisher_platform', 'date_start'],
    'meta_demographics': ['ad_account_id', 'age', 'gender', 'date_start'],
    
    # Twitter tables
    'twitter_profile': ['user_id', 'snapshot_date'],
    'twitter_tweets': ['tweet_id'],
    'twitter_daily_metrics': ['date', 'username'],
    
    # GA4 tables
    'ga4_sessions': ['date'],
    'ga4_traffic_overview': ['date', 'sessionSource', 'sessionMedium', 'sessionCampaignName'],
    'ga4_page_performance': ['date', 'pagePath'],
    'ga4_geographic_data': ['date', 'country', 'region', 'city'],
    'ga4_technology_data': ['date', 'deviceCategory', 'operatingSystem', 'browser'],
    'ga4_event_data': ['date', 'eventName'],
}


def get_table_keys(table_name: str) -> Optional[List[str]]:
    """
    Get the key columns for a table.
    
    Args:
        table_name: Name of the table
        
    Returns:
        List of key column names, or None if not defined
    """
    return TABLE_KEYS.get(table_name)


def clean_column_name(name: str) -> str:
    """
    Clean a column name for DuckDB compatibility.
    
    Replaces any non-alphanumeric characters (except underscore) with underscore.
    
    Args:
        name: Original column name
        
    Returns:
        Cleaned column name
    """
    return re.sub(r'[^a-zA-Z0-9_]', '_', str(name))


def upsert_to_duckdb(
    duckdb_path: Union[str, Path],
    data: Union[List[Dict[str, Any]], pd.DataFrame],
    table_name: str,
    key_columns: Optional[List[str]] = None,
    logger: Optional[logging.Logger] = None
) -> bool:
    """
    Upsert data into a DuckDB table (delete matching keys, then insert).
    
    This function implements an upsert pattern:
    1. If the table doesn't exist, create it with all the data
    2. If it exists, delete rows matching the key columns, then insert new data
    
    This is ideal for incremental updates where you want to:
    - Replace data for specific dates (daily refresh)
    - Update existing records without losing other data
    
    Args:
        duckdb_path: Path to DuckDB database file
        data: Data to upsert (list of dicts or DataFrame)
        table_name: Name of the target table
        key_columns: Columns that form the unique key for upsert.
                    If None, will look up from TABLE_KEYS.
                    If not found, falls back to full replace.
        logger: Optional logger for status messages
        
    Returns:
        True if successful, False otherwise
        
    Example:
        # Upsert campaign data for a specific date range
        >>> upsert_to_duckdb(
        ...     "warehouse.duckdb",
        ...     [{"date": "2024-01-01", "campaign_id": 123, "clicks": 100}],
        ...     "gads_campaigns",
        ...     key_columns=["date", "campaign_id"]
        ... )
        True
    """
    if logger is None:
        logger = logging.getLogger(__name__)
    
    # Convert list to DataFrame if needed
    if isinstance(data, list):
        if not data:
            logger.warning(f"No data to upsert for {table_name}")
            return True
        df = pd.DataFrame(data)
    else:
        df = data
        if df is None or df.empty:
            logger.warning(f"No data to upsert for {table_name}")
            return True
    
    # Clean column names
    df = df.copy()
    df.columns = [clean_column_name(col) for col in df.columns]
    
    # Get key columns (from param, lookup, or None)
    if key_columns is None:
        key_columns = get_table_keys(table_name)
    
    # Clean key column names too
    if key_columns:
        key_columns = [clean_column_name(col) for col in key_columns]
        # Verify all key columns exist in data
        missing_keys = [k for k in key_columns if k not in df.columns]
        if missing_keys:
            logger.warning(f"Key columns {missing_keys} not found in data for {table_name}, falling back to replace")
            key_columns = None
    
    try:
        logger.info(f"Upserting {len(df):,} rows to {table_name}")
        
        # Ensure parent directory exists
        db_path = Path(duckdb_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        
        conn = duckdb.connect(str(db_path))
        
        # Check if table exists
        table_exists = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [table_name]
        ).fetchone()[0] > 0
        
        if not table_exists:
            # Table doesn't exist - create it with all data
            logger.info(f"Table {table_name} doesn't exist, creating...")
            conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
        elif key_columns:
            # Table exists and we have keys - do upsert (delete + insert)
            # Build WHERE clause for delete based on unique key values in new data
            unique_key_values = df[key_columns].drop_duplicates()
            
            if len(key_columns) == 1:
                # Single key column - use IN clause
                key_col = key_columns[0]
                values = unique_key_values[key_col].tolist()
                
                # Handle different types
                if df[key_col].dtype == 'object':
                    values_str = ", ".join([f"'{v}'" for v in values])
                else:
                    values_str = ", ".join([str(v) for v in values])
                
                delete_sql = f"DELETE FROM {table_name} WHERE {key_col} IN ({values_str})"
                logger.debug(f"Deleting existing rows: {delete_sql[:200]}...")
                conn.execute(delete_sql)
            else:
                # Multiple key columns - delete in batches by building OR conditions
                # For efficiency, we'll delete based on unique combinations
                conditions = []
                for _, row in unique_key_values.iterrows():
                    parts = []
                    for col in key_columns:
                        val = row[col]
                        if pd.isna(val):
                            parts.append(f"{col} IS NULL")
                        elif isinstance(val, str):
                            # Escape single quotes
                            val_escaped = str(val).replace("'", "''")
                            parts.append(f"{col} = '{val_escaped}'")
                        else:
                            parts.append(f"{col} = {val}")
                    conditions.append(f"({' AND '.join(parts)})")
                
                # Delete in batches to avoid very long SQL statements
                batch_size = 100
                for i in range(0, len(conditions), batch_size):
                    batch_conditions = conditions[i:i + batch_size]
                    delete_sql = f"DELETE FROM {table_name} WHERE {' OR '.join(batch_conditions)}"
                    conn.execute(delete_sql)
            
            # Insert new data
            conn.execute(f"INSERT INTO {table_name} SELECT * FROM df")
            logger.info(f"Upserted {len(df):,} rows (deleted matching keys, inserted new)")
        else:
            # No key columns - fall back to replace
            logger.warning(f"No key columns for {table_name}, using full replace")
            conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
        
        conn.close()
        
        logger.info(f"Successfully upserted {len(df):,} rows to {table_name}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to upsert {table_name}: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        return False


def load_to_duckdb(
    duckdb_path: Union[str, Path],
    data: List[Dict[str, Any]],
    table_name: str,
    logger: Optional[logging.Logger] = None,
    replace: bool = True,
    key_columns: Optional[List[str]] = None
) -> bool:
    """
    Load a list of dictionaries into a DuckDB table.
    
    Converts the data to a pandas DataFrame, cleans column names,
    and creates/replaces the table in DuckDB.
    
    Args:
        duckdb_path: Path to DuckDB database file
        data: List of dictionaries (rows) to load
        table_name: Name of the table to create/replace
        logger: Optional logger for status messages
        replace: If True, replace existing table; if False, use upsert with key_columns
        key_columns: Key columns for upsert mode (when replace=False)
        
    Returns:
        True if successful, False otherwise
        
    Example:
        >>> data = [{"date": "2024-01-01", "clicks": 100}, ...]
        >>> load_to_duckdb("warehouse.duckdb", data, "gads_campaigns", logger)
        True
    """
    if logger is None:
        logger = logging.getLogger(__name__)
    
    if not data:
        logger.warning(f"No data to load for {table_name}")
        return True
    
    # If not replacing, use upsert logic
    if not replace:
        return upsert_to_duckdb(duckdb_path, data, table_name, key_columns, logger)
    
    try:
        logger.info(f"Loading {len(data):,} rows to {table_name}")
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Clean column names for DuckDB compatibility
        df.columns = [clean_column_name(col) for col in df.columns]
        
        # Ensure parent directory exists
        db_path = Path(duckdb_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Connect and load
        conn = duckdb.connect(str(db_path))
        
        conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
        
        conn.close()
        
        logger.info(f"Successfully loaded {len(data):,} rows to {table_name}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to load {table_name}: {e}")
        return False


def load_dataframe_to_duckdb(
    duckdb_path: Union[str, Path],
    df: pd.DataFrame,
    table_name: str,
    logger: Optional[logging.Logger] = None,
    replace: bool = True,
    key_columns: Optional[List[str]] = None
) -> bool:
    """
    Load a pandas DataFrame into a DuckDB table.
    
    Cleans column names and creates/replaces the table in DuckDB.
    
    Args:
        duckdb_path: Path to DuckDB database file
        df: DataFrame to load
        table_name: Name of the table to create/replace
        logger: Optional logger for status messages
        replace: If True, replace existing table; if False, use upsert with key_columns
        key_columns: Key columns for upsert mode (when replace=False)
        
    Returns:
        True if successful, False otherwise
        
    Example:
        >>> df = pd.DataFrame({"date": ["2024-01-01"], "clicks": [100]})
        >>> load_dataframe_to_duckdb("warehouse.duckdb", df, "meta_campaigns", logger)
        True
    """
    if logger is None:
        logger = logging.getLogger(__name__)
    
    if df is None or df.empty:
        logger.warning(f"No data to load for {table_name}")
        return True
    
    # If not replacing, use upsert logic
    if not replace:
        return upsert_to_duckdb(duckdb_path, df, table_name, key_columns, logger)
    
    try:
        logger.info(f"Loading {len(df):,} rows to {table_name}")
        
        # Clean column names for DuckDB compatibility
        df = df.copy()
        df.columns = [clean_column_name(col) for col in df.columns]
        
        # Ensure parent directory exists
        db_path = Path(duckdb_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Connect and load
        conn = duckdb.connect(str(db_path))
        
        conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
        
        conn.close()
        
        logger.info(f"Successfully loaded {len(df):,} rows to {table_name}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to load {table_name}: {e}")
        return False


def get_table_row_count(
    duckdb_path: Union[str, Path],
    table_name: str
) -> int:
    """
    Get the row count of a DuckDB table.
    
    Args:
        duckdb_path: Path to DuckDB database file
        table_name: Name of the table
        
    Returns:
        Row count, or 0 if table doesn't exist
    """
    try:
        conn = duckdb.connect(str(duckdb_path), read_only=True)
        result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        conn.close()
        return result[0] if result else 0
    except Exception:
        return 0


