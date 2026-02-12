"""
DuckDB Client with query timing and logging.

Replaces the 7+ duplicated _query() / load_data() functions scattered
across dashboard components. Every component should use this client
instead of raw duckdb.connect().

Features:
- Automatic query timing
- Slow query logging (configurable threshold)
- Read-only connections by default (safe for dashboards)
- Stores last N queries for debugging / "Show SQL" expander

Usage:
    from rs_analytics.db import DuckDBClient

    client = DuckDBClient("data/warehouse.duckdb")
    df = client.query("SELECT * FROM gads_campaigns LIMIT 10")
    df, elapsed_ms = client.query_with_timing("SELECT ...")
"""

import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)


# ============================================
# Query Log Entry
# ============================================

@dataclass
class QueryLogEntry:
    """Record of a single query execution for debugging."""
    sql: str
    elapsed_ms: float
    row_count: int
    error: Optional[str] = None
    timestamp: float = field(default_factory=time.time)


# ============================================
# DuckDB Client
# ============================================

class DuckDBClient:
    """
    Thin wrapper around DuckDB that adds timing, logging, and a query history.

    Args:
        db_path: Path to the DuckDB database file
        read_only: Open in read-only mode (default True, safe for dashboards)
        slow_query_threshold_ms: Log a warning for queries slower than this (default 500ms)
        max_query_log: How many recent queries to keep in memory (default 50)
    """

    def __init__(
        self,
        db_path: str,
        read_only: bool = True,
        slow_query_threshold_ms: float = 500.0,
        max_query_log: int = 50,
    ):
        self.db_path = str(db_path)
        self.read_only = read_only
        self.slow_query_threshold_ms = slow_query_threshold_ms
        self.max_query_log = max_query_log

        # Recent query log (ring buffer style)
        self.query_log: List[QueryLogEntry] = []

    # ------------------------------------------
    # Public API
    # ------------------------------------------

    def query(self, sql: str) -> Optional[pd.DataFrame]:
        """
        Execute a SQL query and return the result as a DataFrame.

        Returns None if the query fails (error is logged, not raised).
        This keeps dashboard pages from crashing on bad queries.

        Args:
            sql: SQL query string to execute

        Returns:
            DataFrame with results, or None on error
        """
        df, _ = self.query_with_timing(sql)
        return df

    def query_with_timing(self, sql: str) -> Tuple[Optional[pd.DataFrame], float]:
        """
        Execute a SQL query and return (DataFrame, elapsed_ms).

        Same as query() but also returns how long the query took.
        Useful for the "Show SQL" expander and slow-query surfacing.

        Args:
            sql: SQL query string to execute

        Returns:
            (DataFrame or None, elapsed_milliseconds)
        """
        start = time.perf_counter()
        conn = None
        try:
            conn = duckdb.connect(self.db_path, read_only=self.read_only)
            df = conn.execute(sql).fetchdf()
            elapsed_ms = (time.perf_counter() - start) * 1000

            # Log the query
            entry = QueryLogEntry(
                sql=sql,
                elapsed_ms=elapsed_ms,
                row_count=len(df),
            )
            self._append_log(entry)

            # Warn on slow queries
            if elapsed_ms > self.slow_query_threshold_ms:
                logger.warning(
                    "Slow query (%.0fms, %d rows): %s",
                    elapsed_ms, len(df), sql[:200],
                )

            return df, elapsed_ms

        except Exception as exc:
            elapsed_ms = (time.perf_counter() - start) * 1000
            logger.error("Query failed (%.0fms): %s — %s", elapsed_ms, sql[:200], exc)

            entry = QueryLogEntry(
                sql=sql,
                elapsed_ms=elapsed_ms,
                row_count=0,
                error=str(exc),
            )
            self._append_log(entry)

            return None, elapsed_ms

        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    def execute(self, sql: str) -> bool:
        """
        Execute a DDL/DML statement (CREATE VIEW, INSERT, etc.).

        Opens a read-write connection regardless of self.read_only.
        Returns True on success, False on error.

        Args:
            sql: SQL statement to execute

        Returns:
            True if successful, False on error
        """
        conn = None
        try:
            # DDL needs a read-write connection
            conn = duckdb.connect(self.db_path, read_only=False)
            conn.execute(sql)
            return True
        except Exception as exc:
            logger.error("Execute failed: %s — %s", sql[:200], exc)
            return False
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    def table_exists(self, table_name: str) -> bool:
        """
        Check whether a table or view exists in the database.

        Handles schema-qualified names like 'core.dim_user'.

        Args:
            table_name: Table or view name, optionally schema-qualified

        Returns:
            True if the table/view exists
        """
        # Handle schema-qualified names (e.g., "core.dim_user")
        if "." in table_name:
            schema, name = table_name.split(".", 1)
            sql = f"""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema = '{schema}' AND table_name = '{name}'
            """
        else:
            sql = f"""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_name = '{table_name}'
            """
        df = self.query(sql)
        if df is not None and not df.empty:
            return int(df.iloc[0, 0]) > 0
        return False

    def row_count(self, table_name: str) -> int:
        """
        Get the row count of a table.

        Args:
            table_name: Table name (can be schema-qualified)

        Returns:
            Row count, or 0 if table doesn't exist
        """
        df = self.query(f"SELECT COUNT(*) as cnt FROM {table_name}")
        if df is not None and not df.empty:
            return int(df.iloc[0, 0])
        return 0

    # ------------------------------------------
    # Query Log (for debugging / ETL Control)
    # ------------------------------------------

    def get_slow_queries(self, threshold_ms: Optional[float] = None) -> List[QueryLogEntry]:
        """
        Return recent queries that exceeded the slow-query threshold.

        Args:
            threshold_ms: Override threshold (default: self.slow_query_threshold_ms)

        Returns:
            List of slow QueryLogEntry objects, newest first
        """
        threshold = threshold_ms or self.slow_query_threshold_ms
        slow = [q for q in self.query_log if q.elapsed_ms > threshold]
        return sorted(slow, key=lambda q: q.elapsed_ms, reverse=True)

    def get_recent_queries(self, limit: int = 10) -> List[QueryLogEntry]:
        """
        Return the most recent queries from the log.

        Args:
            limit: Maximum number of queries to return

        Returns:
            List of recent QueryLogEntry objects, newest first
        """
        return list(reversed(self.query_log[-limit:]))

    # ------------------------------------------
    # Internal
    # ------------------------------------------

    def _append_log(self, entry: QueryLogEntry) -> None:
        """Add a query to the log, evicting oldest if over capacity."""
        self.query_log.append(entry)
        if len(self.query_log) > self.max_query_log:
            self.query_log = self.query_log[-self.max_query_log:]

    def __repr__(self) -> str:
        return f"DuckDBClient(db_path='{self.db_path}', read_only={self.read_only})"
