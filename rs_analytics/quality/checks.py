"""
Data Quality Checker with PASS / WARN / FAIL gate.

Runs freshness, PK uniqueness, and null-rate checks against the DuckDB
warehouse. Returns a structured result that the Executive Dashboard uses
to show a trust banner and optionally disable misleading comparisons.

All rules are hardcoded here (no external YAML). For a local app this
is simpler to maintain — just edit the dicts below.

Usage:
    from rs_analytics.quality.checks import DataQualityChecker

    checker = DataQualityChecker(client)
    result = checker.run_all()
    print(result.status)       # 'PASS' | 'WARN' | 'FAIL'
    print(result.summary)      # Human-readable one-liner
    print(result.freshness)    # List of FreshnessResult
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional

import pandas as pd

from rs_analytics.db.client import DuckDBClient
from rs_analytics.db.adapters import SOURCE_FRESHNESS

logger = logging.getLogger(__name__)


# ============================================
# Result Dataclasses
# ============================================

@dataclass
class FreshnessResult:
    """Result of a single freshness check."""
    table: str                   # Table name checked
    label: str                   # Human label (e.g. "Google Ads")
    last_date: Optional[date]    # Most recent date in the table
    hours_since: Optional[float] # Hours since last_date (None if no data)
    expected_hours: float        # Maximum acceptable delay
    critical: bool               # If True, failure → overall FAIL
    status: str                  # 'pass' | 'warn' | 'fail' | 'no_data'


@dataclass
class PKResult:
    """Result of a primary-key uniqueness check."""
    table: str
    key_columns: List[str]
    duplicate_count: int         # Number of extra rows (0 = clean)
    status: str                  # 'pass' | 'fail'


@dataclass
class NullResult:
    """Result of a null-rate check on a critical column."""
    table: str
    column: str
    total_rows: int
    null_count: int
    null_pct: float              # 0.0 - 1.0
    threshold: float             # Maximum acceptable null rate
    status: str                  # 'pass' | 'warn' | 'fail'


@dataclass
class DataQualityResult:
    """
    Aggregate result of all data quality checks.

    The Executive Dashboard reads .status to decide what banner to show:
    - PASS  → green "Data Trust: PASS"
    - WARN  → yellow banner with affected sources
    - FAIL  → red banner, comparisons may be disabled
    """
    status: str                              # 'PASS' | 'WARN' | 'FAIL'
    summary: str                             # One-line summary for the banner
    freshness: List[FreshnessResult] = field(default_factory=list)
    pk_checks: List[PKResult] = field(default_factory=list)
    null_checks: List[NullResult] = field(default_factory=list)
    row_counts: Dict[str, int] = field(default_factory=dict)


# ============================================
# Rules (hardcoded for local app simplicity)
# ============================================

# Primary key uniqueness rules
# table -> list of columns that should be unique together
#
# NOTE: gads_daily_summary is excluded because its campaign_id column
# is always 0 (the GAds ETL doesn't populate it). The rows ARE distinct
# (different campaigns per day), just not identifiable by ID. The proper
# campaign-level PK check uses gads_campaigns instead.
PK_RULES: Dict[str, List[str]] = {
    "gads_campaigns":          ["date", "campaign_id"],
    "meta_campaign_insights":  ["date", "ad_account_id", "campaign_id"],
    "ga4_sessions":            ["date"],
    "gsc_daily_totals":        ["_dataset", "date"],
    "af_daily_geo":            ["date", "country", "platform", "app_id", "media_source", "campaign"],
}

# Null-rate rules for critical columns
# (table, column) -> max acceptable null fraction (0.0 = no nulls allowed)
NULL_RULES: Dict[str, Dict[str, float]] = {
    "gads_campaigns": {
        "campaign_id": 0.0,
        "date": 0.0,
        "cost": 0.01,
    },
    "meta_campaign_insights": {
        "campaign_id": 0.0,
        "date": 0.0,
        "spend": 0.01,
    },
    "core.fact_points_ledger": {
        "user_id": 0.0,
        "ledger_ts": 0.0,
        "points_delta": 0.0,
    },
}


# ============================================
# DataQualityChecker
# ============================================

class DataQualityChecker:
    """
    Runs freshness, PK, and null checks against the DuckDB warehouse.

    Args:
        client: DuckDBClient instance
    """

    def __init__(self, client: DuckDBClient):
        self.client = client

    # ------------------------------------------
    # Main entry point
    # ------------------------------------------

    def run_all(self) -> DataQualityResult:
        """
        Run all data quality checks and compute an overall PASS/WARN/FAIL status.

        Decision logic:
        - FAIL if: any critical freshness check fails, OR any PK check finds > 0 duplicates
        - WARN if: any non-critical freshness check fails, OR any null check exceeds threshold
        - PASS otherwise

        Returns:
            DataQualityResult with full details
        """
        freshness_results = self.check_freshness()
        pk_results = self.check_pk_uniqueness()
        null_results = self.check_nulls()
        row_counts = self._get_row_counts()

        # Compute overall status
        has_critical_fail = any(
            r.status == "fail" and r.critical for r in freshness_results
        )
        has_pk_fail = any(r.status == "fail" for r in pk_results)
        has_freshness_warn = any(
            r.status == "fail" and not r.critical for r in freshness_results
        )
        has_null_warn = any(r.status in ("warn", "fail") for r in null_results)
        has_no_data = any(r.status == "no_data" for r in freshness_results)

        if has_critical_fail or has_pk_fail:
            status = "FAIL"
        elif has_freshness_warn or has_null_warn or has_no_data:
            status = "WARN"
        else:
            status = "PASS"

        # Build summary line
        summary = self._build_summary(
            status, freshness_results, pk_results, null_results
        )

        return DataQualityResult(
            status=status,
            summary=summary,
            freshness=freshness_results,
            pk_checks=pk_results,
            null_checks=null_results,
            row_counts=row_counts,
        )

    # ------------------------------------------
    # Freshness Checks
    # ------------------------------------------

    def check_freshness(self) -> List[FreshnessResult]:
        """
        Check data freshness for each source table.

        Compares MAX(date_column) against the expected delay window
        defined in SOURCE_FRESHNESS (from adapters.py).

        Returns:
            List of FreshnessResult, one per source
        """
        results = []
        now = datetime.now()

        for table, config in SOURCE_FRESHNESS.items():
            label = config["label"]
            date_col = config["date_column"]
            date_fmt = config.get("date_format")
            expected_hours = config["expected_delay_hours"]
            critical = config["critical"]

            # Check if table exists
            if not self.client.table_exists(table):
                results.append(FreshnessResult(
                    table=table, label=label, last_date=None,
                    hours_since=None, expected_hours=expected_hours,
                    critical=critical, status="no_data",
                ))
                continue

            # Query the most recent date
            sql = f'SELECT MAX("{date_col}") as last_date FROM {table}'
            df = self.client.query(sql)

            if df is None or df.empty or df.iloc[0]["last_date"] is None:
                results.append(FreshnessResult(
                    table=table, label=label, last_date=None,
                    hours_since=None, expected_hours=expected_hours,
                    critical=critical, status="no_data",
                ))
                continue

            raw_date = df.iloc[0]["last_date"]
            last_date = self._parse_date(raw_date, date_fmt)

            if last_date is None:
                results.append(FreshnessResult(
                    table=table, label=label, last_date=None,
                    hours_since=None, expected_hours=expected_hours,
                    critical=critical, status="no_data",
                ))
                continue

            # Calculate hours since last data
            last_datetime = datetime.combine(last_date, datetime.min.time())
            hours_since = (now - last_datetime).total_seconds() / 3600

            # Determine status
            if hours_since <= expected_hours:
                check_status = "pass"
            elif hours_since <= expected_hours * 1.5:
                # Within 1.5x the expected window → warn
                check_status = "warn"
            else:
                check_status = "fail"

            results.append(FreshnessResult(
                table=table, label=label, last_date=last_date,
                hours_since=round(hours_since, 1),
                expected_hours=expected_hours,
                critical=critical, status=check_status,
            ))

        return results

    # ------------------------------------------
    # PK Uniqueness Checks
    # ------------------------------------------

    def check_pk_uniqueness(self) -> List[PKResult]:
        """
        Check primary key uniqueness for critical tables.

        Returns:
            List of PKResult, one per table
        """
        results = []

        for table, key_columns in PK_RULES.items():
            if not self.client.table_exists(table):
                continue

            key_cols_str = ", ".join(f'"{c}"' for c in key_columns)
            sql = f"""
                SELECT COUNT(*) as dup_groups, COALESCE(SUM(cnt - 1), 0) as extra_rows
                FROM (
                    SELECT {key_cols_str}, COUNT(*) as cnt
                    FROM {table}
                    GROUP BY {key_cols_str}
                    HAVING COUNT(*) > 1
                )
            """
            df = self.client.query(sql)

            if df is not None and not df.empty:
                extra_rows = int(df.iloc[0]["extra_rows"])
                status = "pass" if extra_rows == 0 else "fail"
            else:
                extra_rows = 0
                status = "pass"

            results.append(PKResult(
                table=table,
                key_columns=key_columns,
                duplicate_count=extra_rows,
                status=status,
            ))

        return results

    # ------------------------------------------
    # Null Rate Checks
    # ------------------------------------------

    def check_nulls(self) -> List[NullResult]:
        """
        Check null rates for critical columns.

        Returns:
            List of NullResult, one per (table, column)
        """
        results = []

        for table, columns in NULL_RULES.items():
            if not self.client.table_exists(table):
                continue

            # Get total row count once per table
            total_df = self.client.query(f"SELECT COUNT(*) as cnt FROM {table}")
            total_rows = int(total_df.iloc[0]["cnt"]) if total_df is not None else 0

            if total_rows == 0:
                continue

            for column, threshold in columns.items():
                null_df = self.client.query(
                    f'SELECT COUNT(*) as cnt FROM {table} WHERE "{column}" IS NULL'
                )
                null_count = int(null_df.iloc[0]["cnt"]) if null_df is not None else 0
                null_pct = null_count / total_rows

                if null_pct <= threshold:
                    status = "pass"
                elif null_pct <= threshold * 2:
                    status = "warn"
                else:
                    status = "fail"

                results.append(NullResult(
                    table=table, column=column,
                    total_rows=total_rows, null_count=null_count,
                    null_pct=round(null_pct, 4), threshold=threshold,
                    status=status,
                ))

        return results

    # ------------------------------------------
    # Row Counts
    # ------------------------------------------

    def _get_row_counts(self) -> Dict[str, int]:
        """Get row counts for all source tables (for display)."""
        counts = {}
        for table in SOURCE_FRESHNESS:
            if self.client.table_exists(table):
                counts[table] = self.client.row_count(table)
            else:
                counts[table] = 0
        return counts

    # ------------------------------------------
    # Helpers
    # ------------------------------------------

    def _parse_date(self, raw_value: Any, date_format: Optional[str]) -> Optional[date]:
        """
        Parse a raw date value into a Python date object.

        Handles:
        - datetime.date objects (returned by DuckDB for DATE columns)
        - Strings in YYYYMMDD or YYYY-MM-DD format
        - Pandas Timestamp
        """
        if isinstance(raw_value, date) and not isinstance(raw_value, datetime):
            return raw_value
        if isinstance(raw_value, datetime):
            return raw_value.date()
        if isinstance(raw_value, pd.Timestamp):
            return raw_value.date()
        if isinstance(raw_value, str):
            raw_value = raw_value.strip()
            if len(raw_value) == 8 and raw_value.isdigit():
                # YYYYMMDD
                return datetime.strptime(raw_value, "%Y%m%d").date()
            try:
                return datetime.strptime(raw_value[:10], "%Y-%m-%d").date()
            except ValueError:
                logger.warning("Cannot parse date: %s", raw_value)
                return None
        return None

    def _build_summary(
        self,
        status: str,
        freshness: List[FreshnessResult],
        pk_checks: List[PKResult],
        null_checks: List[NullResult],
    ) -> str:
        """Build a human-readable one-line summary."""
        if status == "PASS":
            return "All sources within expected freshness windows. No data issues detected."

        issues = []

        # Freshness issues
        stale_sources = [
            r.label for r in freshness if r.status in ("fail", "no_data")
        ]
        if stale_sources:
            issues.append(f"Stale: {', '.join(stale_sources)}")

        # PK issues
        pk_fails = [r.table for r in pk_checks if r.status == "fail"]
        if pk_fails:
            issues.append(f"Duplicates in: {', '.join(pk_fails)}")

        # Null issues
        null_fails = [
            f"{r.table}.{r.column}" for r in null_checks if r.status in ("warn", "fail")
        ]
        if null_fails:
            issues.append(f"Nulls in: {', '.join(null_fails)}")

        return " | ".join(issues) if issues else "Minor issues detected."
