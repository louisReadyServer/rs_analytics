"""
Cohort Funnel Engine with Time-to-Convert Windows.

Builds cohorts from User Logs warehouse data and tracks how users
progress through lifecycle stages over time. Unlike the static funnel
in app_analytics.py (which counts "ever reached" state), this module
tracks WHEN users convert and produces time-bucketed analysis.

Core Concept:
    A "cohort" is a group of users who signed up in the same period
    (week or month). For each cohort, we track what % of users reached
    each stage and HOW LONG it took them.

Stages:
    1. Signup       → core.dim_user.registration_ts
    2. Verified     → core.dim_user.mobile_verified_at
    3. First VPS    → MIN(event_ts) WHERE activity_type = 'LAUNCH_SERVER'
    4. First Paid   → MIN(payment_ts) from core.fact_payment_topup

Output DataFrames:
    - cohort_progression: cohort × stage × time_bucket → user_count, cumulative_pct
    - cohort_summary: cohort → total_users, conversion rates, median time-to-convert
    - time_to_convert: individual user-level time deltas (for distributions)

Usage:
    from rs_analytics.metrics.cohorts import CohortEngine

    engine = CohortEngine(client)
    summary = engine.cohort_summary(granularity="week")
    progression = engine.cohort_progression(granularity="week", max_days=90)
"""

import logging
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Dict, List, Literal, Optional, Tuple

import pandas as pd

from rs_analytics.db.client import DuckDBClient

logger = logging.getLogger(__name__)

# Type alias for cohort granularity
Granularity = Literal["week", "month"]


# ============================================
# Cohort Engine
# ============================================

class CohortEngine:
    """
    Build cohort funnels with time-to-convert windows from the User Logs warehouse.

    Requires:
        - core.dim_user (registration_ts, mobile_verified_at)
        - core.fact_user_activity (user_id, event_ts, activity_type)
        - core.fact_payment_topup (user_id, payment_ts)

    Args:
        client: DuckDBClient connected to the DuckDB warehouse
    """

    def __init__(self, client: DuckDBClient):
        self.client = client

    # ------------------------------------------
    # Public Methods
    # ------------------------------------------

    def cohort_summary(
        self,
        granularity: Granularity = "week",
        min_cohort_size: int = 3,
    ) -> Optional[pd.DataFrame]:
        """
        Get a summary of each cohort's conversion rates.

        Returns a DataFrame with one row per cohort:
            - cohort_start: date (start of the week/month)
            - cohort_size: total signups
            - verified_count, verified_pct
            - first_vps_count, first_vps_pct
            - first_paid_count, first_paid_pct
            - median_days_to_verify, median_days_to_vps, median_days_to_paid

        Args:
            granularity: "week" or "month" — how to group cohorts
            min_cohort_size: Exclude cohorts smaller than this (reduces noise)

        Returns:
            DataFrame or None if no data
        """
        # Build the base CTE that assigns each user to a cohort
        # and finds their milestone timestamps
        cohort_cte = self._build_cohort_cte(granularity)

        sql = f"""
        {cohort_cte}
        SELECT
            cohort_start,
            -- Cohort size
            COUNT(*)                                          AS cohort_size,

            -- Stage counts
            SUM(CASE WHEN verified_at IS NOT NULL THEN 1 ELSE 0 END)   AS verified_count,
            SUM(CASE WHEN first_vps_at IS NOT NULL THEN 1 ELSE 0 END)  AS first_vps_count,
            SUM(CASE WHEN first_paid_at IS NOT NULL THEN 1 ELSE 0 END) AS first_paid_count,

            -- Conversion rates (as %)
            ROUND(100.0 * SUM(CASE WHEN verified_at IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1)
                AS verified_pct,
            ROUND(100.0 * SUM(CASE WHEN first_vps_at IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1)
                AS first_vps_pct,
            ROUND(100.0 * SUM(CASE WHEN first_paid_at IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1)
                AS first_paid_pct,

            -- Median time-to-convert (in days)
            MEDIAN(days_to_verify)                            AS median_days_to_verify,
            MEDIAN(days_to_vps)                               AS median_days_to_vps,
            MEDIAN(days_to_paid)                              AS median_days_to_paid

        FROM user_cohorts
        GROUP BY cohort_start
        HAVING COUNT(*) >= {min_cohort_size}
        ORDER BY cohort_start
        """

        df = self.client.query(sql)
        if df is None or df.empty:
            logger.warning("No cohort data available — are user logs loaded?")
            return None

        return df

    def cohort_progression(
        self,
        granularity: Granularity = "week",
        max_days: int = 90,
        bucket_days: int = 7,
        min_cohort_size: int = 3,
    ) -> Optional[pd.DataFrame]:
        """
        Get cumulative conversion curves: what % of each cohort reached
        each stage within N days of signup.

        Returns a DataFrame with columns:
            - cohort_start, stage, days_bucket, cumulative_pct, user_count

        This powers the "time-to-convert" charts:
            X = days since signup, Y = cumulative %, color = cohort

        Args:
            granularity: "week" or "month"
            max_days: Maximum days after signup to track (default 90)
            bucket_days: Width of each time bucket in days (default 7)
            min_cohort_size: Exclude small cohorts

        Returns:
            DataFrame or None
        """
        cohort_cte = self._build_cohort_cte(granularity)

        # Generate time buckets: 0, 7, 14, 21, ... up to max_days
        buckets = list(range(0, max_days + 1, bucket_days))

        # For each stage and bucket, count how many users converted within that window
        stage_cases = []
        for bucket in buckets:
            for stage, col in [
                ("Verified", "days_to_verify"),
                ("First VPS", "days_to_vps"),
                ("First Paid", "days_to_paid"),
            ]:
                stage_cases.append(f"""
                    SELECT
                        cohort_start,
                        '{stage}' AS stage,
                        {bucket} AS days_bucket,
                        SUM(CASE WHEN {col} IS NOT NULL AND {col} <= {bucket} THEN 1 ELSE 0 END) AS user_count,
                        COUNT(*) AS cohort_size
                    FROM user_cohorts
                    GROUP BY cohort_start
                    HAVING COUNT(*) >= {min_cohort_size}
                """)

        union_sql = " UNION ALL ".join(stage_cases)

        sql = f"""
        {cohort_cte},
        progression AS (
            {union_sql}
        )
        SELECT
            cohort_start,
            stage,
            days_bucket,
            user_count,
            cohort_size,
            ROUND(100.0 * user_count / cohort_size, 1) AS cumulative_pct
        FROM progression
        WHERE user_count > 0
        ORDER BY cohort_start, stage, days_bucket
        """

        df = self.client.query(sql)
        if df is None or df.empty:
            logger.warning("No cohort progression data available")
            return None

        return df

    def time_to_convert_distribution(
        self,
        stage: Literal["verify", "vps", "paid"] = "paid",
        max_days: int = 90,
    ) -> Optional[pd.DataFrame]:
        """
        Get the distribution of time-to-convert for a specific stage.

        Returns a DataFrame with one row per user who converted:
            - user_id, signup_date, convert_date, days_to_convert

        Useful for histogram / violin plots.

        Args:
            stage: Which conversion stage to analyze
            max_days: Only include conversions within this many days

        Returns:
            DataFrame or None
        """
        col_map = {
            "verify": ("verified_at", "days_to_verify"),
            "vps": ("first_vps_at", "days_to_vps"),
            "paid": ("first_paid_at", "days_to_paid"),
        }

        if stage not in col_map:
            raise ValueError(f"Unknown stage: {stage}. Use: {list(col_map.keys())}")

        ts_col, days_col = col_map[stage]
        cohort_cte = self._build_cohort_cte("week")

        sql = f"""
        {cohort_cte}
        SELECT
            user_id,
            cohort_start,
            signup_date,
            DATE({ts_col}) AS convert_date,
            {days_col} AS days_to_convert
        FROM user_cohorts
        WHERE {ts_col} IS NOT NULL
          AND {days_col} <= {max_days}
        ORDER BY {days_col}
        """

        df = self.client.query(sql)
        if df is None or df.empty:
            logger.info("No conversions found for stage=%s within %d days", stage, max_days)
            return None

        return df

    def cohort_retention(
        self,
        granularity: Granularity = "week",
        metric: Literal["active", "paid"] = "active",
        max_periods: int = 12,
        min_cohort_size: int = 3,
    ) -> Optional[pd.DataFrame]:
        """
        Build a classic retention matrix: for each cohort, what % of users
        were active (or paid) in period N after signup.

        Returns a DataFrame with columns:
            - cohort_start, period_offset, user_count, cohort_size, retention_pct

        Args:
            granularity: "week" or "month"
            metric: "active" (any activity) or "paid" (made a payment)
            max_periods: Number of periods to track
            min_cohort_size: Minimum cohort size

        Returns:
            DataFrame or None
        """
        # Determine the date truncation function
        trunc_fn = "DATE_TRUNC('week', registration_ts)" if granularity == "week" else "DATE_TRUNC('month', registration_ts)"
        period_trunc = "DATE_TRUNC('week', event_date)" if granularity == "week" else "DATE_TRUNC('month', event_date)"

        if metric == "active":
            event_source = """
                SELECT user_id, DATE(event_ts) AS event_date
                FROM core.fact_user_activity
            """
        else:
            event_source = """
                SELECT user_id, DATE(payment_ts) AS event_date
                FROM core.fact_payment_topup
            """

        sql = f"""
        WITH cohorts AS (
            SELECT
                user_id,
                {trunc_fn}::DATE AS cohort_start
            FROM core.dim_user
            WHERE registration_ts IS NOT NULL
        ),
        events AS (
            {event_source}
        ),
        user_periods AS (
            SELECT DISTINCT
                c.user_id,
                c.cohort_start,
                {period_trunc}::DATE AS event_period
            FROM cohorts c
            JOIN events e ON c.user_id = e.user_id
        ),
        retention AS (
            SELECT
                cohort_start,
                -- Calculate period offset in weeks or months
                CASE
                    WHEN '{granularity}' = 'week'
                        THEN (event_period - cohort_start) / 7
                    ELSE DATEDIFF('month', cohort_start, event_period)
                END AS period_offset,
                COUNT(DISTINCT user_id) AS user_count
            FROM user_periods
            GROUP BY cohort_start, period_offset
        ),
        cohort_sizes AS (
            SELECT cohort_start, COUNT(*) AS cohort_size
            FROM cohorts
            GROUP BY cohort_start
            HAVING COUNT(*) >= {min_cohort_size}
        )
        SELECT
            r.cohort_start,
            r.period_offset,
            r.user_count,
            cs.cohort_size,
            ROUND(100.0 * r.user_count / cs.cohort_size, 1) AS retention_pct
        FROM retention r
        JOIN cohort_sizes cs ON r.cohort_start = cs.cohort_start
        WHERE r.period_offset >= 0 AND r.period_offset <= {max_periods}
        ORDER BY r.cohort_start, r.period_offset
        """

        df = self.client.query(sql)
        if df is None or df.empty:
            logger.warning("No retention data available")
            return None

        return df

    # ------------------------------------------
    # Internal: Build Cohort CTE
    # ------------------------------------------

    def _build_cohort_cte(self, granularity: Granularity) -> str:
        """
        Build a CTE that assigns each user to a cohort and computes
        their milestone timestamps + days-to-convert for each stage.

        Returns:
            SQL string starting with "WITH user_cohorts AS (...)"
        """
        # Truncate registration date to week or month to define the cohort
        if granularity == "week":
            trunc_fn = "DATE_TRUNC('week', u.registration_ts)::DATE"
        else:
            trunc_fn = "DATE_TRUNC('month', u.registration_ts)::DATE"

        return f"""
        WITH user_cohorts AS (
            SELECT
                u.user_id,

                -- Cohort assignment
                {trunc_fn}                                AS cohort_start,
                DATE(u.registration_ts)                   AS signup_date,

                -- Stage 2: Mobile verification timestamp
                u.mobile_verified_at                      AS verified_at,

                -- Stage 3: First VPS launch
                first_vps.first_vps_at,

                -- Stage 4: First paid top-up
                first_pay.first_paid_at,

                -- Time-to-convert (days from signup to each stage)
                DATEDIFF('day', u.registration_ts, u.mobile_verified_at)
                    AS days_to_verify,
                DATEDIFF('day', u.registration_ts, first_vps.first_vps_at)
                    AS days_to_vps,
                DATEDIFF('day', u.registration_ts, first_pay.first_paid_at)
                    AS days_to_paid

            FROM core.dim_user u

            -- Stage 3: First VPS launch (LEFT JOIN so users without VPS still appear)
            LEFT JOIN (
                SELECT
                    user_id,
                    MIN(event_ts) AS first_vps_at
                FROM core.fact_user_activity
                WHERE activity_type = 'LAUNCH_SERVER'
                GROUP BY user_id
            ) first_vps ON u.user_id = first_vps.user_id

            -- Stage 4: First payment (LEFT JOIN)
            LEFT JOIN (
                SELECT
                    user_id,
                    MIN(payment_ts) AS first_paid_at
                FROM core.fact_payment_topup
                GROUP BY user_id
            ) first_pay ON u.user_id = first_pay.user_id

            WHERE u.registration_ts IS NOT NULL
        )
        """
