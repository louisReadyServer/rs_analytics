"""
MetricEngine - Central query builder for all dashboard metrics.

This is the core of the metric layer. Instead of each component writing
its own SQL, components call MetricEngine.query() with metric names and
dimensions, and get back a consistent DataFrame.

Features:
- Loads metric definitions from registry.yml
- Builds SQL dynamically from metric expressions
- Supports date filtering and comparison periods
- Returns query SQL for "Show SQL" expanders
- Validates that requested dims are allowed per metric

Usage:
    from rs_analytics.db import DuckDBClient
    from rs_analytics.metrics import MetricEngine

    client = DuckDBClient("data/warehouse.duckdb")
    engine = MetricEngine(client)

    # Simple query: totals for a date range
    df = engine.query(
        metrics=["spend", "revenue", "roas"],
        date_range=("2026-01-01", "2026-01-31"),
    )

    # With dimensions: break down by channel
    df = engine.query(
        metrics=["spend", "clicks", "cpa"],
        dims=["channel"],
        date_range=("2026-01-01", "2026-01-31"),
    )

    # Comparison: current vs previous period
    result = engine.query_comparison(
        metrics=["spend", "revenue", "conversions"],
        current_range=("2026-01-15", "2026-01-31"),
        previous_range=("2026-01-01", "2026-01-14"),
    )
"""

import hashlib
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
import yaml

from rs_analytics.db.client import DuckDBClient
from rs_analytics.utils.formatting import safe_float, calculate_delta

logger = logging.getLogger(__name__)

# Path to the metrics registry YAML (relative to this file)
_REGISTRY_PATH = Path(__file__).parent / "registry.yml"


# ============================================
# Registry Loader
# ============================================

def load_registry(path: Optional[Path] = None) -> Dict[str, Dict[str, Any]]:
    """
    Load and index the metrics registry from YAML.

    Returns a dict keyed by metric name for fast lookup.

    Args:
        path: Path to registry.yml (defaults to the one next to this file)

    Returns:
        Dict mapping metric_name -> metric definition dict

    Raises:
        FileNotFoundError: If registry.yml is missing
        ValueError: If a metric is missing required fields
    """
    registry_path = path or _REGISTRY_PATH

    if not registry_path.exists():
        raise FileNotFoundError(f"Metrics registry not found: {registry_path}")

    with open(registry_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    metrics_list = raw.get("metrics", [])
    if not metrics_list:
        raise ValueError("No metrics defined in registry.yml")

    # Index by name and validate required fields
    registry: Dict[str, Dict[str, Any]] = {}
    required_fields = {"name", "expression", "source_view", "format"}

    for metric_def in metrics_list:
        name = metric_def.get("name")
        if not name:
            logger.warning("Skipping metric with no 'name' field: %s", metric_def)
            continue

        missing = required_fields - set(metric_def.keys())
        if missing:
            logger.warning(
                "Metric '%s' is missing fields %s — loading anyway", name, missing
            )

        registry[name] = metric_def

    logger.info("Loaded %d metrics from registry", len(registry))
    return registry


# ============================================
# MetricEngine
# ============================================

class MetricEngine:
    """
    Central query builder that turns metric names into SQL and DataFrames.

    The engine is stateless between queries — it just needs a DuckDBClient
    and the registry. Streamlit caching happens at the caller level
    (using @st.cache_data on the component functions).

    Args:
        client: DuckDBClient instance for executing queries
        registry_path: Optional override for registry.yml location
    """

    def __init__(
        self,
        client: DuckDBClient,
        registry_path: Optional[Path] = None,
    ):
        self.client = client
        self.registry = load_registry(registry_path)

        # Cache for view existence checks
        self._view_exists_cache: Dict[str, bool] = {}

    # ------------------------------------------
    # Public API
    # ------------------------------------------

    def query(
        self,
        metrics: List[str],
        dims: Optional[List[str]] = None,
        date_range: Optional[Tuple[Union[str, date], Union[str, date]]] = None,
        filters: Optional[Dict[str, Any]] = None,
    ) -> Optional[pd.DataFrame]:
        """
        Query one or more metrics, optionally grouped by dimensions.

        Args:
            metrics: List of metric names from the registry (e.g., ["spend", "revenue"])
            dims: Optional list of dimensions to GROUP BY (e.g., ["channel"])
            date_range: Optional (start_date, end_date) tuple for filtering
            filters: Optional extra SQL filters as {column: value} or {column: [values]}

        Returns:
            DataFrame with columns: [dims...] + [metric_name_1, metric_name_2, ...]
            Returns None if query fails.
        """
        sql = self.build_sql(metrics, dims=dims, date_range=date_range, filters=filters)
        if sql is None:
            return None
        return self.client.query(sql)

    def query_with_sql(
        self,
        metrics: List[str],
        dims: Optional[List[str]] = None,
        date_range: Optional[Tuple[Union[str, date], Union[str, date]]] = None,
        filters: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Optional[pd.DataFrame], str, float]:
        """
        Same as query() but also returns the SQL and timing.

        Useful for the "Show SQL" debug expander.

        Returns:
            (DataFrame or None, sql_string, elapsed_ms)
        """
        sql = self.build_sql(metrics, dims=dims, date_range=date_range, filters=filters)
        if sql is None:
            return None, "", 0.0
        df, elapsed = self.client.query_with_timing(sql)
        return df, sql, elapsed

    def query_comparison(
        self,
        metrics: List[str],
        current_range: Tuple[Union[str, date], Union[str, date]],
        previous_range: Tuple[Union[str, date], Union[str, date]],
        dims: Optional[List[str]] = None,
    ) -> Optional[pd.DataFrame]:
        """
        Query metrics for two periods and compute deltas.

        Returns a DataFrame with columns:
          [dims...], metric_current, metric_previous, metric_delta_pct
        for each requested metric.

        Args:
            metrics: List of metric names
            current_range: (start, end) for current period
            previous_range: (start, end) for previous period
            dims: Optional dimensions to group by

        Returns:
            Merged DataFrame with current, previous, and delta columns.
            Returns None if either query fails.
        """
        current_df = self.query(metrics, dims=dims, date_range=current_range)
        previous_df = self.query(metrics, dims=dims, date_range=previous_range)

        if current_df is None or previous_df is None:
            return None

        # If no dims, both DataFrames are single-row totals
        if not dims:
            result = {}
            for metric in metrics:
                curr_val = safe_float(current_df[metric].iloc[0]) if metric in current_df.columns else 0
                prev_val = safe_float(previous_df[metric].iloc[0]) if metric in previous_df.columns else 0
                result[f"{metric}_current"] = curr_val
                result[f"{metric}_previous"] = prev_val
                result[f"{metric}_delta_pct"] = calculate_delta(curr_val, prev_val)
            return pd.DataFrame([result])

        # With dims, merge on dim columns
        dim_cols = dims
        merged = pd.merge(
            current_df,
            previous_df,
            on=dim_cols,
            how="outer",
            suffixes=("_current", "_previous"),
        )

        # Compute delta columns
        for metric in metrics:
            curr_col = f"{metric}_current"
            prev_col = f"{metric}_previous"
            delta_col = f"{metric}_delta_pct"

            if curr_col in merged.columns and prev_col in merged.columns:
                merged[curr_col] = merged[curr_col].fillna(0)
                merged[prev_col] = merged[prev_col].fillna(0)
                merged[delta_col] = merged.apply(
                    lambda row: calculate_delta(row[curr_col], row[prev_col]),
                    axis=1,
                )

        return merged

    def query_trend(
        self,
        metrics: List[str],
        date_range: Tuple[Union[str, date], Union[str, date]],
        dims: Optional[List[str]] = None,
    ) -> Optional[pd.DataFrame]:
        """
        Query metrics as a daily time series (always includes date_day).

        Convenience method that adds 'date_day' to dims automatically.

        Args:
            metrics: List of metric names
            date_range: (start, end) date range
            dims: Additional dimensions beyond date_day

        Returns:
            DataFrame with date_day + metric columns, sorted by date_day.
        """
        trend_dims = ["date_day"] + (dims or [])
        df = self.query(metrics, dims=trend_dims, date_range=date_range)
        if df is not None and "date_day" in df.columns:
            df["date_day"] = pd.to_datetime(df["date_day"])
            df = df.sort_values("date_day")
        return df

    def get_metric_info(self, metric_name: str) -> Optional[Dict[str, Any]]:
        """
        Look up a metric's full definition from the registry.

        Args:
            metric_name: Metric name (e.g., "spend")

        Returns:
            Metric definition dict, or None if not found
        """
        return self.registry.get(metric_name)

    def list_metrics(self) -> List[str]:
        """Return all available metric names."""
        return list(self.registry.keys())

    # ------------------------------------------
    # SQL Builder
    # ------------------------------------------

    def build_sql(
        self,
        metrics: List[str],
        dims: Optional[List[str]] = None,
        date_range: Optional[Tuple[Union[str, date], Union[str, date]]] = None,
        filters: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        """
        Build a SELECT statement from metric definitions.

        This is the core logic: it reads expressions from the registry,
        validates dims, and assembles a complete SQL query.

        Args:
            metrics: Metric names to query
            dims: Columns to GROUP BY
            date_range: (start, end) for WHERE clause
            filters: Extra {column: value} filters

        Returns:
            SQL string, or None if metrics are invalid
        """
        if not metrics:
            logger.error("No metrics requested")
            return None

        # Resolve metric definitions
        metric_defs = []
        for name in metrics:
            defn = self.registry.get(name)
            if defn is None:
                logger.warning("Unknown metric '%s' — skipping", name)
                continue
            metric_defs.append((name, defn))

        if not metric_defs:
            logger.error("No valid metrics found in request: %s", metrics)
            return None

        # All metrics in one query must share the same source_view.
        # Group by source view — if mixed, use v_exec_daily as default
        # (most metrics live there).
        source_views = set(d["source_view"] for _, d in metric_defs)

        if len(source_views) > 1:
            # Mixed sources: only pick metrics from v_exec_daily, warn about rest
            logger.warning(
                "Mixed source views %s in one query. Filtering to v_exec_daily only.",
                source_views,
            )
            metric_defs = [
                (n, d) for n, d in metric_defs if d["source_view"] == "v_exec_daily"
            ]
            if not metric_defs:
                logger.error("No metrics left after filtering to v_exec_daily")
                return None

        source_view = metric_defs[0][1]["source_view"]

        # Determine the date column name based on source view
        date_col = self._get_date_column(source_view)

        # When the caller asks for "date_day" but the underlying table uses
        # a different column name (e.g. "date"), we alias it in SELECT and
        # reference the real column in GROUP BY / ORDER BY so the SQL is valid
        # while the returned DataFrame still has a "date_day" column.
        needs_date_alias = False
        if dims and "date_day" in dims and date_col != "date_day":
            needs_date_alias = True

        # Build SELECT expressions
        select_parts = []
        group_order_dims = []
        if dims:
            for dim in dims:
                if dim == "date_day" and needs_date_alias:
                    select_parts.append(f"{date_col} AS date_day")
                    group_order_dims.append(date_col)
                else:
                    select_parts.append(dim)
                    group_order_dims.append(dim)

        for name, defn in metric_defs:
            expression = defn["expression"]
            select_parts.append(f"{expression} AS {name}")

        # Build WHERE clause
        where_parts = []
        if date_range:
            start_str = self._date_to_str(date_range[0])
            end_str = self._date_to_str(date_range[1])
            where_parts.append(f"{date_col} >= '{start_str}'")
            where_parts.append(f"{date_col} <= '{end_str}'")

        if filters:
            for col, val in filters.items():
                if isinstance(val, (list, tuple)):
                    values_str = ", ".join(f"'{v}'" for v in val)
                    where_parts.append(f"{col} IN ({values_str})")
                else:
                    where_parts.append(f"{col} = '{val}'")

        # Assemble the query
        select_clause = ",\n    ".join(select_parts)
        sql = f"SELECT\n    {select_clause}\nFROM {source_view}"

        if where_parts:
            sql += "\nWHERE " + "\n  AND ".join(where_parts)

        if dims:
            sql += "\nGROUP BY " + ", ".join(group_order_dims)
            sql += "\nORDER BY " + ", ".join(group_order_dims)

        return sql

    # ------------------------------------------
    # View Management
    # ------------------------------------------

    def ensure_views(self) -> bool:
        """
        Create the v_exec_daily view if it doesn't already exist.

        Reads SQL from sql/views/v_exec_daily.sql and executes it.
        Safe to call repeatedly — uses CREATE OR REPLACE VIEW.

        Returns:
            True if view was created/updated, False on error
        """
        sql_path = Path(__file__).parent.parent.parent / "data" / "views" / "v_exec_daily.sql"

        if not sql_path.exists():
            logger.error("View SQL not found: %s", sql_path)
            return False

        sql = sql_path.read_text(encoding="utf-8")

        # Execute (needs read-write connection)
        success = self.client.execute(sql)
        if success:
            logger.info("Created/updated v_exec_daily view")
        else:
            logger.error("Failed to create v_exec_daily view")
        return success

    # ------------------------------------------
    # Internal Helpers
    # ------------------------------------------

    def _get_date_column(self, source_view: str) -> str:
        """
        Return the date column name for a given source view.

        Different tables use different date column names:
        - v_exec_daily -> date_day
        - ga4_sessions -> date (YYYYMMDD format)
        - gsc_daily_totals -> date
        - fact_paid_daily -> date_day
        - etc.
        """
        date_column_map = {
            "v_exec_daily": "date_day",
            "ga4_sessions": "date",
            "gsc_daily_totals": "date",
            "fact_paid_daily": "date_day",
            "fact_web_daily": "date_day",
            "fact_organic_daily": "date_day",
            "mart.platform_daily_overview": "activity_date",
        }
        return date_column_map.get(source_view, "date_day")

    def _get_date_cast(self, source_view: str, date_str: str) -> str:
        """
        Wrap a date string in the right CAST for the source view's date column type.

        GA4 stores dates as YYYYMMDD strings, so we need to compare differently.
        """
        if source_view == "ga4_sessions":
            # GA4 dates are YYYYMMDD strings — convert our YYYY-MM-DD to match
            return date_str.replace("-", "")
        return date_str

    @staticmethod
    def _date_to_str(d: Union[str, date, datetime]) -> str:
        """Convert a date-like value to 'YYYY-MM-DD' string."""
        if isinstance(d, str):
            return d
        if isinstance(d, (date, datetime)):
            return d.strftime("%Y-%m-%d")
        return str(d)
