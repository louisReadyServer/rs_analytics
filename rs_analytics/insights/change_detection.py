"""
Deterministic Change Detection + Driver Analysis.

Replaces the old "What Changed" section in the Executive Dashboard with
structured, threshold-based change detection that explains WHY a metric
moved, not just THAT it moved.

How it works:
1. Query v_exec_daily for current and previous periods
2. For each metric, calculate absolute and percentage change
3. Apply thresholds (min_volume + min_pct must BOTH be exceeded)
4. For significant changes, break down by channel to find top drivers
5. Build human-readable insight sentences

No ML, no heuristics — just deterministic rules you can audit.

Usage:
    from rs_analytics.insights.change_detection import ChangeDetector

    detector = ChangeDetector(engine)
    events = detector.detect(
        current_range=("2026-02-01", "2026-02-12"),
        previous_range=("2026-01-20", "2026-01-31"),
    )
    for event in events:
        print(event.sentence)
        # "Spend up +22.1% ($2,200) driven by Google Ads (+$1,500)"
"""

import logging
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd

from rs_analytics.metrics.engine import MetricEngine
from rs_analytics.utils.formatting import safe_float, safe_divide
from rs_analytics.db.adapters import normalize_channel

logger = logging.getLogger(__name__)


# ============================================
# Data Classes
# ============================================

@dataclass
class Driver:
    """A single dimension value that contributed to a metric change."""
    dim_value: str          # e.g. "google_ads"
    dim_label: str          # e.g. "Google Ads" (human-readable)
    current: float          # Current period value
    previous: float         # Previous period value
    abs_change: float       # current - previous
    pct_of_total_change: float  # What fraction of the total change this accounts for


@dataclass
class ChangeEvent:
    """
    A single detected metric change with explanation.

    The Executive Dashboard renders these as insight cards.
    """
    metric: str             # Registry metric name (e.g. "spend")
    metric_label: str       # Human label (e.g. "Ad Spend")
    current_value: float
    previous_value: float
    abs_change: float       # current - previous
    pct_change: float       # Percentage change
    direction: str          # "up" or "down"
    significance: str       # "high" (>25% or large abs) | "medium" | "low"
    top_drivers: List[Driver]  # Top contributing dimensions
    sentence: str           # Pre-built human-readable insight


# ============================================
# Thresholds
# ============================================

# A change must exceed BOTH min_volume AND min_pct to be reported.
# This prevents noisy alerts on tiny absolute changes or tiny percentages
# on large bases.

CHANGE_THRESHOLDS: Dict[str, Dict[str, float]] = {
    "spend": {
        "min_volume": 50.0,     # Must change by at least $50
        "min_pct": 0.10,        # AND at least 10%
    },
    "revenue": {
        "min_volume": 50.0,
        "min_pct": 0.10,
    },
    "conversions": {
        "min_volume": 5.0,      # At least 5 conversions change
        "min_pct": 0.15,        # AND at least 15%
    },
    "clicks": {
        "min_volume": 100.0,
        "min_pct": 0.10,
    },
    "installs": {
        "min_volume": 10.0,
        "min_pct": 0.15,
    },
    "signups": {
        "min_volume": 5.0,
        "min_pct": 0.15,
    },
}

# Default threshold for any metric not listed above
DEFAULT_THRESHOLD = {
    "min_volume": 10.0,
    "min_pct": 0.10,
}


# ============================================
# ChangeDetector
# ============================================

class ChangeDetector:
    """
    Detect significant metric changes between two time periods.

    Uses the MetricEngine to query v_exec_daily, then applies
    deterministic threshold rules and computes driver breakdowns.

    Args:
        engine: MetricEngine instance (already connected to DuckDB)
    """

    def __init__(self, engine: MetricEngine):
        self.engine = engine

    def detect(
        self,
        current_range: Tuple[Union[str, date], Union[str, date]],
        previous_range: Tuple[Union[str, date], Union[str, date]],
        metrics: Optional[List[str]] = None,
        breakdown_dim: str = "channel",
    ) -> List[ChangeEvent]:
        """
        Detect significant changes between two periods.

        Args:
            current_range: (start, end) dates for the current period
            previous_range: (start, end) dates for the comparison period
            metrics: Which metrics to check (default: all that have thresholds)
            breakdown_dim: Dimension to use for driver analysis (default: "channel")

        Returns:
            List of ChangeEvent, sorted by absolute impact (largest first).
            Empty list if no significant changes found.
        """
        # Default to all metrics that have thresholds defined
        if metrics is None:
            metrics = list(CHANGE_THRESHOLDS.keys())

        # Filter to metrics that actually exist in the registry
        available = set(self.engine.list_metrics())
        metrics = [m for m in metrics if m in available]

        if not metrics:
            logger.warning("No valid metrics to check for changes")
            return []

        # ── Step 1: Get totals for both periods ─────────────────
        current_df = self.engine.query(
            metrics=metrics,
            date_range=current_range,
        )
        previous_df = self.engine.query(
            metrics=metrics,
            date_range=previous_range,
        )

        if current_df is None or previous_df is None:
            logger.warning("Could not query metrics for change detection")
            return []

        # ── Step 2: Get breakdown by dimension for both periods ──
        current_by_dim = self.engine.query(
            metrics=metrics,
            dims=[breakdown_dim],
            date_range=current_range,
        )
        previous_by_dim = self.engine.query(
            metrics=metrics,
            dims=[breakdown_dim],
            date_range=previous_range,
        )

        # ── Step 3: For each metric, check if change is significant ──
        events: List[ChangeEvent] = []

        for metric in metrics:
            curr_val = safe_float(
                current_df[metric].iloc[0] if metric in current_df.columns else 0
            )
            prev_val = safe_float(
                previous_df[metric].iloc[0] if metric in previous_df.columns else 0
            )

            abs_change = curr_val - prev_val
            pct_change = (abs_change / prev_val) if prev_val != 0 else 0

            # Apply thresholds
            thresholds = CHANGE_THRESHOLDS.get(metric, DEFAULT_THRESHOLD)
            if not self._exceeds_threshold(abs_change, pct_change, thresholds):
                continue

            # ── Step 4: Compute drivers ──────────────────────────
            drivers = self._compute_drivers(
                metric, breakdown_dim,
                current_by_dim, previous_by_dim,
                abs_change,
            )

            # ── Step 5: Build the change event ───────────────────
            direction = "up" if abs_change > 0 else "down"
            significance = self._classify_significance(abs_change, pct_change, metric)

            # Get human label from registry
            metric_info = self.engine.get_metric_info(metric)
            metric_label = metric_info.get("label", metric) if metric_info else metric
            metric_format = metric_info.get("format", "float") if metric_info else "float"

            sentence = self._build_sentence(
                metric_label, metric_format, direction,
                curr_val, prev_val, abs_change, pct_change, drivers,
            )

            events.append(ChangeEvent(
                metric=metric,
                metric_label=metric_label,
                current_value=curr_val,
                previous_value=prev_val,
                abs_change=abs_change,
                pct_change=pct_change * 100,  # Store as percentage
                direction=direction,
                significance=significance,
                top_drivers=drivers,
                sentence=sentence,
            ))

        # Sort by absolute impact (largest first)
        events.sort(key=lambda e: abs(e.abs_change), reverse=True)

        return events

    # ------------------------------------------
    # Driver Analysis
    # ------------------------------------------

    def _compute_drivers(
        self,
        metric: str,
        dim: str,
        current_by_dim: Optional[pd.DataFrame],
        previous_by_dim: Optional[pd.DataFrame],
        total_change: float,
    ) -> List[Driver]:
        """
        Compute which dimension values contributed most to the total change.

        Algorithm:
        1. Merge current and previous DataFrames on the dim column
        2. For each dim value, calculate its abs contribution
        3. Calculate what % of the total change it accounts for
        4. Return top 3 drivers sorted by absolute contribution
        """
        if current_by_dim is None or previous_by_dim is None:
            return []
        if dim not in current_by_dim.columns or metric not in current_by_dim.columns:
            return []

        # Select relevant columns
        curr = current_by_dim[[dim, metric]].copy()
        prev = previous_by_dim[[dim, metric]].copy() if dim in previous_by_dim.columns else pd.DataFrame()

        if prev.empty:
            return []

        # Merge on dim
        merged = pd.merge(
            curr, prev,
            on=dim, how="outer",
            suffixes=("_curr", "_prev"),
        )
        merged = merged.fillna(0)

        curr_col = f"{metric}_curr"
        prev_col = f"{metric}_prev"

        if curr_col not in merged.columns or prev_col not in merged.columns:
            return []

        merged["abs_change"] = merged[curr_col] - merged[prev_col]

        # Sort by absolute contribution
        merged = merged.sort_values("abs_change", key=abs, ascending=False)

        # Build drivers (top 3)
        drivers = []
        for _, row in merged.head(3).iterrows():
            dim_value = str(row[dim])
            change = float(row["abs_change"])

            # Skip negligible drivers
            if abs(change) < 1:
                continue

            pct_of_total = (change / total_change * 100) if total_change != 0 else 0

            drivers.append(Driver(
                dim_value=dim_value,
                dim_label=normalize_channel(dim_value),
                current=float(row[curr_col]),
                previous=float(row[prev_col]),
                abs_change=change,
                pct_of_total_change=round(pct_of_total, 1),
            ))

        return drivers

    # ------------------------------------------
    # Threshold Logic
    # ------------------------------------------

    def _exceeds_threshold(
        self,
        abs_change: float,
        pct_change: float,
        thresholds: Dict[str, float],
    ) -> bool:
        """
        Check if a change exceeds BOTH the volume and percentage thresholds.

        Both conditions must be met to reduce noise:
        - A $5 change that's +500% is not interesting (too small absolute)
        - A $1000 change that's +1% is not interesting (too small percentage)
        """
        min_volume = thresholds.get("min_volume", 10.0)
        min_pct = thresholds.get("min_pct", 0.10)

        return abs(abs_change) >= min_volume and abs(pct_change) >= min_pct

    def _classify_significance(
        self, abs_change: float, pct_change: float, metric: str
    ) -> str:
        """Classify a change as high/medium/low significance."""
        pct_abs = abs(pct_change)
        if pct_abs >= 0.25:
            return "high"
        elif pct_abs >= 0.15:
            return "medium"
        else:
            return "low"

    # ------------------------------------------
    # Sentence Builder
    # ------------------------------------------

    def _build_sentence(
        self,
        metric_label: str,
        metric_format: str,
        direction: str,
        current: float,
        previous: float,
        abs_change: float,
        pct_change: float,  # As fraction (0.22 = 22%)
        drivers: List[Driver],
    ) -> str:
        """
        Build a human-readable insight sentence.

        Examples:
            "Ad Spend up +22.1% ($2,200) driven by Google Ads (+$1,500)"
            "Conversions down -15.0% (150 fewer)"
        """
        # Format the change value based on metric type
        pct_str = f"{pct_change * 100:+.1f}%"

        if metric_format == "currency":
            change_str = f"${abs(abs_change):,.0f}"
            more_or_fewer = "more" if direction == "up" else "less"
        elif metric_format == "integer":
            change_str = f"{abs(abs_change):,.0f}"
            more_or_fewer = "more" if direction == "up" else "fewer"
        else:
            change_str = f"{abs(abs_change):,.1f}"
            more_or_fewer = "higher" if direction == "up" else "lower"

        sentence = f"{metric_label} {direction} {pct_str} ({change_str} {more_or_fewer})"

        # Add top driver if available
        if drivers:
            top = drivers[0]
            if metric_format == "currency":
                driver_change = f"${abs(top.abs_change):,.0f}"
                driver_sign = "+" if top.abs_change > 0 else "-"
                sentence += f" driven by {top.dim_label} ({driver_sign}{driver_change})"
            else:
                driver_change = f"{abs(top.abs_change):,.0f}"
                driver_sign = "+" if top.abs_change > 0 else "-"
                sentence += f" driven by {top.dim_label} ({driver_sign}{driver_change})"

        return sentence
