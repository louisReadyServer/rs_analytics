"""
Executive Dashboard Component for RS Analytics

This module provides a unified executive dashboard view combining metrics
from all data sources (Google Ads, Meta Ads, GA4, GSC, AppsFlyer) into
actionable KPIs.

Layout (8 rows):
- Row 0: Header (date selector, data freshness)
- Row 0.5: Mobile Acquisition Funnel (Clicks → Installs → Sign-ups → Loyal)
- Row 1: Core Health KPIs (6 tiles)
- Row 2: Target Tracking (RAG bars)
- Row 3: Channel Contribution (table)
- Row 4: Trend Reality Check (chart)
- Row 5: What Changed (narrative cards)
- Row 6: Risk Signals (alerts)
- Row 7: Data Trust (status strip)

Architecture (v3.0):
- KPI tiles and trends now use MetricEngine + v_exec_daily view
- safe_int/safe_float imported from rs_analytics.utils.formatting
- DuckDB queries go through DuckDBClient (timing + logging)
- Raw duckdb.connect() calls kept ONLY for AppsFlyer funnel
  (complex per-platform queries not yet in the metric layer)
"""

import os
import logging
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple

import streamlit as st
import pandas as pd
import duckdb
import numpy as np
import plotly.graph_objects as go
import plotly.express as px

from app.components.glossary import TERM_TOOLTIPS

# Import centralized utilities from the new rs_analytics library
from rs_analytics.utils.formatting import safe_int, safe_float, safe_divide, calculate_delta, format_delta
from rs_analytics.db.client import DuckDBClient
from rs_analytics.db.adapters import normalize_channel
from rs_analytics.metrics.engine import MetricEngine
from rs_analytics.quality.checks import DataQualityChecker, DataQualityResult
from rs_analytics.insights.change_detection import ChangeDetector, ChangeEvent

logger = logging.getLogger(__name__)


# ============================================
# Configuration
# ============================================

# Default targets (can be overridden via env vars)
DEFAULT_TARGETS = {
    'conversions': int(os.getenv('EXEC_TARGET_CONVERSIONS', 1000)),
    'cpa': float(os.getenv('EXEC_TARGET_CPA', 5.00)),
    'budget': float(os.getenv('EXEC_TARGET_BUDGET', 10000)),
}


# ============================================
# Engine Initialization Helper
# ============================================

def _get_engine(duckdb_path: str) -> MetricEngine:
    """
    Get or create a MetricEngine instance for the given database path.

    Uses Streamlit's cache_resource so the engine (and its registry)
    are loaded once per session, not on every rerun.

    Args:
        duckdb_path: Path to the DuckDB database file

    Returns:
        MetricEngine instance ready to query
    """
    @st.cache_resource
    def _create_engine(_path: str) -> MetricEngine:
        client = DuckDBClient(_path)
        engine = MetricEngine(client)
        # Ensure the v_exec_daily view exists in the database
        engine.ensure_views()
        return engine

    return _create_engine(duckdb_path)


# ============================================
# Data Loading Helpers
# ============================================

def load_data(duckdb_path: str, query: str) -> Optional[pd.DataFrame]:
    """
    Legacy data loader — kept for AppsFlyer funnel and other custom queries
    that don't yet go through the MetricEngine.

    For new code, prefer engine.query() instead.
    """
    try:
        conn = duckdb.connect(duckdb_path, read_only=True)
        df = conn.execute(query).fetchdf()
        conn.close()
        return df
    except Exception as e:
        # Don't show error to user for expected missing tables
        logger.warning("Query error: %s — %s", query[:100], e)
        return None


def get_date_range(days: int, comparison_type: str = "Previous Period") -> Tuple[str, str, str, str]:
    """
    Get current and previous period date ranges based on comparison type.

    Args:
        days: Number of days for the current period
        comparison_type: Type of comparison - "Previous Period", "WoW", or "MoM"

    Returns:
        (start_date, end_date, prev_start_date, prev_end_date)

    Comparison Types:
        - Previous Period: Compare current N days vs the N days immediately before
        - WoW (Week over Week): Compare current period vs same period one week ago
        - MoM (Month over Month): Compare current period vs same period one month ago
    """
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days)

    if comparison_type == "WoW":
        prev_end_date = end_date - timedelta(days=7)
        prev_start_date = start_date - timedelta(days=7)
    elif comparison_type == "MoM":
        prev_end_date = end_date - timedelta(days=30)
        prev_start_date = start_date - timedelta(days=30)
    else:
        prev_end_date = start_date - timedelta(days=1)
        prev_start_date = prev_end_date - timedelta(days=days)

    return (
        start_date.strftime('%Y-%m-%d'),
        end_date.strftime('%Y-%m-%d'),
        prev_start_date.strftime('%Y-%m-%d'),
        prev_end_date.strftime('%Y-%m-%d')
    )


# ============================================
# Metric Aggregation Functions (via MetricEngine)
# ============================================
# These now query v_exec_daily through the engine instead of
# the old fact_paid_daily / fact_web_daily / fact_organic_daily views.
# The old views may still exist but are no longer the source of truth
# for executive metrics.

def get_paid_metrics(duckdb_path: str, start_date: str, end_date: str) -> Dict[str, Any]:
    """
    Get aggregated paid advertising metrics from the metric layer.

    Queries v_exec_daily WHERE channel_type = 'paid' for spend, clicks,
    impressions, conversions, and revenue.
    """
    engine = _get_engine(duckdb_path)
    df = engine.query(
        metrics=["spend", "clicks", "impressions", "conversions", "revenue"],
        date_range=(start_date, end_date),
        filters={"channel_type": "paid"},
    )
    if df is not None and not df.empty:
        return df.iloc[0].to_dict()
    return {'spend': 0, 'clicks': 0, 'impressions': 0, 'conversions': 0, 'revenue': 0}


def get_web_metrics(duckdb_path: str, start_date: str, end_date: str) -> Dict[str, Any]:
    """
    Get aggregated web analytics metrics from GA4.

    GA4 lives in its own source table (ga4_sessions) rather than v_exec_daily
    because it doesn't map to the spend/clicks/conversions schema.
    """
    engine = _get_engine(duckdb_path)
    df = engine.query(
        metrics=["sessions", "users", "bounce_rate"],
        date_range=(start_date, end_date),
    )
    if df is not None and not df.empty:
        result = df.iloc[0].to_dict()
        # Ensure expected keys exist
        result.setdefault('sessions', 0)
        result.setdefault('users', 0)
        result.setdefault('bounce_rate', 0)
        return result
    return {'sessions': 0, 'users': 0, 'new_users': 0, 'bounce_rate': 0}


def get_organic_metrics(duckdb_path: str, start_date: str, end_date: str) -> Dict[str, Any]:
    """
    Get aggregated organic search metrics from GSC.

    Queries the gsc_daily_totals table through the metric engine.
    """
    engine = _get_engine(duckdb_path)
    df = engine.query(
        metrics=["organic_clicks", "avg_position"],
        date_range=(start_date, end_date),
    )
    if df is not None and not df.empty:
        result = df.iloc[0].to_dict()
        # Map to expected key names for backward compatibility
        result['clicks'] = result.pop('organic_clicks', 0)
        result['position'] = result.pop('avg_position', 0)
        result.setdefault('impressions', 0)
        result.setdefault('ctr', 0)
        return result
    return {'clicks': 0, 'impressions': 0, 'ctr': 0, 'position': 0}


def get_channel_breakdown(duckdb_path: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Get performance breakdown by channel from v_exec_daily.

    Returns all channels (paid + organic + app) grouped by channel name,
    with spend, clicks, impressions, conversions, and computed CPA.
    """
    engine = _get_engine(duckdb_path)
    df = engine.query(
        metrics=["spend", "clicks", "impressions", "conversions"],
        dims=["channel"],
        date_range=(start_date, end_date),
        # Only show channels that have spend (paid channels)
        filters={"channel_type": "paid"},
    )
    if df is not None and not df.empty:
        # Compute CPA in Python (derived from two base metrics)
        df['cpa'] = df.apply(
            lambda row: safe_divide(row['spend'], row['conversions']),
            axis=1,
        )
        df = df.sort_values('spend', ascending=False)
        return df
    return pd.DataFrame()


def get_trend_data(duckdb_path: str, days: int = 30) -> pd.DataFrame:
    """
    Get daily trend data from v_exec_daily for the specified period.

    Returns a DataFrame with columns:
    date_day, paid_spend, sessions, conversions, organic_clicks.
    """
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days)

    engine = _get_engine(duckdb_path)

    # Query paid metrics (spend + conversions) by day from v_exec_daily
    paid_df = engine.query_trend(
        metrics=["spend", "conversions"],
        date_range=(str(start_date), str(end_date)),
    )

    # Query web sessions from GA4 (different source view)
    sessions_df = engine.query(
        metrics=["sessions"],
        dims=["date_day"],
        date_range=(str(start_date), str(end_date)),
    )
    if sessions_df is not None and "date_day" in sessions_df.columns:
        # GA4 dates are YYYYMMDD strings — convert for merge
        try:
            sessions_df["date_day"] = pd.to_datetime(sessions_df["date_day"], format="%Y%m%d")
        except Exception:
            sessions_df["date_day"] = pd.to_datetime(sessions_df["date_day"])

    # Query organic clicks from GSC (different source view)
    organic_df = engine.query(
        metrics=["organic_clicks"],
        dims=["date_day"],
        date_range=(str(start_date), str(end_date)),
    )
    if organic_df is not None and "date_day" in organic_df.columns:
        organic_df["date_day"] = pd.to_datetime(organic_df["date_day"])

    # Merge everything on date_day
    if paid_df is None:
        paid_df = pd.DataFrame(columns=["date_day", "spend", "conversions"])

    result = paid_df.rename(columns={"spend": "paid_spend"})

    if sessions_df is not None and not sessions_df.empty:
        result = result.merge(sessions_df, on="date_day", how="outer")

    if organic_df is not None and not organic_df.empty:
        result = result.merge(organic_df, on="date_day", how="outer")

    # Fill NaN and ensure expected columns
    for col in ["paid_spend", "sessions", "conversions", "organic_clicks"]:
        if col not in result.columns:
            result[col] = 0
        result[col] = result[col].fillna(0)

    result = result.sort_values("date_day")
    return result


def get_data_freshness(duckdb_path: str) -> Dict[str, Dict[str, Any]]:
    """
    Get last data update timestamp for each source.

    Checks the MAX(date) for each source table to determine freshness.
    """
    sources = {
        'GA4': 'ga4_sessions',
        'GSC': 'gsc_daily_totals',
        'Google Ads': 'gads_campaigns',
        'Meta Ads': 'meta_daily_account'
    }

    freshness = {}
    for name, table in sources.items():
        try:
            query = f"SELECT MAX(date) as last_date FROM {table}"
            df = load_data(duckdb_path, query)
            if df is not None and not df.empty and df.iloc[0]['last_date']:
                last_date = df.iloc[0]['last_date']
                # Handle different date formats
                if isinstance(last_date, str):
                    if len(last_date) == 8:  # YYYYMMDD
                        last_date = datetime.strptime(last_date, '%Y%m%d').date()
                    else:  # YYYY-MM-DD
                        last_date = datetime.strptime(last_date[:10], '%Y-%m-%d').date()

                days_ago = (datetime.now().date() - last_date).days
                freshness[name] = {
                    'last_date': last_date,
                    'days_ago': days_ago,
                    'status': 'ok' if days_ago <= 2 else 'warning' if days_ago <= 5 else 'error'
                }
            else:
                freshness[name] = {'last_date': None, 'days_ago': None, 'status': 'no_data'}
        except Exception:
            freshness[name] = {'last_date': None, 'days_ago': None, 'status': 'error'}

    return freshness


def generate_insights(
    current_paid: Dict,
    prev_paid: Dict,
    current_organic: Dict,
    prev_organic: Dict,
    channel_df: pd.DataFrame
) -> List[Dict[str, str]]:
    """
    Generate auto-generated insights based on data changes.

    Uses calculate_delta from rs_analytics.utils.formatting for consistent
    percentage change calculations.
    """
    insights = []

    # Check spend change
    if current_paid.get('spend') and prev_paid.get('spend'):
        spend_delta = calculate_delta(current_paid['spend'], prev_paid['spend'])
        if spend_delta and abs(spend_delta) > 10:
            direction = "increased" if spend_delta > 0 else "decreased"
            insights.append({
                'type': 'spend',
                'icon': '💰',
                'title': f"Paid Spend {direction} {abs(spend_delta):.1f}%",
                'detail': f"From ${prev_paid['spend']:,.0f} to ${current_paid['spend']:,.0f}",
                'action': "Review budget allocation" if spend_delta > 20 else "Monitor closely"
            })

    # Check conversion change
    if current_paid.get('conversions') and prev_paid.get('conversions'):
        conv_delta = calculate_delta(current_paid['conversions'], prev_paid['conversions'])
        if conv_delta and abs(conv_delta) > 10:
            direction = "up" if conv_delta > 0 else "down"
            insights.append({
                'type': 'conversions',
                'icon': '🎯',
                'title': f"Conversions {direction} {abs(conv_delta):.1f}%",
                'detail': f"From {prev_paid['conversions']:,.0f} to {current_paid['conversions']:,.0f}",
                'action': "Scale winning campaigns" if conv_delta > 0 else "Investigate drop"
            })

    # Check organic traffic change
    if current_organic.get('clicks') and prev_organic.get('clicks'):
        organic_delta = calculate_delta(current_organic['clicks'], prev_organic['clicks'])
        if organic_delta and abs(organic_delta) > 10:
            direction = "growing" if organic_delta > 0 else "declining"
            insights.append({
                'type': 'organic',
                'icon': '🔍',
                'title': f"Organic clicks {direction} {abs(organic_delta):.1f}%",
                'detail': f"From {prev_organic['clicks']:,.0f} to {current_organic['clicks']:,.0f}",
                'action': "SEO momentum building" if organic_delta > 0 else "Check ranking changes"
            })

    # Check CPA efficiency
    if current_paid.get('spend') and current_paid.get('conversions') and current_paid['conversions'] > 0:
        current_cpa = current_paid['spend'] / current_paid['conversions']
        if prev_paid.get('spend') and prev_paid.get('conversions') and prev_paid['conversions'] > 0:
            prev_cpa = prev_paid['spend'] / prev_paid['conversions']
            cpa_delta = calculate_delta(current_cpa, prev_cpa)
            if cpa_delta and abs(cpa_delta) > 10:
                direction = "increased" if cpa_delta > 0 else "improved"
                color = "🔴" if cpa_delta > 0 else "🟢"
                insights.append({
                    'type': 'cpa',
                    'icon': color,
                    'title': f"CPA {direction} {abs(cpa_delta):.1f}%",
                    'detail': f"From ${prev_cpa:.2f} to ${current_cpa:.2f}",
                    'action': "Optimize targeting" if cpa_delta > 0 else "Increase spend on winners"
                })

    # Channel performance insight
    if not channel_df.empty and len(channel_df) > 1:
        top_channel = channel_df.iloc[0]
        if top_channel.get('cpa') and top_channel['cpa'] > 0:
            channel_label = normalize_channel(str(top_channel['channel']))
            insights.append({
                'type': 'channel',
                'icon': '📊',
                'title': f"{channel_label} leads spend",
                'detail': f"${top_channel['spend']:,.0f} spend, ${top_channel['cpa']:.2f} CPA",
                'action': "Compare efficiency across platforms"
            })

    return insights[:4]  # Return max 4 insights


def detect_risk_signals(
    current_paid: Dict,
    prev_paid: Dict,
    targets: Dict
) -> List[Dict[str, str]]:
    """Detect risk signals and anomalies."""
    signals = []
    
    # Spend growing faster than conversions
    if (current_paid.get('spend') and prev_paid.get('spend') and 
        current_paid.get('conversions') and prev_paid.get('conversions')):
        
        spend_growth = calculate_delta(current_paid['spend'], prev_paid['spend']) or 0
        conv_growth = calculate_delta(current_paid['conversions'], prev_paid['conversions']) or 0
        
        if spend_growth > 10 and conv_growth < spend_growth - 10:
            signals.append({
                'type': 'warning',
                'icon': '⚠️',
                'message': f"Spend growing faster than conversions (+{spend_growth:.0f}% vs +{conv_growth:.0f}%)"
            })
    
    # CPA above target
    if current_paid.get('spend') and current_paid.get('conversions') and current_paid['conversions'] > 0:
        current_cpa = current_paid['spend'] / current_paid['conversions']
        if current_cpa > targets['cpa'] * 1.2:  # 20% above target
            signals.append({
                'type': 'warning',
                'icon': '🔴',
                'message': f"CPA (${current_cpa:.2f}) is {((current_cpa / targets['cpa']) - 1) * 100:.0f}% above target"
            })
    
    # Budget utilization
    if current_paid.get('spend') and targets['budget'] > 0:
        utilization = current_paid['spend'] / targets['budget']
        if utilization > 0.9:
            signals.append({
                'type': 'warning',
                'icon': '💸',
                'message': f"Budget {utilization * 100:.0f}% utilized"
            })
    
    # Positive signals
    if current_paid.get('conversions') and targets['conversions'] > 0:
        progress = current_paid['conversions'] / targets['conversions']
        if progress >= 1.0:
            signals.append({
                'type': 'success',
                'icon': '✅',
                'message': f"Conversion target achieved ({progress * 100:.0f}%)"
            })
    
    return signals


# ============================================
# Mobile Acquisition Funnel
# ============================================


def _has_appsflyer_data(duckdb_path: str) -> bool:
    """Check if af_daily_geo table exists and has rows."""
    try:
        conn = duckdb.connect(duckdb_path, read_only=True)
        tables = conn.execute("SHOW TABLES").fetchdf()["name"].tolist()
        if "af_daily_geo" not in tables:
            conn.close()
            return False
        count = conn.execute("SELECT COUNT(*) FROM af_daily_geo").fetchone()[0]
        conn.close()
        return count > 0
    except Exception:
        return False


def _funnel_pct(numerator: float, denominator: float) -> str:
    """Format a conversion-rate percentage safely."""
    if denominator and denominator > 0:
        return f"{(numerator / denominator) * 100:.1f}%"
    return "–"


def _render_funnel_bar(
    label: str,
    value: float,
    max_value: float,
    color: str,
    pct_label: str = "",
) -> None:
    """
    Render a single horizontal bar for the funnel using styled HTML.
    
    Includes tooltip from glossary when hovering over the label.

    Args:
        label: Stage name (e.g. "Clicks") - should match a TERM_TOOLTIPS key
        value: Numeric value for this stage
        max_value: Maximum value across all stages (for width scaling)
        color: CSS colour string
        pct_label: Conversion-rate label shown to the right
    """
    width_pct = (value / max_value * 100) if max_value > 0 else 0
    width_pct = max(width_pct, 3)  # Minimum 3% so tiny bars are visible
    
    # Get tooltip from glossary
    tooltip = TERM_TOOLTIPS.get(label, "")
    tooltip_attr = f'title="{tooltip}"' if tooltip else ""
    help_icon = ' <span style="color:#666;font-size:9px;">ⓘ</span>' if tooltip else ""

    st.markdown(
        f"""
        <div style="margin-bottom:6px;">
          <div style="display:flex;align-items:center;gap:8px;">
            <div style="min-width:90px;font-size:12px;color:#bbb;text-align:right;cursor:help;" {tooltip_attr}>
                {label}{help_icon}
            </div>
            <div style="flex:1;background:#1a1a2e;border-radius:6px;height:28px;position:relative;">
              <div style="width:{width_pct:.1f}%;background:{color};height:100%;border-radius:6px;
                          display:flex;align-items:center;padding-left:10px;">
                <span style="color:#fff;font-weight:700;font-size:13px;white-space:nowrap;">
                  {value:,.0f}
                </span>
              </div>
            </div>
            <div style="min-width:60px;font-size:12px;color:#888;">{pct_label}</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_mobile_funnel(duckdb_path: str, start_str: str, end_str: str) -> None:
    """
    Render the full mobile acquisition funnel at the top of the executive dashboard.

    Shows:
    1. Overall funnel (Clicks → Installs → Sign-ups → Loyal Users) with conversion %
    2. iOS vs Android side-by-side funnel comparison
    3. Source breakdown table with install share %
    4. Daily funnel trend over time
    5. Top countries by installs per platform

    Args:
        duckdb_path: Path to DuckDB database
        start_str: Start date YYYY-MM-DD
        end_str: End date YYYY-MM-DD
    """
    date_filter = f"date >= '{start_str}' AND date <= '{end_str}'"

    # ── Query: Totals by platform ────────────────────────────
    totals_query = f"""
        SELECT
            platform,
            COALESCE(SUM(clicks), 0)                     AS clicks,
            COALESCE(SUM(installs), 0)                    AS installs,
            COALESCE(SUM(user_sign_up_unique_users), 0)   AS signups,
            COALESCE(SUM(loyal_users), 0)                 AS loyal_users,
            COALESCE(SUM(sessions), 0)                    AS sessions,
            COALESCE(SUM(deposit_unique_users), 0)        AS deposits
        FROM af_daily_geo
        WHERE {date_filter}
        GROUP BY platform
    """
    totals_df = load_data(duckdb_path, totals_query)
    if totals_df is None or totals_df.empty:
        return

    # Extract per-platform rows
    ios = totals_df[totals_df["platform"] == "ios"]
    android = totals_df[totals_df["platform"] == "android"]
    ios_r = ios.iloc[0] if not ios.empty else None
    and_r = android.iloc[0] if not android.empty else None

    # Grand totals
    total_clicks = safe_float(totals_df["clicks"].sum())
    total_installs = safe_float(totals_df["installs"].sum())
    total_signups = safe_float(totals_df["signups"].sum())
    total_loyal = safe_float(totals_df["loyal_users"].sum())
    total_deposits = safe_float(totals_df["deposits"].sum())

    # ── Section header ───────────────────────────────────────
    st.subheader("📱 Mobile Acquisition Funnel")

    # ─────────────────────────────────────────────────────────
    # ROW A: Overall funnel bars
    # ─────────────────────────────────────────────────────────
    max_val = max(total_clicks, total_installs, 1)

    funnel_stages = [
        ("Clicks",      total_clicks,   "#4fc3f7", ""),
        ("Installs",    total_installs, "#29b6f6", _funnel_pct(total_installs, total_clicks)),
        ("Sign-ups",    total_signups,  "#0288d1", _funnel_pct(total_signups, total_installs)),
        ("Loyal Users", total_loyal,    "#01579b", _funnel_pct(total_loyal, total_installs)),
    ]

    for label, val, color, pct in funnel_stages:
        _render_funnel_bar(label, val, max_val, color, pct)

    # Small gap
    st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

    # ─────────────────────────────────────────────────────────
    # ROW B: iOS vs Android side-by-side funnel
    # ─────────────────────────────────────────────────────────
    col_ios, col_and = st.columns(2)

    for col, row, plat_label, color_set in [
        (col_ios, ios_r, "iOS", ["#4fc3f7", "#29b6f6", "#0288d1", "#01579b"]),
        (col_and, and_r, "Android", ["#81c784", "#66bb6a", "#43a047", "#2e7d32"]),
    ]:
        with col:
            st.markdown(f"**{plat_label}**")
            if row is not None:
                p_clicks = safe_float(row.get("clicks"))
                p_installs = safe_float(row.get("installs"))
                p_signups = safe_float(row.get("signups"))
                p_loyal = safe_float(row.get("loyal_users"))

                p_max = max(p_clicks, p_installs, 1)
                stages = [
                    ("Clicks",      p_clicks,   color_set[0], ""),
                    ("Installs",    p_installs, color_set[1], _funnel_pct(p_installs, p_clicks)),
                    ("Sign-ups",    p_signups,  color_set[2], _funnel_pct(p_signups, p_installs)),
                    ("Loyal Users", p_loyal,    color_set[3], _funnel_pct(p_loyal, p_installs)),
                ]
                for lbl, v, c, p in stages:
                    _render_funnel_bar(lbl, v, p_max, c, p)

                # Platform share of total installs
                share = (p_installs / total_installs * 100) if total_installs > 0 else 0
                st.caption(f"{share:.1f}% of total installs")
            else:
                st.info("No data for this platform")

    st.markdown("")  # spacing

    # ─────────────────────────────────────────────────────────
    # ROW C: Daily funnel trend + Top countries
    # ─────────────────────────────────────────────────────────
    col_trend, col_geo = st.columns([3, 2])

    with col_trend:
        st.markdown("##### Daily Installs by Platform")

        daily_query = f"""
            SELECT
                date,
                platform,
                COALESCE(SUM(installs), 0) AS installs,
                COALESCE(SUM(user_sign_up_unique_users), 0) AS signups
            FROM af_daily_geo
            WHERE {date_filter}
            GROUP BY date, platform
            ORDER BY date
        """
        daily_df = load_data(duckdb_path, daily_query)

        if daily_df is not None and not daily_df.empty:
            daily_df["date"] = pd.to_datetime(daily_df["date"])

            fig = px.area(
                daily_df,
                x="date",
                y="installs",
                color="platform",
                color_discrete_map={"ios": "#4fc3f7", "android": "#81c784"},
                labels={"date": "", "installs": "Installs", "platform": "OS"},
            )
            fig.update_layout(
                template="plotly_dark",
                height=300,
                margin=dict(l=0, r=0, t=10, b=0),
                legend=dict(
                    orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
                ),
                xaxis_title="",
                yaxis_title="",
            )
            st.plotly_chart(fig, use_container_width=True)

    with col_geo:
        st.markdown("##### Top Countries")

        geo_query = f"""
            SELECT
                country,
                SUM(CASE WHEN platform='ios' THEN installs ELSE 0 END) AS ios,
                SUM(CASE WHEN platform='android' THEN installs ELSE 0 END) AS android,
                SUM(installs) AS total
            FROM af_daily_geo
            WHERE {date_filter}
            GROUP BY country
            HAVING SUM(installs) > 0
            ORDER BY total DESC
            LIMIT 10
        """
        geo_df = load_data(duckdb_path, geo_query)

        if geo_df is not None and not geo_df.empty:
            fig_geo = go.Figure()
            fig_geo.add_trace(go.Bar(
                y=geo_df["country"],
                x=geo_df["ios"],
                name="iOS",
                orientation="h",
                marker_color="#4fc3f7",
            ))
            fig_geo.add_trace(go.Bar(
                y=geo_df["country"],
                x=geo_df["android"],
                name="Android",
                orientation="h",
                marker_color="#81c784",
            ))
            fig_geo.update_layout(
                barmode="stack",
                template="plotly_dark",
                height=300,
                margin=dict(l=0, r=0, t=10, b=0),
                legend=dict(
                    orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
                ),
                yaxis=dict(autorange="reversed"),
                xaxis_title="",
                yaxis_title="",
            )
            st.plotly_chart(fig_geo, use_container_width=True)


# ============================================
# Render Functions
# ============================================


def render_data_freshness(duckdb_path: str):
    """Render data freshness indicators as status badges."""
    freshness = get_data_freshness(duckdb_path)
    
    cols = st.columns(len(freshness))
    for i, (source, info) in enumerate(freshness.items()):
        with cols[i]:
            if info['status'] == 'ok':
                icon = "✅"
                text = f"{info['days_ago']}d ago" if info['days_ago'] else "today"
                st.success(f"**{source}**: {text}", icon=icon)
            elif info['status'] == 'warning':
                icon = "⚠️"
                text = f"{info['days_ago']}d lag"
                st.warning(f"**{source}**: {text}", icon=icon)
            elif info['status'] == 'no_data':
                icon = "❌"
                text = "no data"
                st.error(f"**{source}**: {text}", icon=icon)
            else:
                icon = "❓"
                text = "error"
                st.error(f"**{source}**: {text}", icon=icon)


def render_kpi_tiles(duckdb_path: str, start_date: str, end_date: str, 
                     prev_start: str, prev_end: str):
    """Render Row 1 - Core Health KPIs (6 tiles)."""
    
    # Get current and previous period metrics
    current_paid = get_paid_metrics(duckdb_path, start_date, end_date)
    prev_paid = get_paid_metrics(duckdb_path, prev_start, prev_end)
    
    current_web = get_web_metrics(duckdb_path, start_date, end_date)
    prev_web = get_web_metrics(duckdb_path, prev_start, prev_end)
    
    current_organic = get_organic_metrics(duckdb_path, start_date, end_date)
    prev_organic = get_organic_metrics(duckdb_path, prev_start, prev_end)
    
    # Calculate derived metrics
    current_cpa = (current_paid['spend'] / current_paid['conversions'] 
                   if current_paid['conversions'] and current_paid['conversions'] > 0 else None)
    prev_cpa = (prev_paid['spend'] / prev_paid['conversions']
                if prev_paid['conversions'] and prev_paid['conversions'] > 0 else None)
    
    current_roas = (current_paid['revenue'] / current_paid['spend'] 
                    if current_paid['spend'] and current_paid['spend'] > 0 else None)
    
    # Create 6 columns for KPI tiles
    cols = st.columns(6)
    
    # Tile 1: Paid Spend
    with cols[0]:
        st.metric(
            label="💰 Paid Spend",
            value=f"${current_paid['spend']:,.0f}" if current_paid['spend'] else "$0",
            delta=format_delta(current_paid['spend'] or 0, prev_paid['spend'] or 0),
            delta_color="inverse",  # Lower spend can be good
            help=TERM_TOOLTIPS.get("Spend"),
        )
    
    # Tile 2: Total Sessions
    with cols[1]:
        st.metric(
            label="👁️ Sessions",
            value=f"{safe_int(current_web['sessions']):,}",
            delta=format_delta(safe_float(current_web['sessions']), safe_float(prev_web['sessions'])),
            help=TERM_TOOLTIPS.get("Sessions"),
        )
    
    # Tile 3: Conversions (North Star)
    with cols[2]:
        st.metric(
            label="🎯 Conversions",
            value=f"{safe_int(current_paid['conversions']):,}",
            delta=format_delta(safe_float(current_paid['conversions']), safe_float(prev_paid['conversions'])),
            help=TERM_TOOLTIPS.get("Conversions"),
        )
    
    # Tile 4: Blended CPA
    with cols[3]:
        st.metric(
            label="📉 CPA",
            value=f"${current_cpa:.2f}" if current_cpa else "-",
            delta=format_delta(current_cpa or 0, prev_cpa or 0) if current_cpa and prev_cpa else None,
            delta_color="inverse",  # Lower CPA is better
            help=TERM_TOOLTIPS.get("CPA"),
        )
    
    # Tile 5: Organic Clicks
    with cols[4]:
        st.metric(
            label="🔍 Organic Clicks",
            value=f"{safe_int(current_organic['clicks']):,}",
            delta=format_delta(safe_float(current_organic['clicks']), safe_float(prev_organic['clicks'])),
            help=TERM_TOOLTIPS.get("Clicks"),
        )
    
    # Tile 6: Revenue/ROAS
    with cols[5]:
        revenue_val = safe_float(current_paid['revenue'])
        if revenue_val > 0:
            st.metric(
                label="💵 Revenue",
                value=f"${revenue_val:,.0f}",
                delta=f"{current_roas:.1f}x ROAS" if current_roas else None,
                help=TERM_TOOLTIPS.get("Revenue"),
            )
        else:
            # Show clicks as fallback
            st.metric(
                label="🖱️ Paid Clicks",
                value=f"{safe_int(current_paid['clicks']):,}",
                delta=format_delta(safe_float(current_paid['clicks']), safe_float(prev_paid['clicks'])),
                help=TERM_TOOLTIPS.get("Clicks"),
            )
    
    return current_paid, prev_paid, current_organic, prev_organic


def render_target_tracking(current_paid: Dict, targets: Dict):
    """Render Row 2 - Target tracking with RAG bars."""
    
    st.subheader("Target Progress")
    
    cols = st.columns(3)
    
    # Conversions vs Target
    with cols[0]:
        conv_progress = (current_paid['conversions'] / targets['conversions'] 
                        if targets['conversions'] > 0 else 0)
        conv_progress = min(conv_progress, 1.5)  # Cap at 150%
        
        if conv_progress >= 1.0:
            color = "🟢"
        elif conv_progress >= 0.7:
            color = "🟡"
        else:
            color = "🔴"
        
        st.markdown(f"**{color} Conversions**")
        st.progress(min(conv_progress, 1.0))
        st.caption(f"{int(current_paid['conversions'] or 0):,} / {targets['conversions']:,} ({conv_progress * 100:.0f}%)")
    
    # CPA vs Target (inverted - lower is better)
    with cols[1]:
        current_cpa = (current_paid['spend'] / current_paid['conversions']
                      if current_paid['conversions'] and current_paid['conversions'] > 0 else 0)
        
        if current_cpa <= targets['cpa']:
            color = "🟢"
            cpa_progress = 1.0
        elif current_cpa <= targets['cpa'] * 1.2:
            color = "🟡"
            cpa_progress = 0.7
        else:
            color = "🔴"
            cpa_progress = 0.3
        
        st.markdown(f"**{color} CPA Target**")
        st.progress(cpa_progress)
        st.caption(f"${current_cpa:.2f} / ${targets['cpa']:.2f} target")
    
    # Spend vs Budget
    with cols[2]:
        spend_progress = (current_paid['spend'] / targets['budget']
                         if targets['budget'] > 0 else 0)
        
        if spend_progress <= 0.9:
            color = "🟢"
        elif spend_progress <= 1.0:
            color = "🟡"
        else:
            color = "🔴"
        
        st.markdown(f"**{color} Budget**")
        st.progress(min(spend_progress, 1.0))
        st.caption(f"${current_paid['spend']:,.0f} / ${targets['budget']:,.0f} ({spend_progress * 100:.0f}%)")


def render_channel_table(duckdb_path: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Render Row 3 - Channel contribution table."""
    
    st.subheader("Channel Performance")
    
    channel_df = get_channel_breakdown(duckdb_path, start_date, end_date)
    
    if channel_df.empty:
        st.info("No channel data available for this period.")
        return channel_df
    
    # Add organic as a row
    organic = get_organic_metrics(duckdb_path, start_date, end_date)
    if organic['clicks'] and organic['clicks'] > 0:
        organic_row = pd.DataFrame([{
            'channel': 'Organic Search',
            'spend': 0,
            'clicks': organic['clicks'],
            'impressions': organic['impressions'],
            'conversions': 0,  # GSC doesn't track conversions
            'cpa': None
        }])
        channel_df = pd.concat([channel_df, organic_row], ignore_index=True)
    
    # Add web sessions from GA4
    web = get_web_metrics(duckdb_path, start_date, end_date)
    
    # Format for display
    display_df = channel_df.copy()
    display_df['channel'] = display_df['channel'].apply(
        lambda x: x.replace('_', ' ').title() if isinstance(x, str) else x
    )
    display_df['spend'] = display_df['spend'].apply(
        lambda x: f"${x:,.0f}" if x and x > 0 else "-"
    )
    display_df['clicks'] = display_df['clicks'].apply(
        lambda x: f"{int(x):,}" if x else "-"
    )
    display_df['impressions'] = display_df['impressions'].apply(
        lambda x: f"{int(x):,}" if x else "-"
    )
    display_df['conversions'] = display_df['conversions'].apply(
        lambda x: f"{int(x):,}" if x and x > 0 else "-"
    )
    display_df['cpa'] = display_df['cpa'].apply(
        lambda x: f"${x:.2f}" if x and x > 0 else "-"
    )
    
    st.dataframe(
        display_df,
        width="stretch",
        hide_index=True,
        column_config={
            "channel": "Channel",
            "spend": "Spend",
            "clicks": "Clicks",
            "impressions": "Impressions",
            "conversions": "Conversions",
            "cpa": "CPA"
        }
    )
    
    return channel_df


def render_trend_chart(duckdb_path: str, days: int = 30):
    """Render Row 4 - Trend reality check chart."""
    
    st.subheader("Performance Trends")
    
    trend_df = get_trend_data(duckdb_path, days)
    
    if trend_df.empty:
        st.info("No trend data available.")
        return
    
    # Convert date_day to datetime for proper plotting
    trend_df['date_day'] = pd.to_datetime(trend_df['date_day'])
    trend_df = trend_df.set_index('date_day')
    
    # Create tabs for different views
    tab1, tab2, tab3 = st.tabs(["Spend & Conversions", "Sessions", "All Metrics"])
    
    with tab1:
        chart_df = trend_df[['paid_spend', 'conversions']].copy()
        chart_df.columns = ['Paid Spend ($)', 'Conversions']
        st.line_chart(chart_df)
    
    with tab2:
        chart_df = trend_df[['sessions', 'organic_clicks']].copy()
        chart_df.columns = ['Sessions (GA4)', 'Organic Clicks (GSC)']
        st.line_chart(chart_df)
    
    with tab3:
        # Normalize data for comparison
        normalized_df = trend_df.copy()
        for col in normalized_df.columns:
            max_val = normalized_df[col].max()
            if max_val > 0:
                normalized_df[col] = normalized_df[col] / max_val * 100
        
        normalized_df.columns = ['Paid Spend', 'Sessions', 'Conversions', 'Organic Clicks']
        st.line_chart(normalized_df)
        st.caption("Note: Metrics normalized to 0-100 scale for comparison")


def render_insights(insights: List[Dict[str, str]]):
    """Render Row 5 - What Changed narrative cards."""
    
    st.subheader("What Changed")
    
    if not insights:
        st.info("No significant changes detected in this period.")
        return
    
    cols = st.columns(min(len(insights), 4))
    
    for i, insight in enumerate(insights):
        with cols[i % 4]:
            st.markdown(f"""
            <div style="padding: 1rem; border-radius: 0.5rem; background-color: rgba(100, 100, 100, 0.1); margin-bottom: 0.5rem;">
                <div style="font-size: 1.5rem;">{insight['icon']}</div>
                <div style="font-weight: bold; margin: 0.5rem 0;">{insight['title']}</div>
                <div style="font-size: 0.9rem; color: gray;">{insight['detail']}</div>
                <div style="font-size: 0.8rem; margin-top: 0.5rem; font-style: italic;">→ {insight['action']}</div>
            </div>
            """, unsafe_allow_html=True)


def render_risk_signals(signals: List[Dict[str, str]]):
    """Render Row 6 - Risk signals and alerts."""
    
    if not signals:
        return
    
    st.subheader("Signals & Alerts")
    
    cols = st.columns(min(len(signals), 4))
    
    for i, signal in enumerate(signals):
        with cols[i % 4]:
            if signal['type'] == 'warning':
                st.warning(f"{signal['icon']} {signal['message']}")
            elif signal['type'] == 'success':
                st.success(f"{signal['icon']} {signal['message']}")
            else:
                st.info(f"{signal['icon']} {signal['message']}")


def render_data_trust_footer(duckdb_path: str):
    """Render Row 7 - Data trust status strip."""
    
    with st.expander("Data Trust & Operations", expanded=False):
        freshness = get_data_freshness(duckdb_path)
        
        st.markdown("**Last Data Update by Source:**")
        
        cols = st.columns(len(freshness))
        for i, (source, info) in enumerate(freshness.items()):
            with cols[i]:
                if info['last_date']:
                    st.markdown(f"**{source}**")
                    st.caption(f"Last: {info['last_date']}")
                    if info['days_ago'] and info['days_ago'] > 2:
                        st.caption(f"⚠️ {info['days_ago']} days lag")
                else:
                    st.markdown(f"**{source}**")
                    st.caption("No data")
        
        st.markdown("---")
        st.caption("""
        **Data Lag Notes:**
        - GA4: 24-48 hour processing delay
        - GSC: 2-3 day data delay  
        - Google Ads: Same-day data
        - Meta Ads: Same-day data
        """)


# ============================================
# Data Trust Banner (Phase 2)
# ============================================

def render_trust_banner(duckdb_path: str) -> DataQualityResult:
    """
    Run data quality checks and display a trust banner at the top
    of the Executive Dashboard.

    Shows:
    - PASS (green): All sources within expected freshness windows
    - WARN (yellow): Some sources stale or minor issues
    - FAIL (red): Critical source stale or PK duplicates detected

    Returns:
        DataQualityResult so downstream code can react (e.g. disable comparisons)
    """
    client = DuckDBClient(duckdb_path)
    checker = DataQualityChecker(client)
    result = checker.run_all()

    if result.status == "FAIL":
        st.error(
            f"**Data Trust: FAIL** — {result.summary}  \n"
            "Data integrity issue detected. Check ETL Control for details.",
            icon="🔴",
        )
    elif result.status == "WARN":
        st.warning(
            f"**Data Trust: WARN** — {result.summary}  \n"
            "Run ETL to refresh stale sources.",
            icon="🟡",
        )
    else:
        st.success(
            "**Data Trust: PASS** — All sources fresh. No issues detected.",
            icon="🟢",
        )

    # Show freshness details in a collapsible row
    with st.expander("Freshness details", expanded=False):
        if result.freshness:
            cols = st.columns(len(result.freshness))
            for i, f in enumerate(result.freshness):
                with cols[i]:
                    if f.status == "pass":
                        icon = "✅"
                    elif f.status == "warn":
                        icon = "⚠️"
                    elif f.status == "fail":
                        icon = "🔴"
                    else:
                        icon = "❌"

                    days_ago = round(f.hours_since / 24, 1) if f.hours_since else None
                    date_str = str(f.last_date) if f.last_date else "no data"
                    lag_str = f"{days_ago}d ago" if days_ago is not None else "—"

                    st.markdown(f"**{icon} {f.label}**")
                    st.caption(f"Last: {date_str} ({lag_str})")

    return result


# ============================================
# Deterministic Change Detection (Phase 2)
# ============================================

def render_change_events(
    duckdb_path: str,
    current_range: Tuple[str, str],
    previous_range: Tuple[str, str],
) -> List[ChangeEvent]:
    """
    Run deterministic change detection and render insight cards.

    Replaces the old generate_insights() + render_insights() approach
    with structured, threshold-based change detection that shows drivers.

    Args:
        duckdb_path: Path to DuckDB database
        current_range: (start_date, end_date) strings
        previous_range: (prev_start, prev_end) strings

    Returns:
        List of detected ChangeEvent objects
    """
    st.subheader("What Changed")

    engine = _get_engine(duckdb_path)
    detector = ChangeDetector(engine)

    events = detector.detect(
        current_range=current_range,
        previous_range=previous_range,
    )

    if not events:
        st.info("No significant changes detected between these periods.")
        return []

    # Render insight cards (up to 4)
    display_events = events[:4]
    cols = st.columns(min(len(display_events), 4))

    for i, event in enumerate(display_events):
        with cols[i % 4]:
            # Pick icon based on metric and direction
            icon = _change_icon(event.metric, event.direction)
            sig_color = {
                "high": "#ff4444" if event.direction == "down" else "#44bb44",
                "medium": "#ff8800" if event.direction == "down" else "#4488ff",
                "low": "#888888",
            }.get(event.significance, "#888888")

            st.markdown(
                f"""
                <div style="padding: 1rem; border-radius: 0.5rem;
                            background-color: rgba(100, 100, 100, 0.1);
                            border-left: 4px solid {sig_color};
                            margin-bottom: 0.5rem;">
                    <div style="font-size: 1.5rem;">{icon}</div>
                    <div style="font-weight: bold; margin: 0.3rem 0;">
                        {event.metric_label} {event.direction} {event.pct_change:+.1f}%
                    </div>
                    <div style="font-size: 0.85rem; color: #ccc;">
                        {event.sentence}
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    # Driver breakdown table (collapsible)
    if any(e.top_drivers for e in events):
        with st.expander("Driver breakdown", expanded=False):
            rows = []
            for event in events:
                for driver in event.top_drivers:
                    rows.append({
                        "Metric": event.metric_label,
                        "Change": f"{event.pct_change:+.1f}%",
                        "Driver": driver.dim_label,
                        "Contribution": f"{driver.abs_change:+,.0f}",
                        "% of Change": f"{driver.pct_of_total_change:.0f}%",
                    })
            if rows:
                st.dataframe(
                    pd.DataFrame(rows),
                    hide_index=True,
                    use_container_width=True,
                )

    return events


def _change_icon(metric: str, direction: str) -> str:
    """Pick an appropriate icon for a change event card."""
    icons = {
        "spend": "💰",
        "revenue": "💵",
        "conversions": "🎯",
        "clicks": "🖱️",
        "installs": "📱",
        "signups": "📝",
    }
    base = icons.get(metric, "📊")
    # Add directional indicator
    if direction == "up":
        return f"{base} ↑"
    else:
        return f"{base} ↓"


# ============================================
# Main Render Function
# ============================================

def render_executive_dashboard(duckdb_path: str):
    """
    Render the complete Executive Dashboard.

    Layout:
    - Trust Banner (PASS/WARN/FAIL from data quality checks)
    - Date Range Picker
    - Mobile Acquisition Funnel (if AppsFlyer data exists)
    - Core KPI Tiles (6 metrics)
    - Target Tracking (RAG bars)
    - Channel Performance Table
    - Trend Charts
    - What Changed (deterministic change detection with drivers)
    - Risk Signals
    - Data Trust Footer

    Args:
        duckdb_path: Path to the DuckDB database
    """
    st.title("Executive Dashboard")
    st.markdown("*Unified view of marketing performance across all platforms*")

    # ── Trust Banner (Phase 2) ─────────────────────────────────
    # Runs freshness + PK + null checks, shows PASS/WARN/FAIL
    dq_result = render_trust_banner(duckdb_path)

    st.divider()

    # ── Date Range Selection ───────────────────────────────────
    from app.components.date_picker import render_date_range_picker

    start_date, end_date, prev_start_date, prev_end_date = render_date_range_picker(
        key="executive_dashboard",
        default_days=30,
        max_days=365,
        show_comparison=True,
    )

    # Convert dates to strings for SQL queries
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')
    prev_start = prev_start_date.strftime('%Y-%m-%d') if prev_start_date else None
    prev_end = prev_end_date.strftime('%Y-%m-%d') if prev_end_date else None

    st.divider()

    # ── Mobile Acquisition Funnel ──────────────────────────────
    if _has_appsflyer_data(duckdb_path):
        render_mobile_funnel(duckdb_path, start_str, end_str)
        st.divider()

    # ── Core KPIs ──────────────────────────────────────────────
    current_paid, prev_paid, current_organic, prev_organic = render_kpi_tiles(
        duckdb_path, start_date, end_date, prev_start, prev_end
    )

    st.divider()

    # ── Target Tracking ────────────────────────────────────────
    render_target_tracking(current_paid, DEFAULT_TARGETS)

    st.divider()

    # ── Channel Performance ────────────────────────────────────
    channel_df = render_channel_table(duckdb_path, start_date, end_date)

    st.divider()

    # ── Trend Chart ────────────────────────────────────────────
    days = (end_date - start_date).days + 1
    render_trend_chart(duckdb_path, days=days)

    st.divider()

    # ── What Changed (Phase 2: deterministic change detection) ─
    # Uses threshold-based rules + driver decomposition instead
    # of the old heuristic-based generate_insights() approach.
    if prev_start and prev_end:
        render_change_events(
            duckdb_path,
            current_range=(start_str, end_str),
            previous_range=(prev_start, prev_end),
        )
    else:
        # Fallback: old-style insights if no comparison period
        insights = generate_insights(
            current_paid, prev_paid,
            current_organic, prev_organic,
            channel_df
        )
        render_insights(insights)

    # ── Risk Signals ───────────────────────────────────────────
    signals = detect_risk_signals(current_paid, prev_paid, DEFAULT_TARGETS)
    render_risk_signals(signals)

    st.divider()

    # ── Data Trust Footer ──────────────────────────────────────
    render_data_trust_footer(duckdb_path)
