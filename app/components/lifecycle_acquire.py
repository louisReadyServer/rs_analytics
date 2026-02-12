"""
Acquire Mega-Page — Marketing Spend → Clicks → Installs → Signups.

Consolidates all acquisition metrics across channels (Google Ads, Meta,
GSC organic, AppsFlyer) into a single view organized around the question:
"How efficiently are we acquiring new users?"

Layout:
1. Acquisition KPI tiles (total spend, clicks, installs, signups, CPA, ROAS)
2. Channel efficiency table (spend, clicks, installs, CPA by channel)
3. Acquisition funnel chart (Impressions → Clicks → Installs → Signups)
4. Trend chart: spend vs signups over time
5. Channel mix evolution over time

Data source: v_exec_daily (canonical view from MetricEngine)
"""

import logging
from datetime import date, datetime
from typing import Optional, Tuple

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from rs_analytics.db.client import DuckDBClient
from rs_analytics.db.adapters import normalize_channel
from rs_analytics.metrics.engine import MetricEngine
from rs_analytics.utils.formatting import safe_float, safe_divide

logger = logging.getLogger(__name__)


# ============================================
# Helpers
# ============================================

def _format_delta(current: float, previous: float) -> Optional[str]:
    """
    Format a period-over-period delta as a string.
    Caps extreme values at +/- 999% to avoid noisy display.
    Returns None if previous is 0 or None.
    """
    if not previous or previous == 0:
        return None
    pct = (current - previous) / previous * 100
    if abs(pct) > 999:
        return f"{'+' if pct > 0 else '-'}999%+"
    return f"{pct:+.1f}%"


def _get_engine(duckdb_path: str) -> MetricEngine:
    """Get or create a cached MetricEngine."""
    @st.cache_resource
    def _create(path: str) -> MetricEngine:
        client = DuckDBClient(path)
        engine = MetricEngine(client)
        engine.ensure_views()
        return engine
    return _create(duckdb_path)


# ============================================
# Main Render Function
# ============================================

def render_acquire_page(duckdb_path: str):
    """
    Render the Acquire mega-page.

    Shows the full picture of user acquisition: how much we spend,
    what we get, and which channels are most efficient.
    """
    st.title("🚀 Acquire")
    st.markdown("*Marketing spend → clicks → installs → signups across all channels*")

    # Date range
    from app.components.date_picker import render_date_range_picker
    start_date, end_date, prev_start, prev_end = render_date_range_picker(
        key="acquire_page",
        default_days=30,
        max_days=365,
        show_comparison=True,
    )

    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    prev_start_str = prev_start.strftime("%Y-%m-%d") if prev_start else None
    prev_end_str = prev_end.strftime("%Y-%m-%d") if prev_end else None

    engine = _get_engine(duckdb_path)

    st.divider()

    # ── Row 1: Acquisition KPI Tiles ───────────────────────
    st.subheader("Acquisition KPIs")

    metrics_to_query = ["spend", "clicks", "impressions", "installs", "signups", "conversions", "revenue"]
    current_df = engine.query(
        metrics=metrics_to_query,
        date_range=(start_str, end_str),
    )

    prev_df = None
    if prev_start_str and prev_end_str:
        prev_df = engine.query(
            metrics=metrics_to_query,
            date_range=(prev_start_str, prev_end_str),
        )

    if current_df is not None and not current_df.empty:
        row = current_df.iloc[0]
        spend = safe_float(row.get("spend", 0))
        clicks = safe_float(row.get("clicks", 0))
        impressions = safe_float(row.get("impressions", 0))
        installs = safe_float(row.get("installs", 0))
        signups = safe_float(row.get("signups", 0))
        conversions = safe_float(row.get("conversions", 0))
        revenue = safe_float(row.get("revenue", 0))

        cpa_install = safe_divide(spend, installs)
        cpa_signup = safe_divide(spend, signups)
        roas = safe_divide(revenue, spend)

        # Previous period for deltas
        prev_spend = prev_clicks = prev_installs = prev_signups = None
        if prev_df is not None and not prev_df.empty:
            prow = prev_df.iloc[0]
            prev_spend = safe_float(prow.get("spend", 0))
            prev_clicks = safe_float(prow.get("clicks", 0))
            prev_installs = safe_float(prow.get("installs", 0))
            prev_signups = safe_float(prow.get("signups", 0))

        # KPI tiles (deltas capped at +/- 999% for readability)
        col1, col2, col3, col4, col5, col6 = st.columns(6)

        with col1:
            st.metric("Total Spend", f"${spend:,.0f}", delta=_format_delta(spend, prev_spend))
        with col2:
            st.metric("Clicks", f"{clicks:,.0f}", delta=_format_delta(clicks, prev_clicks))
        with col3:
            st.metric("Installs", f"{installs:,.0f}", delta=_format_delta(installs, prev_installs))
        with col4:
            st.metric("Signups", f"{signups:,.0f}", delta=_format_delta(signups, prev_signups))
        with col5:
            st.metric("CPA (Install)", f"${cpa_install:,.2f}" if cpa_install else "—")
        with col6:
            st.metric("ROAS", f"{roas:.2f}x" if roas else "—")
    else:
        st.warning("No acquisition data available for this period.")
        return

    st.divider()

    # ── Row 2: Channel Efficiency Table ────────────────────
    st.subheader("Channel Efficiency")

    channel_df = engine.query(
        metrics=["spend", "clicks", "impressions", "installs", "signups", "revenue"],
        dims=["channel"],
        date_range=(start_str, end_str),
    )

    if channel_df is not None and not channel_df.empty:
        # Normalize raw channel names to human-readable labels
        channel_df["channel"] = channel_df["channel"].apply(normalize_channel)

        # Round raw metric columns to integers for clean display
        for col in ["spend", "clicks", "impressions", "installs", "signups", "revenue"]:
            if col in channel_df.columns:
                channel_df[col] = channel_df[col].round(0).astype(int)

        # Calculate derived metrics per channel
        channel_df["cpa_install"] = channel_df.apply(
            lambda r: round(safe_divide(r["spend"], r["installs"]), 2), axis=1
        )
        channel_df["cpa_signup"] = channel_df.apply(
            lambda r: round(safe_divide(r["spend"], r["signups"]), 2), axis=1
        )
        channel_df["ctr"] = channel_df.apply(
            lambda r: round(safe_divide(r["clicks"], r["impressions"]) * 100, 2) if r["impressions"] > 0 else 0, axis=1
        )

        # Sort by spend
        channel_df = channel_df.sort_values("spend", ascending=False)

        # Display
        display_cols = {
            "channel": "Channel",
            "spend": "Spend",
            "clicks": "Clicks",
            "impressions": "Impressions",
            "installs": "Installs",
            "signups": "Signups",
            "cpa_install": "CPA (Install)",
            "ctr": "CTR %",
        }
        display_df = channel_df[[c for c in display_cols if c in channel_df.columns]].rename(columns=display_cols)

        st.dataframe(
            display_df,
            hide_index=True,
            use_container_width=True,
            column_config={
                "Spend": st.column_config.NumberColumn(format="$%d"),
                "Clicks": st.column_config.NumberColumn(format="%d"),
                "Impressions": st.column_config.NumberColumn(format="%d"),
                "Installs": st.column_config.NumberColumn(format="%d"),
                "Signups": st.column_config.NumberColumn(format="%d"),
                "CPA (Install)": st.column_config.NumberColumn(format="$%.2f"),
                "CTR %": st.column_config.NumberColumn(format="%.2f%%"),
            },
        )

        # Channel share pie chart
        col_pie1, col_pie2 = st.columns(2)
        with col_pie1:
            spend_data = channel_df[channel_df["spend"] > 0]
            if not spend_data.empty:
                fig = px.pie(
                    spend_data, values="spend", names="channel",
                    title="Spend Distribution",
                )
                fig.update_layout(height=300, margin=dict(t=40, b=10))
                st.plotly_chart(fig, use_container_width=True)

        with col_pie2:
            install_data = channel_df[channel_df["installs"] > 0]
            if not install_data.empty:
                fig = px.pie(
                    install_data, values="installs", names="channel",
                    title="Install Distribution",
                )
                fig.update_layout(height=300, margin=dict(t=40, b=10))
                st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ── Row 3: Acquisition Funnel ──────────────────────────
    st.subheader("Acquisition Funnel")

    funnel_data = pd.DataFrame({
        "stage": ["Impressions", "Clicks", "Installs", "Signups"],
        "count": [impressions, clicks, installs, signups],
    })
    funnel_data = funnel_data[funnel_data["count"] > 0]

    if not funnel_data.empty:
        fig = go.Figure(go.Funnel(
            y=funnel_data["stage"],
            x=funnel_data["count"],
            textinfo="value+percent previous",
            marker=dict(color=["#4A90D9", "#67B26F", "#F4A261", "#E76F51"]),
        ))
        fig.update_layout(
            height=300,
            margin=dict(t=20, b=20, l=10, r=10),
        )
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ── Row 4: Spend vs Signups Trend ──────────────────────
    st.subheader("Spend vs Signups Over Time")

    trend_df = engine.query(
        metrics=["spend", "signups", "installs"],
        dims=["date_day"],
        date_range=(start_str, end_str),
    )

    if trend_df is not None and not trend_df.empty:
        trend_df = trend_df.sort_values("date_day")

        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=trend_df["date_day"], y=trend_df["spend"],
            name="Spend ($)", yaxis="y", marker_color="#4A90D9", opacity=0.6,
        ))
        fig.add_trace(go.Scatter(
            x=trend_df["date_day"], y=trend_df["signups"],
            name="Signups", yaxis="y2", line=dict(color="#E76F51", width=2),
        ))
        fig.add_trace(go.Scatter(
            x=trend_df["date_day"], y=trend_df["installs"],
            name="Installs", yaxis="y2", line=dict(color="#67B26F", width=2, dash="dot"),
        ))
        fig.update_layout(
            height=350,
            margin=dict(t=20, b=30),
            yaxis=dict(title="Spend ($)", side="left"),
            yaxis2=dict(title="Count", side="right", overlaying="y"),
            legend=dict(orientation="h", y=-0.15),
            hovermode="x unified",
        )
        st.plotly_chart(fig, use_container_width=True)

    # ── Row 5: Channel Mix Over Time ───────────────────────
    with st.expander("Channel Mix Over Time", expanded=False):
        mix_df = engine.query(
            metrics=["spend"],
            dims=["date_day", "channel"],
            date_range=(start_str, end_str),
        )

        if mix_df is not None and not mix_df.empty:
            mix_df = mix_df.sort_values("date_day")
            fig = px.area(
                mix_df, x="date_day", y="spend", color="channel",
                title="Daily Spend by Channel",
            )
            fig.update_layout(
                height=350,
                margin=dict(t=40, b=30),
                legend=dict(orientation="h", y=-0.2),
            )
            st.plotly_chart(fig, use_container_width=True)
