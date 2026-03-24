"""
AppsFlyer Mobile Analytics Dashboard Component

Provides a Streamlit dashboard for visualizing AppsFlyer aggregate data
with side-by-side iOS and Android views.

Layout:
- Row 0: Platform selector + date filter
- Row 1: KPI tiles (Installs, Clicks, Sessions, Sign-ups, Loyal Users)
- Row 2: Daily installs trend (iOS vs Android)
- Row 3: Top countries by installs
- Row 4: Media source breakdown
- Row 5: In-app events funnel
- Row 6: Raw data explorer

Data source: af_daily_geo table in DuckDB
"""

from datetime import datetime, timedelta, date
from typing import Optional

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from app.components.glossary import TERM_TOOLTIPS
from app.components.utils import query_duckdb, check_tables_exist

AF_TABLES = ["af_daily_sources", "af_daily_geo"]


# ============================================
# KPI Tile Helper
# ============================================


def _kpi_tile(label: str, ios_val, android_val, fmt: str = ",.0f"):
    """
    Render a KPI tile with iOS and Android side-by-side.
    
    Includes a tooltip with the glossary definition (hover over label).

    Args:
        label: KPI name (e.g., "Installs") - should match a TERM_TOOLTIPS key
        ios_val: iOS metric value
        android_val: Android metric value
        fmt: Python format string for the number
    """
    ios_display = f"{ios_val:{fmt}}" if ios_val is not None else "–"
    android_display = f"{android_val:{fmt}}" if android_val is not None else "–"
    total = (ios_val or 0) + (android_val or 0)
    total_display = f"{total:{fmt}}"
    
    # Get tooltip from glossary
    tooltip = TERM_TOOLTIPS.get(label, "")
    tooltip_attr = f'title="{tooltip}"' if tooltip else ""
    # Add help indicator if tooltip exists
    help_indicator = ' <span style="color:#666;font-size:10px;">ⓘ</span>' if tooltip else ""

    st.markdown(
        f"""
        <div style="background:#1e1e2f;border-radius:10px;padding:14px 16px;text-align:center;">
            <div style="color:#aaa;font-size:12px;text-transform:uppercase;cursor:help;" {tooltip_attr}>
                {label}{help_indicator}
            </div>
            <div style="font-size:28px;font-weight:700;color:#fff;margin:4px 0;">{total_display}</div>
            <div style="display:flex;justify-content:space-around;margin-top:6px;">
                <div>
                    <span style="color:#999;font-size:11px;">iOS</span><br/>
                    <span style="color:#4fc3f7;font-size:15px;font-weight:600;">{ios_display}</span>
                </div>
                <div>
                    <span style="color:#999;font-size:11px;">Android</span><br/>
                    <span style="color:#81c784;font-size:15px;font-weight:600;">{android_display}</span>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ============================================
# Main Dashboard Renderer
# ============================================


def render_appsflyer_dashboard(duckdb_path: str) -> None:
    """
    Render the full AppsFlyer mobile analytics dashboard.

    Args:
        duckdb_path: Path to the DuckDB database
    """
    st.header("📱 AppsFlyer – Mobile App Analytics")

    # ── Check data availability ──
    has_data, total_rows, tables = check_tables_exist(duckdb_path, AF_TABLES)

    if not has_data:
        st.info(
            """
            **No AppsFlyer data available yet.**

            Run the AppsFlyer ETL pipeline to populate the database:
            ```bash
            python scripts/run_etl_appsflyer.py --lookback-days 30
            ```
            """
        )
        return

    st.success(f"AppsFlyer data loaded: {total_rows:,} rows across {len(tables)} table(s)")

    # ── Date range filter ──
    date_range_df = query_duckdb(
        duckdb_path,
        "SELECT MIN(date) as min_d, MAX(date) as max_d FROM af_daily_geo",
    )

    if date_range_df is None or date_range_df.empty:
        st.warning("No data found in af_daily_geo.")
        return

    min_date = pd.to_datetime(date_range_df["min_d"].iloc[0]).date()
    max_date = pd.to_datetime(date_range_df["max_d"].iloc[0]).date()

    col_d1, col_d2, col_d3 = st.columns([1, 1, 2])
    with col_d1:
        start_date = st.date_input("From", value=min_date, min_value=min_date, max_value=max_date, key="af_start")
    with col_d2:
        end_date = st.date_input("To", value=max_date, min_value=min_date, max_value=max_date, key="af_end")
    with col_d3:
        # Quick range buttons
        quick = st.radio(
            "Quick range",
            ["Custom", "Last 7 days", "Last 14 days", "Last 30 days"],
            horizontal=True,
            key="af_quick",
        )
        if quick == "Last 7 days":
            start_date = max_date - timedelta(days=6)
        elif quick == "Last 14 days":
            start_date = max_date - timedelta(days=13)
        elif quick == "Last 30 days":
            start_date = min_date  # Use all available data

    # Convert to string for SQL
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    date_filter = f"date >= '{start_str}' AND date <= '{end_str}'"

    st.divider()

    # ============================================================
    # SECTION 1: KPI TILES
    # ============================================================
    st.subheader("Key Performance Indicators")

    kpi_query = f"""
        SELECT
            platform,
            SUM(installs)                   AS installs,
            SUM(clicks)                     AS clicks,
            SUM(sessions)                   AS sessions,
            SUM(loyal_users)                AS loyal_users,
            SUM(user_sign_up_unique_users)  AS signups,
            SUM(screen_view_event_counter)  AS screen_views,
            SUM(deposit_unique_users)       AS deposits,
            SUM(total_cost)                 AS cost,
            SUM(total_revenue)              AS revenue
        FROM af_daily_geo
        WHERE {date_filter}
        GROUP BY platform
    """
    kpi_df = query_duckdb(duckdb_path, kpi_query)

    if kpi_df is not None and not kpi_df.empty:
        # Extract per-platform values
        ios_row = kpi_df[kpi_df["platform"] == "ios"].iloc[0] if "ios" in kpi_df["platform"].values else None
        and_row = kpi_df[kpi_df["platform"] == "android"].iloc[0] if "android" in kpi_df["platform"].values else None

        def _val(row, col):
            """Safely get a numeric value from a row."""
            if row is None:
                return None
            v = row.get(col)
            if v is None or pd.isna(v):
                return 0
            return float(v)

        # Render 5 KPI tiles in a row
        k1, k2, k3, k4, k5 = st.columns(5)
        with k1:
            _kpi_tile("Installs", _val(ios_row, "installs"), _val(and_row, "installs"))
        with k2:
            _kpi_tile("Clicks", _val(ios_row, "clicks"), _val(and_row, "clicks"))
        with k3:
            _kpi_tile("Sessions", _val(ios_row, "sessions"), _val(and_row, "sessions"))
        with k4:
            _kpi_tile("Sign-ups", _val(ios_row, "signups"), _val(and_row, "signups"))
        with k5:
            _kpi_tile("Loyal Users", _val(ios_row, "loyal_users"), _val(and_row, "loyal_users"))

    st.divider()

    # ============================================================
    # SECTION 2: DAILY INSTALLS TREND (iOS vs Android)
    # ============================================================
    st.subheader("📈 Daily Installs Trend")

    trend_query = f"""
        SELECT
            date,
            platform,
            SUM(installs)  AS installs,
            SUM(clicks)    AS clicks,
            SUM(sessions)  AS sessions,
            SUM(user_sign_up_unique_users) AS signups
        FROM af_daily_geo
        WHERE {date_filter}
        GROUP BY date, platform
        ORDER BY date
    """
    trend_df = query_duckdb(duckdb_path, trend_query)

    if trend_df is not None and not trend_df.empty:
        trend_df["date"] = pd.to_datetime(trend_df["date"])

        tab_inst, tab_click, tab_sess, tab_sign = st.tabs(
            ["Installs", "Clicks", "Sessions", "Sign-ups"]
        )

        for tab, metric, title in [
            (tab_inst, "installs", "Daily Installs"),
            (tab_click, "clicks", "Daily Clicks"),
            (tab_sess, "sessions", "Daily Sessions"),
            (tab_sign, "signups", "Daily Sign-ups"),
        ]:
            with tab:
                fig = px.line(
                    trend_df,
                    x="date",
                    y=metric,
                    color="platform",
                    color_discrete_map={"ios": "#4fc3f7", "android": "#81c784"},
                    title=title,
                    labels={"date": "Date", metric: title, "platform": "Platform"},
                )
                fig.update_layout(
                    template="plotly_dark",
                    height=380,
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                )
                st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ============================================================
    # SECTION 3: TOP COUNTRIES
    # ============================================================
    st.subheader("🌍 Top Countries by Installs")

    col_ios_geo, col_and_geo = st.columns(2)

    for col, plat, color in [
        (col_ios_geo, "ios", "#4fc3f7"),
        (col_and_geo, "android", "#81c784"),
    ]:
        with col:
            st.markdown(f"**{'iOS' if plat == 'ios' else 'Android'}**")
            geo_query = f"""
                SELECT
                    country,
                    SUM(installs) AS installs,
                    SUM(clicks)   AS clicks,
                    SUM(user_sign_up_unique_users) AS signups
                FROM af_daily_geo
                WHERE {date_filter} AND platform = '{plat}'
                GROUP BY country
                HAVING SUM(installs) > 0
                ORDER BY installs DESC
                LIMIT 15
            """
            geo_df = query_duckdb(duckdb_path, geo_query)

            if geo_df is not None and not geo_df.empty:
                fig_geo = px.bar(
                    geo_df,
                    x="country",
                    y="installs",
                    color_discrete_sequence=[color],
                    text="installs",
                    title=f"Top Countries – {'iOS' if plat == 'ios' else 'Android'}",
                )
                fig_geo.update_layout(
                    template="plotly_dark",
                    height=350,
                    xaxis_title="Country",
                    yaxis_title="Installs",
                    showlegend=False,
                )
                fig_geo.update_traces(texttemplate="%{text:.0f}", textposition="outside")
                st.plotly_chart(fig_geo, use_container_width=True)
            else:
                st.info("No install data for this platform in the selected range.")

    st.divider()

    # ============================================================
    # SECTION 4: MEDIA SOURCE BREAKDOWN
    # ============================================================
    st.subheader("📊 Media Source Breakdown")

    source_query = f"""
        SELECT
            platform,
            media_source,
            SUM(installs) AS installs,
            SUM(clicks)   AS clicks,
            SUM(sessions) AS sessions,
            SUM(user_sign_up_unique_users) AS signups
        FROM af_daily_geo
        WHERE {date_filter}
        GROUP BY platform, media_source
        ORDER BY installs DESC
    """
    source_df = query_duckdb(duckdb_path, source_query)

    if source_df is not None and not source_df.empty:
        col_src_ios, col_src_and = st.columns(2)

        for col, plat, label in [
            (col_src_ios, "ios", "iOS"),
            (col_src_and, "android", "Android"),
        ]:
            with col:
                st.markdown(f"**{label} – Media Sources**")
                plat_df = source_df[source_df["platform"] == plat].copy()

                if not plat_df.empty:
                    fig_src = px.pie(
                        plat_df,
                        names="media_source",
                        values="installs",
                        title=f"{label} Installs by Source",
                        hole=0.4,
                    )
                    fig_src.update_layout(template="plotly_dark", height=350)
                    st.plotly_chart(fig_src, use_container_width=True)

                    # Also show as a data table
                    display = plat_df[["media_source", "installs", "clicks", "signups"]].copy()
                    display.columns = ["Source", "Installs", "Clicks", "Sign-ups"]
                    st.dataframe(display, use_container_width=True, hide_index=True)
                else:
                    st.info(f"No data for {label}.")

    st.divider()

    # ============================================================
    # SECTION 5: IN-APP EVENTS COMPARISON
    # ============================================================
    st.subheader("🎯 In-App Events (iOS vs Android)")

    events_query = f"""
        SELECT
            platform,
            SUM(app_install_unique_users)       AS app_installs,
            SUM(user_sign_up_unique_users)      AS sign_ups,
            SUM(create_instance_unique_users)   AS create_instance,
            SUM(screen_view_unique_users)       AS screen_views,
            SUM(deep_link_opened_unique_users)  AS deep_links,
            SUM(deposit_unique_users)           AS deposits
        FROM af_daily_geo
        WHERE {date_filter}
        GROUP BY platform
    """
    events_df = query_duckdb(duckdb_path, events_query)

    if events_df is not None and not events_df.empty:
        # Melt to long format for grouped bar chart
        event_cols = ["app_installs", "sign_ups", "create_instance", "screen_views", "deep_links", "deposits"]
        melted = events_df.melt(
            id_vars=["platform"],
            value_vars=event_cols,
            var_name="event",
            value_name="unique_users",
        )
        # Clean up event names for display
        melted["event"] = melted["event"].str.replace("_", " ").str.title()

        fig_events = px.bar(
            melted,
            x="event",
            y="unique_users",
            color="platform",
            barmode="group",
            color_discrete_map={"ios": "#4fc3f7", "android": "#81c784"},
            title="In-App Events – Unique Users (iOS vs Android)",
            labels={"event": "Event", "unique_users": "Unique Users", "platform": "Platform"},
        )
        fig_events.update_layout(
            template="plotly_dark",
            height=400,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        st.plotly_chart(fig_events, use_container_width=True)

    st.divider()

    # ============================================================
    # SECTION 6: DAILY COUNTRY HEATMAP
    # ============================================================
    st.subheader("🗺️ Daily Installs by Country")

    platform_choice = st.radio(
        "Platform",
        ["ios", "android"],
        format_func=lambda x: "iOS" if x == "ios" else "Android",
        horizontal=True,
        key="af_heatmap_plat",
    )

    heatmap_query = f"""
        SELECT date, country, SUM(installs) AS installs
        FROM af_daily_geo
        WHERE {date_filter} AND platform = '{platform_choice}'
        GROUP BY date, country
        HAVING SUM(installs) > 0
        ORDER BY date, country
    """
    heat_df = query_duckdb(duckdb_path, heatmap_query)

    if heat_df is not None and not heat_df.empty:
        # Pivot for heatmap: rows = country, cols = date
        pivot = heat_df.pivot_table(index="country", columns="date", values="installs", aggfunc="sum", fill_value=0)

        # Sort by total installs descending
        pivot["_total"] = pivot.sum(axis=1)
        pivot = pivot.sort_values("_total", ascending=False).drop(columns=["_total"]).head(15)

        fig_heat = px.imshow(
            pivot,
            labels=dict(x="Date", y="Country", color="Installs"),
            color_continuous_scale="Blues" if platform_choice == "ios" else "Greens",
            aspect="auto",
            title=f"Daily Installs Heatmap – {'iOS' if platform_choice == 'ios' else 'Android'} (Top 15 Countries)",
        )
        fig_heat.update_layout(template="plotly_dark", height=450)
        st.plotly_chart(fig_heat, use_container_width=True)
    else:
        st.info("No data for the selected range.")

    st.divider()

    # ============================================================
    # SECTION 7: RAW DATA EXPLORER
    # ============================================================
    with st.expander("📋 Explore Raw AppsFlyer Data"):
        table_options = [t for t in tables]
        if not table_options:
            table_options = ["af_daily_geo"]

        table_choice = st.selectbox("Select Table", options=table_options, key="af_table_choice")

        if table_choice:
            raw_df = query_duckdb(duckdb_path, f"SELECT * FROM {table_choice} ORDER BY date DESC LIMIT 500")
            if raw_df is not None:
                st.dataframe(raw_df, use_container_width=True)
            else:
                st.info("No data available.")
