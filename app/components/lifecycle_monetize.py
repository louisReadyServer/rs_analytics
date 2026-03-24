"""
Monetize Mega-Page — Revenue, LTV, Points Economy.

Answers: "How effectively do we monetize our users?"

Layout:
1. Revenue KPI tiles (total revenue, ARPU, payer count, avg transaction, LTV proxy)
2. Revenue trend (daily revenue + payer count over time)
3. Points economy health (earned vs spent, velocity, balance growth)
4. Revenue cohort analysis (revenue per signup cohort)
5. Top-up package distribution
6. User monetization segments (whales / mid / low / free)

Data sources:
- mart.platform_daily_overview (daily revenue KPIs)
- core.fact_payment_topup (payment-level data)
- core.user_account_state (per-user LTV proxy)
- core.fact_points_ledger (points movements)
- v_exec_daily (cross-channel revenue from MetricEngine)
"""

import logging
from datetime import date
from typing import Optional

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from rs_analytics.db.client import DuckDBClient
from rs_analytics.utils.formatting import safe_float, safe_divide

from app.components.utils import query_duckdb as _query
from app.components.glossary import TERM_TOOLTIPS

logger = logging.getLogger(__name__)


# ============================================
# Main Render Function
# ============================================

def render_monetize_page(duckdb_path: str):
    """
    Render the Monetize mega-page.

    Shows revenue performance, points economy health, and user
    monetization segmentation.
    """
    st.title("💰 Monetize")
    st.markdown("*Revenue, LTV, points economy — how well do we monetize our user base?*")

    # Date range
    from app.components.date_picker import render_date_range_picker
    start_date, end_date, prev_start, prev_end = render_date_range_picker(
        key="monetize_page",
        default_days=30,
        max_days=365,
        show_comparison=True,
    )

    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    prev_start_str = prev_start.strftime("%Y-%m-%d") if prev_start else None
    prev_end_str = prev_end.strftime("%Y-%m-%d") if prev_end else None

    st.divider()

    # ── Row 1: Revenue KPIs ────────────────────────────────
    st.subheader("Revenue KPIs")

    kpi_sql = f"""
    SELECT
        COALESCE(SUM(topups_sum_sgd), 0)    AS total_revenue,
        COALESCE(SUM(topups_count), 0)      AS total_topups,
        COALESCE(SUM(payer_count), 0)       AS total_payer_days,
        COALESCE(SUM(new_signups), 0)       AS total_signups
    FROM mart.platform_daily_overview
    WHERE activity_date >= '{start_str}'
      AND activity_date <= '{end_str}'
    """

    kpi_df = _query(duckdb_path, kpi_sql)
    if kpi_df is None:
        st.warning(
            "No revenue data available. Run the User Logs ETL first: "
            "`python scripts/run_etl_user_logs.py`"
        )
        return

    row = kpi_df.iloc[0]
    total_revenue = safe_float(row["total_revenue"])
    total_topups = int(row["total_topups"])
    total_payer_days = int(row["total_payer_days"])
    total_signups = int(row["total_signups"])

    avg_transaction = safe_divide(total_revenue, total_topups)
    arpu = safe_divide(total_revenue, total_signups)  # Revenue per signup in period

    # Comparison period
    prev_revenue = None
    if prev_start_str and prev_end_str:
        prev_kpi = _query(duckdb_path, f"""
            SELECT COALESCE(SUM(topups_sum_sgd), 0) AS total_revenue,
                   COALESCE(SUM(topups_count), 0) AS total_topups
            FROM mart.platform_daily_overview
            WHERE activity_date >= '{prev_start_str}'
              AND activity_date <= '{prev_end_str}'
        """)
        if prev_kpi is not None:
            prev_revenue = safe_float(prev_kpi.iloc[0]["total_revenue"])

    # Display tiles
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        delta = f"{((total_revenue - prev_revenue) / prev_revenue * 100):+.1f}%" if prev_revenue else None
        st.metric("Revenue (SGD)", f"${total_revenue:,.0f}", delta=delta)
    with col2:
        st.metric("Transactions", f"{total_topups:,}")
    with col3:
        st.metric("Avg Transaction", f"${avg_transaction:,.2f}" if avg_transaction else "—")
    with col4:
        st.metric("ARPU", f"${arpu:,.2f}" if arpu else "—",
                  help="Average Revenue Per User (signups in period)")
    with col5:
        st.metric("Unique Payer-Days", f"{total_payer_days:,}",
                  help="Sum of unique daily payers across period")

    st.divider()

    # ── Row 2: Revenue Trend ───────────────────────────────
    st.subheader("Revenue Trend")

    trend_sql = f"""
    SELECT
        activity_date AS day,
        COALESCE(topups_sum_sgd, 0) AS revenue,
        COALESCE(topups_count, 0)   AS transactions,
        COALESCE(payer_count, 0)    AS payers
    FROM mart.platform_daily_overview
    WHERE activity_date >= '{start_str}'
      AND activity_date <= '{end_str}'
    ORDER BY activity_date
    """

    trend_df = _query(duckdb_path, trend_sql)

    if trend_df is not None and not trend_df.empty:
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=trend_df["day"], y=trend_df["revenue"],
            name="Revenue ($)", marker_color="#67B26F", opacity=0.7,
        ))
        fig.add_trace(go.Scatter(
            x=trend_df["day"], y=trend_df["payers"],
            name="Unique Payers", yaxis="y2",
            line=dict(color="#E76F51", width=2),
        ))
        fig.update_layout(
            height=350,
            margin=dict(t=20, b=30),
            yaxis=dict(title="Revenue (SGD)"),
            yaxis2=dict(title="Unique Payers", side="right", overlaying="y"),
            legend=dict(orientation="h", y=-0.15),
            hovermode="x unified",
        )
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ── Row 3: Points Economy ──────────────────────────────
    st.subheader("Points Economy Health")
    st.caption("Are users earning and spending points at a healthy rate?")

    points_sql = f"""
    SELECT
        activity_date AS day,
        COALESCE(points_earned_paid, 0) AS earned_paid,
        COALESCE(points_earned_free, 0) AS earned_free,
        COALESCE(points_spent, 0)       AS spent,
        COALESCE(net_points_delta, 0)   AS net_delta
    FROM mart.platform_daily_overview
    WHERE activity_date >= '{start_str}'
      AND activity_date <= '{end_str}'
    ORDER BY activity_date
    """

    points_df = _query(duckdb_path, points_sql)

    if points_df is not None and not points_df.empty:
        # Summary tiles
        total_earned_paid = points_df["earned_paid"].sum()
        total_earned_free = points_df["earned_free"].sum()
        total_earned = total_earned_paid + total_earned_free
        total_spent = points_df["spent"].sum()
        velocity = (total_spent / total_earned * 100) if total_earned > 0 else 0

        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.metric("Points Earned (Paid)", f"{total_earned_paid:,.0f}",
                      help="From actual monetary top-ups (payment.csv)")
        with col2:
            st.metric("Points Earned (Free)", f"{total_earned_free:,.0f}",
                      help="From promos, bonuses, referrals")
        with col3:
            st.metric("Points Spent", f"{total_spent:,.0f}")
        with col4:
            # Velocity: 50-80% is healthy
            velocity_color = "normal" if 50 <= velocity <= 80 else ("inverse" if velocity > 100 else "off")
            st.metric("Velocity", f"{velocity:.1f}%",
                      help="Points spent / earned. 50-80% is healthy. >100% = spending down balances.")
        with col5:
            net = total_earned - total_spent
            st.metric("Net Balance Change", f"{net:+,.0f}")

        # Points trend chart
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=points_df["day"], y=points_df["earned_paid"],
            name="Earned (Paid)", marker_color="#4A90D9",
        ))
        fig.add_trace(go.Bar(
            x=points_df["day"], y=points_df["earned_free"],
            name="Earned (Free)", marker_color="#67B26F",
        ))
        fig.add_trace(go.Bar(
            x=points_df["day"], y=-points_df["spent"],
            name="Spent", marker_color="#E76F51",
        ))
        fig.update_layout(
            height=350,
            margin=dict(t=20, b=30),
            barmode="relative",
            yaxis_title="Points",
            legend=dict(orientation="h", y=-0.15),
            hovermode="x unified",
        )
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ── Row 4: User Monetization Segments ──────────────────
    st.subheader("User Monetization Segments")
    st.caption("How is revenue distributed across your user base?")

    segments_sql = """
    SELECT
        CASE
            WHEN total_points_earned_paid = 0 THEN 'Free (no payment)'
            WHEN total_points_earned_paid <= 7200 THEN 'Low (≤$50)'
            WHEN total_points_earned_paid <= 28800 THEN 'Mid ($50-$200)'
            ELSE 'Whale (>$200)'
        END AS segment,
        COUNT(*) AS user_count,
        SUM(total_points_earned_paid) AS total_points_paid,
        AVG(total_points_earned_paid) AS avg_points_paid,
        SUM(total_points_spent) AS total_points_consumed,
        AVG(current_vps_live) AS avg_live_vps
    FROM core.user_account_state
    GROUP BY segment
    ORDER BY
        CASE segment
            WHEN 'Whale (>$200)' THEN 1
            WHEN 'Mid ($50-$200)' THEN 2
            WHEN 'Low (≤$50)' THEN 3
            ELSE 4
        END
    """

    segments_df = _query(duckdb_path, segments_sql)

    if segments_df is not None and not segments_df.empty:
        # Summary table — round values for clean display
        display_df = segments_df.copy()
        display_df["revenue_estimate"] = (display_df["total_points_paid"] / 144).round(2)
        display_df["avg_revenue"] = (display_df["avg_points_paid"] / 144).round(2)
        display_df["avg_live_vps"] = display_df["avg_live_vps"].round(1)

        col1, col2 = st.columns([2, 1])

        with col1:
            st.dataframe(
                display_df[["segment", "user_count", "revenue_estimate", "avg_revenue", "avg_live_vps"]].rename(columns={
                    "segment": "Segment",
                    "user_count": "Users",
                    "revenue_estimate": "Total Revenue ($)",
                    "avg_revenue": "Avg Revenue ($)",
                    "avg_live_vps": "Avg Live VPS",
                }),
                hide_index=True,
                use_container_width=True,
                column_config={
                    "Total Revenue ($)": st.column_config.NumberColumn(format="$%.2f"),
                    "Avg Revenue ($)": st.column_config.NumberColumn(format="$%.2f"),
                    "Avg Live VPS": st.column_config.NumberColumn(format="%.1f"),
                },
            )

        with col2:
            # Revenue distribution pie
            paid_segments = display_df[display_df["revenue_estimate"] > 0]
            if not paid_segments.empty:
                fig = px.pie(
                    paid_segments,
                    values="revenue_estimate",
                    names="segment",
                    title="Revenue Share",
                    color_discrete_sequence=["#E76F51", "#F4A261", "#67B26F"],
                )
                fig.update_layout(height=300, margin=dict(t=40, b=10))
                st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ── Row 5: Top-Up Package Distribution ─────────────────
    with st.expander("Top-Up Package Distribution", expanded=False):
        package_sql = f"""
        SELECT
            COALESCE(package_code, 'Unknown') AS package,
            COUNT(*) AS transactions,
            SUM(cash_amount_sgd) AS total_sgd,
            AVG(cash_amount_sgd) AS avg_sgd
        FROM core.fact_payment_topup
        WHERE DATE(payment_ts) >= '{start_str}'
          AND DATE(payment_ts) <= '{end_str}'
        GROUP BY package_code
        ORDER BY total_sgd DESC
        """

        package_df = _query(duckdb_path, package_sql)

        if package_df is not None and not package_df.empty:
            col1, col2 = st.columns(2)
            with col1:
                st.dataframe(
                    package_df.rename(columns={
                        "package": "Package",
                        "transactions": "Count",
                        "total_sgd": "Total (SGD)",
                        "avg_sgd": "Avg (SGD)",
                    }),
                    hide_index=True,
                    use_container_width=True,
                )
            with col2:
                fig = px.bar(
                    package_df, x="package", y="total_sgd",
                    title="Revenue by Package",
                    labels={"package": "Package", "total_sgd": "Revenue (SGD)"},
                    color="total_sgd",
                    color_continuous_scale="Greens",
                )
                fig.update_layout(height=300, margin=dict(t=40, b=20), showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No top-up transactions in this period.")

    # ── Row 6: LTV Proxy Distribution ──────────────────────
    with st.expander("LTV Distribution (Lifetime Revenue)", expanded=False):
        st.caption("Distribution of total lifetime revenue across all paying users")

        ltv_sql = """
        SELECT
            total_points_earned_paid / 144.0 AS lifetime_revenue_sgd
        FROM core.user_account_state
        WHERE total_points_earned_paid > 0
        ORDER BY lifetime_revenue_sgd
        """

        ltv_df = _query(duckdb_path, ltv_sql)

        if ltv_df is not None and not ltv_df.empty:
            fig = px.histogram(
                ltv_df, x="lifetime_revenue_sgd",
                nbins=30,
                title="Lifetime Revenue Distribution (Paying Users)",
                labels={"lifetime_revenue_sgd": "Lifetime Revenue (SGD)"},
            )
            fig.update_layout(height=300, margin=dict(t=40, b=30))
            st.plotly_chart(fig, use_container_width=True)

            # Summary stats
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Paying Users", f"{len(ltv_df):,}")
            with col2:
                st.metric("Median LTV", f"${ltv_df['lifetime_revenue_sgd'].median():,.2f}")
            with col3:
                st.metric("Mean LTV", f"${ltv_df['lifetime_revenue_sgd'].mean():,.2f}")
            with col4:
                st.metric("Max LTV", f"${ltv_df['lifetime_revenue_sgd'].max():,.2f}")
        else:
            st.info("No paying users found.")
