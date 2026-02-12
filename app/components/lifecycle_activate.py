"""
Activate Mega-Page — Signup → Verify → First VPS → First Payment.

Shows the in-product activation journey after a user signs up.
Answers: "Once we acquire users, how effectively do we activate them?"

Layout:
1. Activation KPI tiles (signups, verified, first VPS, first payment, conversion rates)
2. Activation funnel (visual)
3. Cohort-based time-to-activate curves (reuses CohortEngine)
4. Activation bottleneck analysis (where do users drop off?)
5. User activation timeline (daily new activations)

Data sources:
- core.dim_user (signups, mobile verification)
- core.fact_user_activity (VPS launches)
- core.fact_payment_topup (first payment)
- CohortEngine for time-window analysis
"""

import logging
from datetime import date
from typing import Optional

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import duckdb

from rs_analytics.db.client import DuckDBClient
from rs_analytics.metrics.cohorts import CohortEngine
from rs_analytics.utils.formatting import safe_float, safe_divide

logger = logging.getLogger(__name__)


# ============================================
# Helpers
# ============================================

def _query(duckdb_path: str, sql: str) -> Optional[pd.DataFrame]:
    """Run a read-only query against the DuckDB warehouse."""
    try:
        conn = duckdb.connect(duckdb_path, read_only=True)
        df = conn.execute(sql).fetchdf()
        conn.close()
        return df if not df.empty else None
    except Exception as e:
        logger.warning("Query failed: %s", e)
        return None


# ============================================
# Main Render Function
# ============================================

def render_activate_page(duckdb_path: str):
    """
    Render the Activate mega-page.

    Shows the in-product activation funnel: how quickly and effectively
    new signups become verified, engaged (VPS), and paying users.
    """
    st.title("⚡ Activate")
    st.markdown("*Signup → verify → first VPS → first payment — how fast do users activate?*")

    # Date range
    from app.components.date_picker import render_date_range_picker
    start_date, end_date, prev_start, prev_end = render_date_range_picker(
        key="activate_page",
        default_days=90,
        max_days=365,
        show_comparison=True,
    )

    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    st.divider()

    # ── Row 1: Activation KPIs ─────────────────────────────
    st.subheader("Activation KPIs")
    st.caption(f"Users who signed up between {start_str} and {end_str}")

    kpi_sql = f"""
    WITH period_users AS (
        SELECT
            u.user_id,
            u.registration_ts,
            u.mobile_verified,
            u.mobile_verified_at,
            s.total_launch_count,
            s.total_points_earned_paid,
            s.current_vps_live,
            s.last_activity_ts
        FROM core.dim_user u
        LEFT JOIN core.user_account_state s ON u.user_id = s.user_id
        WHERE DATE(u.registration_ts) >= '{start_str}'
          AND DATE(u.registration_ts) <= '{end_str}'
    )
    SELECT
        COUNT(*)                                                    AS total_signups,
        SUM(CASE WHEN mobile_verified THEN 1 ELSE 0 END)          AS verified,
        SUM(CASE WHEN total_launch_count > 0 THEN 1 ELSE 0 END)   AS created_vps,
        SUM(CASE WHEN total_points_earned_paid > 0 THEN 1 ELSE 0 END) AS paid,
        SUM(CASE WHEN current_vps_live > 0 THEN 1 ELSE 0 END)    AS active_vps_now
    FROM period_users
    """

    kpi_df = _query(duckdb_path, kpi_sql)

    if kpi_df is None or kpi_df.empty:
        st.warning(
            "No user data available. Run the User Logs ETL first: "
            "`python scripts/run_etl_user_logs.py`"
        )
        return

    row = kpi_df.iloc[0]
    signups = int(row["total_signups"])
    verified = int(row["verified"])
    created_vps = int(row["created_vps"])
    paid = int(row["paid"])
    active_now = int(row["active_vps_now"])

    if signups == 0:
        st.info("No signups in this period.")
        return

    verify_rate = verified / signups * 100
    vps_rate = created_vps / signups * 100
    paid_rate = paid / signups * 100

    # KPI tiles
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Signups", f"{signups:,}")
    with col2:
        st.metric("Verified", f"{verified:,}", delta=f"{verify_rate:.1f}%")
    with col3:
        st.metric("First VPS", f"{created_vps:,}", delta=f"{vps_rate:.1f}%")
    with col4:
        st.metric("First Payment", f"{paid:,}", delta=f"{paid_rate:.1f}%")
    with col5:
        st.metric("Active VPS Now", f"{active_now:,}")

    st.divider()

    # ── Row 2: Activation Funnel ───────────────────────────
    st.subheader("Activation Funnel")

    funnel_data = pd.DataFrame({
        "stage": ["Signup", "Verified", "Created VPS", "First Payment"],
        "count": [signups, verified, created_vps, paid],
    })

    fig = go.Figure(go.Funnel(
        y=funnel_data["stage"],
        x=funnel_data["count"],
        textinfo="value+percent previous",
        marker=dict(color=["#4A90D9", "#67B26F", "#F4A261", "#E76F51"]),
    ))
    fig.update_layout(
        height=300,
        margin=dict(t=10, b=10, l=10, r=10),
    )
    st.plotly_chart(fig, use_container_width=True)

    # Step-by-step conversion rates
    st.markdown("**Step Conversion Rates:**")
    steps = [
        ("Signup → Verified", verified, signups),
        ("Verified → VPS", created_vps, verified),
        ("VPS → Paid", paid, created_vps),
    ]
    cols = st.columns(3)
    for i, (label, num, denom) in enumerate(steps):
        rate = (num / denom * 100) if denom > 0 else 0
        dropoff = max(0, denom - num)  # Prevent negative drop-off
        with cols[i]:
            st.markdown(f"**{label}**")
            if rate > 100:
                # When rate > 100%, users reached this stage via another path
                st.markdown(f"Conv: **{rate:.1f}%** *")
                st.caption("*Some users skip earlier steps")
            else:
                st.markdown(f"Conv: **{rate:.1f}%** | Drop-off: {dropoff:,}")

    st.divider()

    # ── Row 3: Time-to-Activate Curves ─────────────────────
    st.subheader("Time-to-Activate")
    st.caption("How many days after signup do users reach each milestone?")

    client = DuckDBClient(duckdb_path)
    cohort_engine = CohortEngine(client)

    # Use weekly cohorts for the selected period
    progression_df = cohort_engine.cohort_progression(
        granularity="week",
        max_days=90,
        bucket_days=7,
        min_cohort_size=2,
    )

    if progression_df is not None and not progression_df.empty:
        # Aggregate across all cohorts for an overall view
        overall = progression_df.groupby(["stage", "days_bucket"]).agg(
            total_users=("user_count", "sum"),
            total_cohort=("cohort_size", "sum"),
        ).reset_index()
        overall["cumulative_pct"] = (overall["total_users"] / overall["total_cohort"] * 100).round(1)

        fig = px.line(
            overall,
            x="days_bucket",
            y="cumulative_pct",
            color="stage",
            title="Cumulative Activation Over Time (All Cohorts)",
            labels={
                "days_bucket": "Days Since Signup",
                "cumulative_pct": "Cumulative %",
                "stage": "Stage",
            },
            color_discrete_map={
                "Verified": "#67B26F",
                "First VPS": "#F4A261",
                "First Paid": "#E76F51",
            },
        )
        fig.update_layout(
            height=350,
            margin=dict(t=40, b=30),
            legend=dict(orientation="h", y=-0.15),
        )
        st.plotly_chart(fig, use_container_width=True)

        # Median time to convert
        summary = cohort_engine.cohort_summary(granularity="week", min_cohort_size=2)
        if summary is not None and not summary.empty:
            overall_medians = summary[["median_days_to_verify", "median_days_to_vps", "median_days_to_paid"]].median()
            col1, col2, col3 = st.columns(3)
            with col1:
                val = overall_medians.get("median_days_to_verify")
                st.metric("Median Days → Verify", f"{val:.0f}" if pd.notna(val) else "—")
            with col2:
                val = overall_medians.get("median_days_to_vps")
                st.metric("Median Days → VPS", f"{val:.0f}" if pd.notna(val) else "—")
            with col3:
                val = overall_medians.get("median_days_to_paid")
                st.metric("Median Days → Paid", f"{val:.0f}" if pd.notna(val) else "—")
    else:
        st.info("No cohort progression data available.")

    st.divider()

    # ── Row 4: Daily Activation Timeline ───────────────────
    st.subheader("Daily Activation Timeline")

    timeline_sql = f"""
    SELECT
        DATE(registration_ts) AS day,
        COUNT(*) AS signups,
        SUM(CASE WHEN mobile_verified THEN 1 ELSE 0 END) AS verified
    FROM core.dim_user
    WHERE DATE(registration_ts) >= '{start_str}'
      AND DATE(registration_ts) <= '{end_str}'
    GROUP BY day
    ORDER BY day
    """

    timeline_df = _query(duckdb_path, timeline_sql)

    if timeline_df is not None and not timeline_df.empty:
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=timeline_df["day"], y=timeline_df["signups"],
            name="Signups", marker_color="#4A90D9", opacity=0.7,
        ))
        fig.add_trace(go.Bar(
            x=timeline_df["day"], y=timeline_df["verified"],
            name="Verified", marker_color="#67B26F", opacity=0.7,
        ))
        fig.update_layout(
            height=300,
            margin=dict(t=20, b=30),
            barmode="overlay",
            legend=dict(orientation="h", y=-0.15),
            hovermode="x unified",
        )
        st.plotly_chart(fig, use_container_width=True)

    # ── Row 5: Bottleneck Analysis ─────────────────────────
    with st.expander("Bottleneck Analysis", expanded=False):
        st.markdown("""
        **Where are users getting stuck?**

        The biggest drop-off point is the bottleneck you should focus on.
        """)

        # Find the step with the lowest conversion (the real bottleneck)
        # Filter out steps where rate > 100% (non-linear paths)
        valid_steps = [(l, n, d) for l, n, d in steps if d > 0 and (n / d * 100) <= 100]
        if valid_steps:
            worst_drop = min(valid_steps, key=lambda s: s[1] / s[2])
            worst_label, worst_num, worst_denom = worst_drop
            worst_rate = (worst_num / worst_denom * 100) if worst_denom > 0 else 0
            worst_dropoff = max(0, worst_denom - worst_num)

            st.warning(
                f"**Biggest bottleneck: {worst_label}** — "
                f"Only {worst_rate:.1f}% convert ({worst_dropoff:,} users lost). "
                "Focus improvement efforts here."
            )
        else:
            st.info("No clear bottleneck detected (some steps have non-linear paths).")

        # Show all steps as a bar chart
        step_df = pd.DataFrame([
            {"Step": label, "Conversion %": min((n / d * 100) if d > 0 else 0, 100), "Drop-off": max(0, d - n)}
            for label, n, d in steps
        ])
        fig = px.bar(
            step_df, x="Step", y="Conversion %",
            text="Conversion %",
            color="Conversion %",
            color_continuous_scale="RdYlGn",
        )
        fig.update_traces(texttemplate="%{text:.1f}%")
        fig.update_layout(
            height=250,
            margin=dict(t=10, b=20),
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True)
