"""
App Analytics Dashboard Component

This module implements the App Analytics dashboard using the User Logs mart data.
It provides an overview of the app's KPIs including:
    - Daily/Weekly platform metrics
    - User activity tracking
    - Points economy overview
    - Revenue and top-up analytics
    - VPS (Virtual Private Server) usage
    - User demographics

Data Sources (from DuckDB mart schema):
    - mart.platform_daily_overview: Daily platform KPIs
    - mart.platform_weekly_overview: Weekly aggregated KPIs
    - mart.user_daily_activity: Per-user daily activity
    - mart.user_daily_points: Per-user daily points movements
    - core.user_account_state: Current user balances and states
    - core.dim_user: User registration data

Author: rs_analytics
Created: 2026-02-12
"""

from datetime import datetime, timedelta, date
from typing import Optional, Dict, Any, List, Tuple
import streamlit as st
import pandas as pd
import duckdb
import logging
import plotly.express as px


# Set up logging
logger = logging.getLogger(__name__)


# ============================================
# Metric Definitions and Tooltips
# ============================================

METRIC_DEFINITIONS = {
    # Revenue Metrics
    "revenue": {
        "name": "Revenue (SGD)",
        "definition": "Total cash received from user top-ups in Singapore Dollars.",
        "calculation": "SUM(topups_sum_sgd) from mart.platform_daily_overview",
        "interpretation": "Higher is better. Track daily/weekly trends to identify growth or decline."
    },
    "topups": {
        "name": "Top-ups",
        "definition": "Number of individual top-up transactions completed.",
        "calculation": "COUNT of payment transactions",
        "interpretation": "Indicates purchase frequency. Compare with revenue to assess average transaction value."
    },
    "avg_transaction": {
        "name": "Average Transaction Value",
        "definition": "Average amount per top-up transaction in SGD.",
        "calculation": "Total Revenue ÷ Number of Top-ups",
        "interpretation": "Higher values suggest users are purchasing larger packages."
    },
    
    # User Metrics
    "new_signups": {
        "name": "New Signups",
        "definition": "Users who registered for an account during the period.",
        "calculation": "COUNT of new user registrations",
        "interpretation": "Measures acquisition effectiveness. Compare with marketing spend."
    },
    "mobile_verified": {
        "name": "Mobile Verified",
        "definition": "Users who completed phone number verification.",
        "calculation": "COUNT of users completing mobile verification",
        "interpretation": "Higher verification rates indicate more committed users."
    },
    "active_users": {
        "name": "Active Users",
        "definition": "Distinct users who performed any activity (VPS action, login, etc.) on a given day.",
        "calculation": "COUNT(DISTINCT user_id) with activity events",
        "interpretation": "Key engagement metric. Track Daily Active Users (DAU) trends."
    },
    "paying_users": {
        "name": "Paying Users",
        "definition": "Users who have made at least one paid top-up (actual monetary purchase).",
        "calculation": "COUNT(DISTINCT user_id) WHERE total_points_earned_paid > 0 (from payment.csv only)",
        "interpretation": "Conversion metric. Only payment.csv transactions count; point.csv TOP_UP does not indicate paid."
    },
    
    # VPS Metrics
    "vps_created": {
        "name": "VPS Created",
        "definition": "Number of new Virtual Private Server instances launched.",
        "calculation": "COUNT of LAUNCH_SERVER activity events",
        "interpretation": "Product usage indicator. More VPS = more engagement and potential revenue."
    },
    "vps_terminated": {
        "name": "VPS Terminated",
        "definition": "Number of VPS instances shut down.",
        "calculation": "COUNT of TERMINATE_SERVER activity events",
        "interpretation": "Monitor for churn signals. High termination may indicate issues."
    },
    "net_vps": {
        "name": "Net VPS Change",
        "definition": "Net change in active VPS instances (created minus terminated).",
        "calculation": "VPS Created - VPS Terminated",
        "interpretation": "Positive = growth, Negative = contraction. Should trend upward."
    },
    "live_vps": {
        "name": "Live VPS",
        "definition": "Currently running VPS instances for a user.",
        "calculation": "Cumulative (Launches - Terminates) per user",
        "interpretation": "Power users typically have multiple live instances."
    },
    
    # Points Metrics
    "points_earned_paid": {
        "name": "Points Earned (Paid)",
        "definition": "Points from actual monetary top-ups only (payment.csv). At 144 points per SGD. point.csv TOP_UP is not paid.",
        "calculation": "SUM(points_delta) WHERE points_source = 'paid' (ledger rows from payment.csv only)",
        "interpretation": "Direct correlation with revenue. 1 SGD = 144 points."
    },
    "points_earned_free": {
        "name": "Points Earned (Free)",
        "definition": "Points from promotions, bonuses, referrals, and mobile verification rewards.",
        "calculation": "SUM(points_delta) WHERE points_source = 'free_claim'",
        "interpretation": "Marketing cost in points. Monitor for abuse patterns."
    },
    "points_spent": {
        "name": "Points Spent",
        "definition": "Points consumed/redeemed for VPS usage.",
        "calculation": "ABS(SUM(points_delta)) WHERE points_delta < 0",
        "interpretation": "Product consumption. Should correlate with VPS activity."
    },
    "points_velocity": {
        "name": "Points Velocity",
        "definition": "Rate at which points are consumed relative to points earned.",
        "calculation": "Points Spent ÷ Points Earned × 100%",
        "interpretation": "50-80% is healthy. >100% means users spending down balances."
    },
    "points_balance": {
        "name": "Points Balance",
        "definition": "Current unspent points in a user's account.",
        "calculation": "SUM(all points_delta) for the user",
        "interpretation": "High balances may indicate inactive users or saving behavior."
    },
}


def get_metric_tooltip(metric_key: str) -> str:
    """Get a formatted tooltip for a metric."""
    if metric_key not in METRIC_DEFINITIONS:
        return ""
    
    m = METRIC_DEFINITIONS[metric_key]
    return f"{m['definition']}\n\n**Calculation:** {m['calculation']}\n\n**Interpretation:** {m['interpretation']}"


# ============================================
# Conversion Funnel Component
# ============================================

def render_conversion_funnel(
    duckdb_path: str, 
    start_date: date, 
    end_date: date,
    prev_start_date: Optional[date] = None,
    prev_end_date: Optional[date] = None
):
    """
    Render the User Conversion Funnel showing the journey from signup to payment.
    
    Funnel Steps:
        1. Sign Up - User creates an account
        2. Mobile Verified - User verifies their phone number
        3. Created VPS - User launches at least one VPS instance
        4. Paid Money - User makes at least one top-up purchase
    
    Shows:
        - Absolute numbers at each step
        - Conversion rate (%) between each step
        - Drop-off rate between steps
        - Comparison with previous period (if available)
    
    Args:
        duckdb_path: Path to DuckDB database
        start_date: Start of current period
        end_date: End of current period
        prev_start_date: Start of comparison period (optional)
        prev_end_date: End of comparison period (optional)
    """
    
    st.header("🔄 User Conversion Funnel")
    st.caption(f"*User journey from signup to payment for {format_date_range_label(start_date, end_date)}*")
    
    with st.expander("ℹ️ Understanding the Funnel", expanded=False):
        st.markdown("""
        **Funnel Stages:**
        
        1. **Sign Up** → User creates an account (entry point)
        2. **Mobile Verified** → User completes phone verification (trust signal)
        3. **Created VPS** → User launches at least one VPS (product adoption)
        4. **Paid Money** → User makes at least one actual monetary top-up (from payment data only; not promo/free credits)
        
        **Metrics Explained:**
        - **Absolute Number**: Total users at each stage
        - **Step Conversion %**: % of users from previous step who reached this step
        - **Overall Conversion %**: % of original signups who reached this step
        - **Drop-off**: Users who didn't proceed to the next step
        
        **Healthy Benchmarks:**
        - Signup → Verified: 60-80%
        - Verified → Created VPS: 30-50%
        - Created VPS → Paid: 20-40%
        """)
    
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')
    
    # Query funnel data for current period
    # Users who signed up in the selected period
    funnel_query = f"""
    WITH period_signups AS (
        -- Users who signed up in the period
        SELECT user_id, registration_ts, mobile_verified
        FROM core.dim_user
        WHERE DATE(registration_ts) >= '{start_str}' 
          AND DATE(registration_ts) <= '{end_str}'
    ),
    funnel_stats AS (
        SELECT 
            -- Step 1: Total signups
            COUNT(*) as signups,
            
            -- Step 2: Mobile verified
            SUM(CASE WHEN mobile_verified THEN 1 ELSE 0 END) as verified,
            
            -- Step 3: Created VPS (at least one launch)
            (SELECT COUNT(DISTINCT ps.user_id) 
             FROM period_signups ps
             JOIN core.user_account_state s ON ps.user_id = s.user_id
             WHERE s.total_launch_count > 0) as created_vps,
            
            -- Step 4: Paid money (has paid points)
            (SELECT COUNT(DISTINCT ps.user_id) 
             FROM period_signups ps
             JOIN core.user_account_state s ON ps.user_id = s.user_id
             WHERE s.total_points_earned_paid > 0) as paid
             
        FROM period_signups
    )
    SELECT * FROM funnel_stats
    """
    
    current_df = load_app_data(duckdb_path, funnel_query, suppress_error=True)
    
    if current_df is None or current_df.empty:
        st.warning(f"No funnel data available for {format_date_range_label(start_date, end_date)}. Run the User Logs ETL to populate data.")
        return
    
    # Extract current period values
    current = current_df.iloc[0]
    signups = int(current['signups']) if pd.notna(current['signups']) else 0
    verified = int(current['verified']) if pd.notna(current['verified']) else 0
    created_vps = int(current['created_vps']) if pd.notna(current['created_vps']) else 0
    paid = int(current['paid']) if pd.notna(current['paid']) else 0
    
    if signups == 0:
        st.info(f"No signups found for {format_date_range_label(start_date, end_date)}.")
        return
    
    # Query comparison period if available
    prev_signups = None
    prev_verified = None
    prev_created_vps = None
    prev_paid = None
    has_comparison = False
    
    if prev_start_date and prev_end_date:
        prev_start_str = prev_start_date.strftime('%Y-%m-%d')
        prev_end_str = prev_end_date.strftime('%Y-%m-%d')
        
        prev_funnel_query = f"""
        WITH period_signups AS (
            SELECT user_id, registration_ts, mobile_verified
            FROM core.dim_user
            WHERE DATE(registration_ts) >= '{prev_start_str}' 
              AND DATE(registration_ts) <= '{prev_end_str}'
        ),
        funnel_stats AS (
            SELECT 
                COUNT(*) as signups,
                SUM(CASE WHEN mobile_verified THEN 1 ELSE 0 END) as verified,
                (SELECT COUNT(DISTINCT ps.user_id) 
                 FROM period_signups ps
                 JOIN core.user_account_state s ON ps.user_id = s.user_id
                 WHERE s.total_launch_count > 0) as created_vps,
                (SELECT COUNT(DISTINCT ps.user_id) 
                 FROM period_signups ps
                 JOIN core.user_account_state s ON ps.user_id = s.user_id
                 WHERE s.total_points_earned_paid > 0) as paid
            FROM period_signups
        )
        SELECT * FROM funnel_stats
        """
        
        prev_df = load_app_data(duckdb_path, prev_funnel_query, suppress_error=True)
        
        if prev_df is not None and not prev_df.empty:
            prev_row = prev_df.iloc[0]
            prev_signups = int(prev_row['signups']) if pd.notna(prev_row['signups']) else 0
            prev_verified = int(prev_row['verified']) if pd.notna(prev_row['verified']) else 0
            prev_created_vps = int(prev_row['created_vps']) if pd.notna(prev_row['created_vps']) else 0
            prev_paid = int(prev_row['paid']) if pd.notna(prev_row['paid']) else 0
            has_comparison = prev_signups > 0
    
    # Build funnel data structure
    funnel_steps = [
        {
            'step': 1,
            'name': 'Sign Up',
            'icon': '👤',
            'count': signups,
            'prev_count': prev_signups,
            'color': '#4A90D9'
        },
        {
            'step': 2,
            'name': 'Mobile Verified',
            'icon': '📱',
            'count': verified,
            'prev_count': prev_verified,
            'color': '#67B26F'
        },
        {
            'step': 3,
            'name': 'Created VPS',
            'icon': '🖥️',
            'count': created_vps,
            'prev_count': prev_created_vps,
            'color': '#F4A261'
        },
        {
            'step': 4,
            'name': 'Paid Money',
            'icon': '💰',
            'count': paid,
            'prev_count': prev_paid,
            'color': '#E76F51'
        }
    ]
    
    # Calculate conversion rates
    for i, step in enumerate(funnel_steps):
        # Overall conversion from signup
        step['overall_rate'] = (step['count'] / signups * 100) if signups > 0 else 0
        
        # Step-to-step conversion
        if i == 0:
            step['step_rate'] = 100.0
            step['dropoff'] = 0
            step['dropoff_pct'] = 0
        else:
            prev_step = funnel_steps[i - 1]
            step['step_rate'] = (step['count'] / prev_step['count'] * 100) if prev_step['count'] > 0 else 0
            step['dropoff'] = prev_step['count'] - step['count']
            step['dropoff_pct'] = 100 - step['step_rate']
        
        # Comparison period rates
        if has_comparison and step['prev_count'] is not None:
            step['prev_overall_rate'] = (step['prev_count'] / prev_signups * 100) if prev_signups > 0 else 0
            step['count_change'] = calculate_percentage_change(step['count'], step['prev_count'])
            step['rate_change'] = step['overall_rate'] - step['prev_overall_rate']
        else:
            step['prev_overall_rate'] = None
            step['count_change'] = None
            step['rate_change'] = None
    
    # ========================================
    # Visual Funnel Display
    # ========================================
    
    # Create funnel visualization using columns
    st.markdown("### Funnel Overview")
    
    cols = st.columns(4)
    
    for i, step in enumerate(funnel_steps):
        with cols[i]:
            # Step header
            st.markdown(f"### {step['icon']} Step {step['step']}")
            st.markdown(f"**{step['name']}**")
            
            # Main count metric with comparison
            if has_comparison and step['count_change'] is not None:
                delta_str = f"{step['count_change']:+.1f}% vs prev"
            else:
                delta_str = None
            
            st.metric(
                label="Users",
                value=f"{step['count']:,}",
                delta=delta_str,
                help=f"Total users who reached '{step['name']}' stage"
            )
            
            # Conversion rates
            if i > 0:
                st.markdown(f"**Step Conv:** {step['step_rate']:.1f}%")
                st.markdown(f"**Overall:** {step['overall_rate']:.1f}%")
                
                # Drop-off indicator
                if step['dropoff'] > 0:
                    st.caption(f"📉 {step['dropoff']:,} dropped ({step['dropoff_pct']:.1f}%)")
            else:
                st.markdown("**Entry Point**")
                st.markdown(f"**100%** of funnel")
    
    # ========================================
    # Funnel Flow Arrows with Drop-off
    # ========================================
    
    st.divider()
    st.markdown("### Step-by-Step Conversion Rates")
    
    # Create flow visualization
    flow_cols = st.columns(7)  # 4 steps + 3 arrows
    
    for i, step in enumerate(funnel_steps):
        col_idx = i * 2  # Position for step (0, 2, 4, 6)
        
        with flow_cols[col_idx]:
            # Step box
            st.markdown(f"""
            <div style="
                background-color: {step['color']}20;
                border: 2px solid {step['color']};
                border-radius: 10px;
                padding: 15px;
                text-align: center;
                min-height: 120px;
            ">
                <div style="font-size: 24px;">{step['icon']}</div>
                <div style="font-weight: bold; margin: 5px 0;">{step['name']}</div>
                <div style="font-size: 20px; font-weight: bold;">{step['count']:,}</div>
                <div style="font-size: 12px; color: gray;">{step['overall_rate']:.1f}% of signups</div>
            </div>
            """, unsafe_allow_html=True)
        
        # Arrow between steps
        if i < len(funnel_steps) - 1:
            next_step = funnel_steps[i + 1]
            with flow_cols[col_idx + 1]:
                st.markdown(f"""
                <div style="
                    text-align: center;
                    padding-top: 40px;
                ">
                    <div style="font-size: 24px;">→</div>
                    <div style="font-size: 14px; font-weight: bold; color: {'#67B26F' if next_step['step_rate'] >= 50 else '#E76F51'};">
                        {next_step['step_rate']:.1f}%
                    </div>
                    <div style="font-size: 10px; color: gray;">
                        -{next_step['dropoff']:,}
                    </div>
                </div>
                """, unsafe_allow_html=True)
    
    # ========================================
    # Comparison Table (if available)
    # ========================================
    
    st.divider()
    
    if has_comparison:
        st.markdown(f"### 📊 Period Comparison")
        st.caption(f"Current: {format_date_range_label(start_date, end_date)} vs Previous: {format_date_range_label(prev_start_date, prev_end_date)}")
        
        # Build comparison table
        comparison_data = []
        for step in funnel_steps:
            comparison_data.append({
                'Stage': f"{step['icon']} {step['name']}",
                'Current': f"{step['count']:,}",
                'Previous': f"{step['prev_count']:,}" if step['prev_count'] is not None else "-",
                'Change': f"{step['count_change']:+.1f}%" if step['count_change'] is not None else "-",
                'Current Rate': f"{step['overall_rate']:.1f}%",
                'Previous Rate': f"{step['prev_overall_rate']:.1f}%" if step['prev_overall_rate'] is not None else "-",
                'Rate Δ': f"{step['rate_change']:+.1f}pp" if step['rate_change'] is not None else "-"
            })
        
        comparison_df = pd.DataFrame(comparison_data)
        
        st.dataframe(
            comparison_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Stage": st.column_config.TextColumn("Funnel Stage"),
                "Current": st.column_config.TextColumn("Current Period", help="Users in current date range"),
                "Previous": st.column_config.TextColumn("Previous Period", help="Users in comparison date range"),
                "Change": st.column_config.TextColumn("Count Change", help="% change in absolute numbers"),
                "Current Rate": st.column_config.TextColumn("Current Conv %", help="Conversion rate from signup (current)"),
                "Previous Rate": st.column_config.TextColumn("Previous Conv %", help="Conversion rate from signup (previous)"),
                "Rate Δ": st.column_config.TextColumn("Rate Change", help="Percentage point change in conversion rate")
            }
        )
        
        # Insights based on comparison
        st.markdown("**📈 Key Insights:**")
        
        insights = []
        
        # Check signup trend
        if funnel_steps[0]['count_change'] is not None:
            if funnel_steps[0]['count_change'] > 10:
                insights.append(f"✅ Signups increased by {funnel_steps[0]['count_change']:.1f}% - acquisition is improving")
            elif funnel_steps[0]['count_change'] < -10:
                insights.append(f"⚠️ Signups decreased by {abs(funnel_steps[0]['count_change']):.1f}% - check acquisition channels")
        
        # Check conversion rate changes
        for step in funnel_steps[1:]:
            if step['rate_change'] is not None:
                if step['rate_change'] > 5:
                    insights.append(f"✅ {step['name']} conversion improved by {step['rate_change']:.1f}pp")
                elif step['rate_change'] < -5:
                    insights.append(f"⚠️ {step['name']} conversion dropped by {abs(step['rate_change']):.1f}pp - needs attention")
        
        if insights:
            for insight in insights:
                st.markdown(f"- {insight}")
        else:
            st.markdown("- Funnel metrics are relatively stable compared to the previous period")
    
    else:
        st.info("📊 **Comparison not available.** Enable comparison in the date picker above to see period-over-period changes.")
    
    # ========================================
    # Funnel Health Summary
    # ========================================
    
    st.divider()
    st.markdown("### 🎯 Funnel Health Summary")
    
    summary_cols = st.columns(4)
    
    with summary_cols[0]:
        signup_to_verified = (verified / signups * 100) if signups > 0 else 0
        health = "🟢" if signup_to_verified >= 60 else "🟡" if signup_to_verified >= 40 else "🔴"
        st.metric(
            "Signup → Verified",
            f"{health} {signup_to_verified:.1f}%",
            help="Target: 60-80%. Users completing mobile verification."
        )
    
    with summary_cols[1]:
        verified_to_vps = (created_vps / verified * 100) if verified > 0 else 0
        health = "🟢" if verified_to_vps >= 30 else "🟡" if verified_to_vps >= 15 else "🔴"
        st.metric(
            "Verified → VPS",
            f"{health} {verified_to_vps:.1f}%",
            help="Target: 30-50%. Verified users who try the product."
        )
    
    with summary_cols[2]:
        vps_to_paid = (paid / created_vps * 100) if created_vps > 0 else 0
        health = "🟢" if vps_to_paid >= 20 else "🟡" if vps_to_paid >= 10 else "🔴"
        st.metric(
            "VPS → Paid",
            f"{health} {vps_to_paid:.1f}%",
            help="Target: 20-40%. VPS users who convert to paying."
        )
    
    with summary_cols[3]:
        overall_conversion = (paid / signups * 100) if signups > 0 else 0
        health = "🟢" if overall_conversion >= 5 else "🟡" if overall_conversion >= 2 else "🔴"
        st.metric(
            "Overall Conversion",
            f"{health} {overall_conversion:.1f}%",
            help="Target: 5-15%. Total signup to paid conversion."
        )


# ============================================
# Data Loading Helpers
# ============================================

def load_app_data(duckdb_path: str, query: str, suppress_error: bool = False) -> Optional[pd.DataFrame]:
    """
    Load app analytics data from DuckDB with error handling.
    
    Args:
        duckdb_path: Path to DuckDB database file
        query: SQL query to execute
        suppress_error: If True, don't show error messages (for optional tables)
    
    Returns:
        DataFrame with query results, or None if error occurs
    """
    try:
        conn = duckdb.connect(duckdb_path, read_only=True)
        df = conn.execute(query).fetchdf()
        conn.close()
        return df
    except Exception as e:
        if not suppress_error:
            error_msg = str(e).lower()
            if "does not exist" in error_msg or "not found" in error_msg:
                logger.warning(f"Table not found: {e}")
            else:
                st.error(f"Query error: {e}")
        return None


def check_mart_tables_exist(duckdb_path: str) -> Dict[str, bool]:
    """
    Check if the required mart/core tables exist in the database.
    
    Returns:
        Dictionary mapping table names to availability (True/False)
    """
    tables = {
        'mart.platform_daily_overview': False,
        'mart.platform_weekly_overview': False,
        'mart.user_daily_activity': False,
        'mart.user_daily_points': False,
        'core.user_account_state': False,
        'core.dim_user': False,
        'core.fact_points_ledger': False,
        'core.fact_user_activity': False,
    }
    
    try:
        conn = duckdb.connect(duckdb_path, read_only=True)
        
        for table in tables.keys():
            try:
                result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                tables[table] = result[0] > 0 if result else False
            except Exception:
                tables[table] = False
        
        conn.close()
    except Exception as e:
        logger.error(f"Error checking mart tables: {e}")
    
    return tables


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """Safely divide two numbers, handling zero division."""
    if denominator is None or denominator == 0:
        return default
    if numerator is None:
        return default
    return numerator / denominator


def calculate_percentage_change(current: float, previous: float) -> Optional[float]:
    """Calculate percentage change between two periods."""
    if previous is None or previous == 0 or current is None:
        return None
    return ((current - previous) / abs(previous)) * 100


def format_currency(value: float, currency: str = "SGD") -> str:
    """Format a number as currency."""
    if value is None:
        return "-"
    return f"${value:,.2f}"


def format_points(value: int) -> str:
    """Format points with thousands separator."""
    if value is None:
        return "-"
    return f"{value:,}"


def format_date_range_label(start_date: date, end_date: date) -> str:
    """Format date range for display."""
    return f"{start_date.strftime('%b %d, %Y')} - {end_date.strftime('%b %d, %Y')}"


# ============================================
# Component 1: Platform KPIs Overview
# ============================================

def render_platform_kpis(duckdb_path: str, start_date: date, end_date: date):
    """
    Render Platform KPIs - key metrics summary cards with explanations.
    """
    
    st.header("📊 Platform KPIs")
    st.caption(f"*Key app metrics for {format_date_range_label(start_date, end_date)}*")
    
    # Metric explainer
    with st.expander("ℹ️ Understanding These Metrics", expanded=False):
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("""
            **Revenue Metrics:**
            - **Revenue (SGD)**: Total cash from top-ups
            - **Top-ups**: Number of purchase transactions
            - **Payers**: Unique users who made payments
            
            **User Metrics:**
            - **New Signups**: New registrations
            - **Mobile Verified**: Users with verified phone
            """)
        with col2:
            st.markdown("""
            **VPS Metrics:**
            - **VPS Created**: New servers launched
            - **Net VPS**: Created minus terminated
            
            **Points Economy:**
            - **Points (Paid)**: From top-ups (144 pts/SGD)
            - **Points (Free)**: From promos/bonuses
            - **Points Spent**: Consumed for VPS usage
            """)
    
    # Format dates for SQL
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')
    
    # Query platform overview for the period
    kpi_query = f"""
    SELECT 
        COALESCE(SUM(new_signups), 0) as total_signups,
        COALESCE(SUM(mobile_verified_new), 0) as total_verified,
        COALESCE(SUM(active_users), 0) as total_active_user_days,
        COALESCE(SUM(new_vps_created), 0) as total_vps_created,
        COALESCE(SUM(vps_terminated), 0) as total_vps_terminated,
        COALESCE(SUM(net_vps_change), 0) as total_net_vps,
        COALESCE(SUM(topups_count), 0) as total_topups,
        COALESCE(SUM(topups_sum_sgd), 0) as total_revenue,
        COALESCE(SUM(payer_count), 0) as total_payer_days,
        COALESCE(SUM(points_earned_paid), 0) as total_points_paid,
        COALESCE(SUM(points_earned_free), 0) as total_points_free,
        COALESCE(SUM(points_spent), 0) as total_points_spent,
        COUNT(DISTINCT activity_date) as days_count
    FROM mart.platform_daily_overview
    WHERE activity_date >= '{start_str}' AND activity_date <= '{end_str}'
    """
    
    kpi_df = load_app_data(duckdb_path, kpi_query)
    
    if kpi_df is None or kpi_df.empty or kpi_df.iloc[0]['days_count'] == 0:
        st.warning(f"No platform data available for {format_date_range_label(start_date, end_date)}. Adjust the date range or run the User Logs ETL.")
        return
    
    row = kpi_df.iloc[0]
    
    # Extract metrics with safe handling
    total_revenue = float(row['total_revenue']) if pd.notna(row['total_revenue']) else 0
    total_signups = int(row['total_signups']) if pd.notna(row['total_signups']) else 0
    total_verified = int(row['total_verified']) if pd.notna(row['total_verified']) else 0
    total_vps_created = int(row['total_vps_created']) if pd.notna(row['total_vps_created']) else 0
    total_vps_terminated = int(row['total_vps_terminated']) if pd.notna(row['total_vps_terminated']) else 0
    total_net_vps = int(row['total_net_vps']) if pd.notna(row['total_net_vps']) else 0
    total_topups = int(row['total_topups']) if pd.notna(row['total_topups']) else 0
    total_points_paid = int(row['total_points_paid']) if pd.notna(row['total_points_paid']) else 0
    total_points_free = int(row['total_points_free']) if pd.notna(row['total_points_free']) else 0
    total_points_spent = int(row['total_points_spent']) if pd.notna(row['total_points_spent']) else 0
    days_count = int(row['days_count']) if pd.notna(row['days_count']) else 1
    
    # Display KPIs in 6 columns
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    
    with col1:
        st.metric(
            label="💰 Revenue (SGD)",
            value=format_currency(total_revenue),
            delta=f"${total_revenue/days_count:,.0f}/day avg" if days_count > 0 else None,
            help=get_metric_tooltip("revenue"),
        )
    
    with col2:
        st.metric(
            label="👥 New Signups",
            value=f"{total_signups:,}",
            delta=f"{total_signups/days_count:.1f}/day" if days_count > 0 else None,
            help=get_metric_tooltip("new_signups"),
        )
    
    with col3:
        verification_rate = (total_verified / total_signups * 100) if total_signups > 0 else 0
        st.metric(
            label="📱 Mobile Verified",
            value=f"{total_verified:,}",
            delta=f"{verification_rate:.1f}% of signups",
            help=get_metric_tooltip("mobile_verified"),
        )
    
    with col4:
        st.metric(
            label="🖥️ VPS Created",
            value=f"{total_vps_created:,}",
            help=get_metric_tooltip("vps_created"),
        )
    
    with col5:
        vps_delta = "+" if total_net_vps > 0 else ""
        st.metric(
            label="📊 Net VPS Change",
            value=f"{vps_delta}{total_net_vps:,}",
            delta=f"{total_vps_terminated:,} terminated",
            delta_color="inverse",
            help=get_metric_tooltip("net_vps"),
        )
    
    with col6:
        avg_txn = total_revenue / total_topups if total_topups > 0 else 0
        st.metric(
            label="💳 Top-ups",
            value=f"{total_topups:,}",
            delta=f"${avg_txn:.0f} avg",
            help=get_metric_tooltip("topups"),
        )
    
    # Second row: Points Economy
    st.divider()
    st.subheader("💎 Points Economy")
    
    col1, col2, col3, col4 = st.columns(4)
    
    total_earned = total_points_paid + total_points_free
    
    with col1:
        st.metric(
            label="Points Earned (Paid)",
            value=format_points(total_points_paid),
            delta=f"{(total_points_paid/total_earned*100):.1f}% of total" if total_earned > 0 else None,
            help=get_metric_tooltip("points_earned_paid"),
        )
    
    with col2:
        st.metric(
            label="Points Earned (Free)",
            value=format_points(total_points_free),
            delta=f"{(total_points_free/total_earned*100):.1f}% of total" if total_earned > 0 else None,
            help=get_metric_tooltip("points_earned_free"),
        )
    
    with col3:
        st.metric(
            label="Points Spent",
            value=format_points(total_points_spent),
            help=get_metric_tooltip("points_spent"),
        )
    
    with col4:
        velocity = (total_points_spent / total_earned * 100) if total_earned > 0 else 0
        net_points = total_earned - total_points_spent
        st.metric(
            label="Points Velocity",
            value=f"{velocity:.1f}%",
            delta=f"Net: {format_points(net_points)}",
            help=get_metric_tooltip("points_velocity"),
        )


# ============================================
# Component 2: Revenue Trends
# ============================================

def render_revenue_trends(duckdb_path: str, start_date: date, end_date: date):
    """
    Render Revenue Trends chart with proper legends and date filtering.
    """
    
    st.header("💰 Revenue Trends")
    st.caption(f"*Daily top-up revenue and transactions for {format_date_range_label(start_date, end_date)}*")
    
    # Metric explainer
    with st.expander("ℹ️ Chart Guide", expanded=False):
        st.markdown("""
        **How to read these charts:**
        
        - **Revenue Tab**: Blue line shows daily revenue in SGD. Look for trends and spikes.
        - **Transactions Tab**: Green bars show number of top-ups per day.
        - **Combined Tab**: Dual-axis view comparing revenue (left axis) with transaction count (right axis).
        
        **Key insights to look for:**
        - Revenue spikes may indicate promotions or viral moments
        - Consistent daily patterns suggest stable user base
        - Divergence between revenue and transactions indicates changing average order value
        """)
    
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')
    
    trend_query = f"""
    SELECT 
        activity_date as date,
        COALESCE(topups_sum_sgd, 0) as revenue,
        COALESCE(topups_count, 0) as topups,
        COALESCE(payer_count, 0) as payers
    FROM mart.platform_daily_overview
    WHERE activity_date >= '{start_str}' AND activity_date <= '{end_str}'
    ORDER BY activity_date
    """
    
    trend_df = load_app_data(duckdb_path, trend_query)
    
    if trend_df is None or trend_df.empty:
        st.info(f"No revenue data available for {format_date_range_label(start_date, end_date)}.")
        return
    
    trend_df['date'] = pd.to_datetime(trend_df['date'])
    trend_df['avg_transaction'] = trend_df.apply(
        lambda r: r['revenue'] / r['topups'] if r['topups'] > 0 else 0, 
        axis=1
    )
    
    tab1, tab2, tab3 = st.tabs(["📈 Revenue", "🧾 Transactions", "📊 Combined"])
    
    with tab1:
        import plotly.express as px
        
        fig = px.line(
            trend_df,
            x='date',
            y='revenue',
            title=f'Daily Revenue (SGD) - {format_date_range_label(start_date, end_date)}',
            labels={'date': 'Date', 'revenue': 'Revenue (SGD)'}
        )
        fig.update_traces(
            line_color='#2E86AB', 
            line_width=2,
            name='Daily Revenue',
            hovertemplate='<b>%{x|%b %d, %Y}</b><br>Revenue: $%{y:,.2f}<extra></extra>'
        )
        fig.update_layout(
            margin=dict(l=0, r=0, t=40, b=0),
            height=350,
            hovermode='x unified',
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        st.plotly_chart(fig, use_container_width=True)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Revenue", format_currency(trend_df['revenue'].sum()), 
                     help="Sum of all revenue in the selected period")
        with col2:
            st.metric("Daily Average", format_currency(trend_df['revenue'].mean()),
                     help="Average daily revenue")
        with col3:
            peak_date = trend_df.loc[trend_df['revenue'].idxmax(), 'date']
            st.metric("Peak Day", f"{format_currency(trend_df['revenue'].max())}",
                     delta=peak_date.strftime('%b %d'),
                     help="Highest revenue day in the period")
    
    with tab2:
        import plotly.express as px
        
        fig = px.bar(
            trend_df,
            x='date',
            y='topups',
            title=f'Daily Top-up Count - {format_date_range_label(start_date, end_date)}',
            labels={'date': 'Date', 'topups': 'Number of Transactions'}
        )
        fig.update_traces(
            marker_color='#67B26F',
            name='Transactions',
            hovertemplate='<b>%{x|%b %d, %Y}</b><br>Transactions: %{y:,}<extra></extra>'
        )
        fig.update_layout(
            margin=dict(l=0, r=0, t=40, b=0),
            height=350,
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        st.plotly_chart(fig, use_container_width=True)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Transactions", f"{int(trend_df['topups'].sum()):,}",
                     help="Total number of top-up transactions")
        with col2:
            avg_val = trend_df['revenue'].sum() / trend_df['topups'].sum() if trend_df['topups'].sum() > 0 else 0
            st.metric("Avg Transaction Value", format_currency(avg_val),
                     help="Average revenue per transaction")
        with col3:
            st.metric("Unique Payer Days", f"{int(trend_df['payers'].sum()):,}",
                     help="Sum of unique payers across all days (same user on different days counts multiple times)")
    
    with tab3:
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots
        
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        
        fig.add_trace(
            go.Scatter(
                x=trend_df['date'],
                y=trend_df['revenue'],
                name='Revenue (SGD)',
                line=dict(color='#2E86AB', width=2),
                hovertemplate='<b>%{x|%b %d}</b><br>Revenue: $%{y:,.2f}<extra></extra>'
            ),
            secondary_y=False
        )
        
        fig.add_trace(
            go.Bar(
                x=trend_df['date'],
                y=trend_df['topups'],
                name='Transactions',
                marker_color='#67B26F',
                opacity=0.6,
                hovertemplate='<b>%{x|%b %d}</b><br>Transactions: %{y:,}<extra></extra>'
            ),
            secondary_y=True
        )
        
        fig.update_layout(
            title=f'Revenue vs Transactions - {format_date_range_label(start_date, end_date)}',
            margin=dict(l=0, r=0, t=40, b=0),
            height=400,
            legend=dict(
                orientation="h", 
                yanchor="bottom", 
                y=1.02, 
                xanchor="right", 
                x=1,
                bgcolor="rgba(255,255,255,0.8)"
            ),
            hovermode='x unified'
        )
        fig.update_yaxes(title_text="Revenue (SGD)", secondary_y=False, tickprefix="$")
        fig.update_yaxes(title_text="Transactions", secondary_y=True)
        
        st.plotly_chart(fig, use_container_width=True)
        
        st.caption("**Legend:** Blue line = Revenue (left axis) | Green bars = Transactions (right axis)")


# ============================================
# Component 3: User Activity Trends
# ============================================

def render_user_activity_trends(duckdb_path: str, start_date: date, end_date: date):
    """
    Render User Activity Trends with proper legends and date filtering.
    """
    
    st.header("👥 User Activity Trends")
    st.caption(f"*Daily user engagement and VPS activity for {format_date_range_label(start_date, end_date)}*")
    
    with st.expander("ℹ️ Chart Guide", expanded=False):
        st.markdown("""
        **Understanding User Activity:**
        
        - **Active Users** (blue area): Users who performed any action that day
        - **New Signups** (green bars): New registrations on each day
        - **VPS Created** (blue bars): New server instances launched
        - **VPS Terminated** (red bars): Servers shut down
        
        **What to watch for:**
        - Growing active users indicates healthy engagement
        - Net VPS should trend positive for growth
        - High terminations may signal user churn
        """)
    
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')
    
    activity_query = f"""
    SELECT 
        activity_date as date,
        COALESCE(new_signups, 0) as signups,
        COALESCE(active_users, 0) as active_users,
        COALESCE(new_vps_created, 0) as vps_created,
        COALESCE(vps_terminated, 0) as vps_terminated,
        COALESCE(net_vps_change, 0) as net_vps
    FROM mart.platform_daily_overview
    WHERE activity_date >= '{start_str}' AND activity_date <= '{end_str}'
    ORDER BY activity_date
    """
    
    activity_df = load_app_data(duckdb_path, activity_query)
    
    if activity_df is None or activity_df.empty:
        st.info(f"No activity data available for {format_date_range_label(start_date, end_date)}.")
        return
    
    activity_df['date'] = pd.to_datetime(activity_df['date'])
    
    tab1, tab2, tab3 = st.tabs(["👥 Users", "🖥️ VPS Activity", "📊 Cumulative"])
    
    with tab1:
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots
        
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        
        fig.add_trace(
            go.Scatter(
                x=activity_df['date'],
                y=activity_df['active_users'],
                name='Active Users',
                fill='tozeroy',
                line=dict(color='#4A90D9', width=2),
                fillcolor='rgba(74, 144, 217, 0.3)',
                hovertemplate='<b>%{x|%b %d}</b><br>Active Users: %{y:,}<extra></extra>'
            ),
            secondary_y=False
        )
        
        fig.add_trace(
            go.Bar(
                x=activity_df['date'],
                y=activity_df['signups'],
                name='New Signups',
                marker_color='#67B26F',
                opacity=0.7,
                hovertemplate='<b>%{x|%b %d}</b><br>New Signups: %{y:,}<extra></extra>'
            ),
            secondary_y=True
        )
        
        fig.update_layout(
            title=f'Active Users & New Signups - {format_date_range_label(start_date, end_date)}',
            margin=dict(l=0, r=0, t=40, b=0),
            height=400,
            legend=dict(
                orientation="h", 
                yanchor="bottom", 
                y=1.02, 
                xanchor="right", 
                x=1,
                bgcolor="rgba(255,255,255,0.8)"
            ),
            hovermode='x unified'
        )
        fig.update_yaxes(title_text="Active Users", secondary_y=False)
        fig.update_yaxes(title_text="New Signups", secondary_y=True)
        
        st.plotly_chart(fig, use_container_width=True)
        
        st.caption("**Legend:** Blue area = Active Users (left axis) | Green bars = New Signups (right axis)")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total New Signups", f"{int(activity_df['signups'].sum()):,}",
                     help=get_metric_tooltip("new_signups"))
        with col2:
            st.metric("Avg Daily Active", f"{activity_df['active_users'].mean():,.0f}",
                     help=get_metric_tooltip("active_users"))
        with col3:
            st.metric("Peak Active Users", f"{int(activity_df['active_users'].max()):,}")
    
    with tab2:
        import plotly.graph_objects as go
        
        fig = go.Figure()
        
        fig.add_trace(go.Bar(
            x=activity_df['date'],
            y=activity_df['vps_created'],
            name='VPS Created',
            marker_color='#4A90D9',
            hovertemplate='<b>%{x|%b %d}</b><br>Created: %{y:,}<extra></extra>'
        ))
        
        fig.add_trace(go.Bar(
            x=activity_df['date'],
            y=activity_df['vps_terminated'],
            name='VPS Terminated',
            marker_color='#E76F51',
            hovertemplate='<b>%{x|%b %d}</b><br>Terminated: %{y:,}<extra></extra>'
        ))
        
        fig.update_layout(
            title=f'VPS Launch vs Terminate - {format_date_range_label(start_date, end_date)}',
            barmode='group',
            margin=dict(l=0, r=0, t=40, b=0),
            height=400,
            legend=dict(
                orientation="h", 
                yanchor="bottom", 
                y=1.02, 
                xanchor="right", 
                x=1,
                bgcolor="rgba(255,255,255,0.8)"
            ),
            xaxis_title="Date",
            yaxis_title="Count"
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        st.caption("**Legend:** Blue bars = VPS Created | Red bars = VPS Terminated")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Created", f"{int(activity_df['vps_created'].sum()):,}",
                     help=get_metric_tooltip("vps_created"))
        with col2:
            st.metric("Total Terminated", f"{int(activity_df['vps_terminated'].sum()):,}",
                     help=get_metric_tooltip("vps_terminated"))
        with col3:
            net = int(activity_df['net_vps'].sum())
            st.metric("Net Change", f"{'+' if net > 0 else ''}{net:,}",
                     help=get_metric_tooltip("net_vps"))
    
    with tab3:
        import plotly.graph_objects as go
        
        activity_df['cumulative_signups'] = activity_df['signups'].cumsum()
        activity_df['cumulative_net_vps'] = activity_df['net_vps'].cumsum()
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=activity_df['date'],
            y=activity_df['cumulative_signups'],
            name='Cumulative Signups',
            line=dict(color='#67B26F', width=2),
            hovertemplate='<b>%{x|%b %d}</b><br>Cumulative Signups: %{y:,}<extra></extra>'
        ))
        
        fig.add_trace(go.Scatter(
            x=activity_df['date'],
            y=activity_df['cumulative_net_vps'],
            name='Cumulative Net VPS',
            line=dict(color='#4A90D9', width=2),
            hovertemplate='<b>%{x|%b %d}</b><br>Cumulative Net VPS: %{y:,}<extra></extra>'
        ))
        
        fig.update_layout(
            title=f'Cumulative Growth - {format_date_range_label(start_date, end_date)}',
            margin=dict(l=0, r=0, t=40, b=0),
            height=400,
            legend=dict(
                orientation="h", 
                yanchor="bottom", 
                y=1.02, 
                xanchor="right", 
                x=1,
                bgcolor="rgba(255,255,255,0.8)"
            ),
            xaxis_title="Date",
            yaxis_title="Cumulative Count"
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        st.caption("**Legend:** Green line = Cumulative New Signups | Blue line = Cumulative Net VPS Change")


# ============================================
# Component 4: Points Economy Analysis
# ============================================

def render_points_economy(duckdb_path: str, start_date: date, end_date: date):
    """
    Render Points Economy Analysis with proper legends and date filtering.
    """
    
    st.header("💎 Points Economy")
    st.caption(f"*Points flow analysis for {format_date_range_label(start_date, end_date)}*")
    
    with st.expander("ℹ️ Understanding Points Economy", expanded=False):
        st.markdown("""
        **Points System Overview:**
        
        - **Exchange Rate**: 1 SGD = 144 Points
        - **Points Sources**: Paid top-ups, promotions, referral bonuses, mobile verification
        - **Points Usage**: Consumed when running VPS instances
        
        **Key Metrics:**
        - **Velocity**: % of earned points that are spent. 50-80% is healthy.
        - **Net Flow**: Positive = points accumulating, Negative = users spending down balances
        
        **Warning Signs:**
        - Very high free points % may indicate promotion abuse
        - Velocity >100% means users are spending more than earning
        """)
    
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')
    
    points_query = f"""
    SELECT 
        activity_date as date,
        COALESCE(points_earned_paid, 0) as paid,
        COALESCE(points_earned_free, 0) as free,
        COALESCE(points_spent, 0) as spent,
        COALESCE(net_points_delta, 0) as net
    FROM mart.platform_daily_overview
    WHERE activity_date >= '{start_str}' AND activity_date <= '{end_str}'
    ORDER BY activity_date
    """
    
    points_df = load_app_data(duckdb_path, points_query)
    
    if points_df is None or points_df.empty:
        st.info(f"No points data available for {format_date_range_label(start_date, end_date)}.")
        return
    
    points_df['date'] = pd.to_datetime(points_df['date'])
    
    # Summary metrics
    total_paid = int(points_df['paid'].sum())
    total_free = int(points_df['free'].sum())
    total_spent = int(points_df['spent'].sum())
    total_earned = total_paid + total_free
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Earned", format_points(total_earned),
                 help="Sum of all points earned (paid + free)")
    with col2:
        pct_paid = (total_paid / total_earned * 100) if total_earned > 0 else 0
        st.metric("% from Paid", f"{pct_paid:.1f}%", 
                 help="Percentage of points from paid top-ups (vs free/promos)")
    with col3:
        st.metric("Total Spent", format_points(total_spent),
                 help=get_metric_tooltip("points_spent"))
    with col4:
        velocity = (total_spent / total_earned * 100) if total_earned > 0 else 0
        velocity_status = "🟢" if 50 <= velocity <= 80 else "🟡" if velocity < 50 else "🔴"
        st.metric("Velocity", f"{velocity_status} {velocity:.1f}%", 
                 help=get_metric_tooltip("points_velocity"))
    
    st.divider()
    
    tab1, tab2, tab3 = st.tabs(["📈 Trends", "🥧 Sources", "📊 Net Flow"])
    
    with tab1:
        import plotly.graph_objects as go
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=points_df['date'],
            y=points_df['paid'] + points_df['free'],
            name='Points Earned',
            fill='tozeroy',
            line=dict(color='#67B26F', width=2),
            fillcolor='rgba(103, 178, 111, 0.3)',
            hovertemplate='<b>%{x|%b %d}</b><br>Earned: %{y:,.0f}<extra></extra>'
        ))
        
        fig.add_trace(go.Scatter(
            x=points_df['date'],
            y=points_df['spent'],
            name='Points Spent',
            line=dict(color='#E76F51', width=2, dash='dot'),
            hovertemplate='<b>%{x|%b %d}</b><br>Spent: %{y:,.0f}<extra></extra>'
        ))
        
        fig.update_layout(
            title=f'Points Earned vs Spent - {format_date_range_label(start_date, end_date)}',
            margin=dict(l=0, r=0, t=40, b=0),
            height=400,
            legend=dict(
                orientation="h", 
                yanchor="bottom", 
                y=1.02, 
                xanchor="right", 
                x=1,
                bgcolor="rgba(255,255,255,0.8)"
            ),
            hovermode='x unified',
            yaxis_title='Points',
            xaxis_title='Date'
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        st.caption("**Legend:** Green area = Points Earned (Paid + Free) | Red dashed line = Points Spent")
    
    with tab2:
        import plotly.graph_objects as go
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            # Donut chart for points sources
            fig = go.Figure(data=[go.Pie(
                labels=['Paid Top-ups', 'Free/Promos'],
                values=[total_paid, total_free],
                hole=0.4,
                marker_colors=['#4A90D9', '#67B26F'],
                textinfo='percent+value',
                texttemplate='%{percent:.1%}<br>%{value:,.0f}',
                hovertemplate='<b>%{label}</b><br>Points: %{value:,.0f}<br>Percentage: %{percent:.1%}<extra></extra>'
            )])
            
            fig.update_layout(
                title=f'Points by Source - {format_date_range_label(start_date, end_date)}',
                margin=dict(l=0, r=0, t=40, b=0),
                height=350,
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=-0.1,
                    xanchor="center",
                    x=0.5
                ),
                annotations=[dict(
                    text=f'{format_points(total_earned)}<br>Total',
                    x=0.5, y=0.5,
                    font_size=14,
                    showarrow=False
                )]
            )
            
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("### Source Breakdown")
            st.metric("💳 Paid Top-ups", format_points(total_paid))
            st.metric("🎁 Free/Promos", format_points(total_free))
            
            st.markdown("---")
            st.markdown("**Legend:**")
            st.markdown("🔵 Blue = Paid (from purchases)")
            st.markdown("🟢 Green = Free (promos, bonuses)")
    
    with tab3:
        import plotly.graph_objects as go
        
        points_df['cumulative_net'] = points_df['net'].cumsum()
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=points_df['date'],
            y=points_df['cumulative_net'],
            name='Cumulative Net Points',
            fill='tozeroy',
            line=dict(color='#4A90D9', width=2),
            fillcolor='rgba(74, 144, 217, 0.3)',
            hovertemplate='<b>%{x|%b %d}</b><br>Cumulative Net: %{y:,.0f}<extra></extra>'
        ))
        
        # Add zero line
        fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
        
        fig.update_layout(
            title=f'Cumulative Net Points Change - {format_date_range_label(start_date, end_date)}',
            margin=dict(l=0, r=0, t=40, b=0),
            height=350,
            legend=dict(
                orientation="h", 
                yanchor="bottom", 
                y=1.02, 
                xanchor="right", 
                x=1
            ),
            xaxis_title='Date',
            yaxis_title='Net Points'
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        net_total = int(points_df['cumulative_net'].iloc[-1]) if len(points_df) > 0 else 0
        status = "📈 Growing" if net_total > 0 else "📉 Declining"
        st.caption(f"**Net Change**: {format_points(net_total)} ({status})")


# ============================================
# Component 5: User Demographics
# ============================================

def render_user_demographics(duckdb_path: str, start_date: date, end_date: date):
    """
    Render User Demographics pie charts for the selected period.
    
    Shows:
        - User verification status breakdown
        - User engagement segments
        - Payment status distribution
    """
    
    st.header("👤 User Demographics")
    st.caption(f"*User segmentation analysis for {format_date_range_label(start_date, end_date)}*")
    
    with st.expander("ℹ️ Understanding User Segments", expanded=False):
        st.markdown("""
        **User Segments Explained:**
        
        - **Verified vs Unverified**: Users who completed mobile verification
        - **Paying vs Non-Paying**: Users who have made at least one purchase
        - **Active VPS**: Users currently running VPS instances
        - **Engagement Tiers**: Based on activity frequency
        
        **Why this matters:**
        - Higher verification rates indicate committed users
        - Paying user % is your conversion rate
        - Active VPS users are your most engaged segment
        """)
    
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')
    
    # Get users who signed up in the date range
    demo_query = f"""
    WITH period_users AS (
        SELECT DISTINCT u.user_id, u.mobile_verified, u.registration_ts
        FROM core.dim_user u
        WHERE DATE(u.registration_ts) >= '{start_str}' 
          AND DATE(u.registration_ts) <= '{end_str}'
    ),
    user_stats AS (
        SELECT 
            COUNT(*) as total_users,
            SUM(CASE WHEN mobile_verified THEN 1 ELSE 0 END) as verified_users,
            SUM(CASE WHEN NOT mobile_verified THEN 1 ELSE 0 END) as unverified_users
        FROM period_users
    ),
    paying_stats AS (
        SELECT 
            COUNT(DISTINCT pu.user_id) as paying_users
        FROM period_users pu
        JOIN core.user_account_state s ON pu.user_id = s.user_id
        WHERE s.total_points_earned_paid > 0
    ),
    vps_stats AS (
        SELECT 
            COUNT(DISTINCT pu.user_id) as vps_users
        FROM period_users pu
        JOIN core.user_account_state s ON pu.user_id = s.user_id
        WHERE s.current_vps_live > 0
    )
    SELECT 
        us.total_users,
        us.verified_users,
        us.unverified_users,
        COALESCE(ps.paying_users, 0) as paying_users,
        us.total_users - COALESCE(ps.paying_users, 0) as non_paying_users,
        COALESCE(vs.vps_users, 0) as vps_users,
        us.total_users - COALESCE(vs.vps_users, 0) as no_vps_users
    FROM user_stats us
    CROSS JOIN paying_stats ps
    CROSS JOIN vps_stats vs
    """
    
    demo_df = load_app_data(duckdb_path, demo_query, suppress_error=True)
    
    if demo_df is None or demo_df.empty or (demo_df.iloc[0]['total_users'] == 0):
        st.info(f"No user demographic data available for users who signed up in {format_date_range_label(start_date, end_date)}.")
        return
    
    row = demo_df.iloc[0]
    total_users = int(row['total_users'])
    verified = int(row['verified_users'])
    unverified = int(row['unverified_users'])
    paying = int(row['paying_users'])
    non_paying = int(row['non_paying_users'])
    vps_users = int(row['vps_users'])
    no_vps = int(row['no_vps_users'])
    
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    
    # Create 3 pie charts side by side
    col1, col2, col3 = st.columns(3)
    
    with col1:
        fig = go.Figure(data=[go.Pie(
            labels=['Verified', 'Unverified'],
            values=[verified, unverified],
            hole=0.4,
            marker_colors=['#67B26F', '#E0E0E0'],
            textinfo='percent+value',
            texttemplate='%{percent:.1%}<br>(%{value:,})',
            hovertemplate='<b>%{label}</b><br>Users: %{value:,}<br>%{percent:.1%}<extra></extra>'
        )])
        
        fig.update_layout(
            title=dict(text='Mobile Verification', x=0.5, xanchor='center'),
            margin=dict(l=10, r=10, t=40, b=10),
            height=300,
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=-0.15, xanchor="center", x=0.5),
            annotations=[dict(text=f'{total_users:,}<br>Total', x=0.5, y=0.5, font_size=12, showarrow=False)]
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        verification_rate = (verified / total_users * 100) if total_users > 0 else 0
        st.caption(f"**Verification Rate:** {verification_rate:.1f}%")
    
    with col2:
        fig = go.Figure(data=[go.Pie(
            labels=['Paying', 'Non-Paying'],
            values=[paying, non_paying],
            hole=0.4,
            marker_colors=['#4A90D9', '#E0E0E0'],
            textinfo='percent+value',
            texttemplate='%{percent:.1%}<br>(%{value:,})',
            hovertemplate='<b>%{label}</b><br>Users: %{value:,}<br>%{percent:.1%}<extra></extra>'
        )])
        
        fig.update_layout(
            title=dict(text='Payment Status', x=0.5, xanchor='center'),
            margin=dict(l=10, r=10, t=40, b=10),
            height=300,
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=-0.15, xanchor="center", x=0.5),
            annotations=[dict(text=f'{total_users:,}<br>Total', x=0.5, y=0.5, font_size=12, showarrow=False)]
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        conversion_rate = (paying / total_users * 100) if total_users > 0 else 0
        st.caption(f"**Conversion Rate:** {conversion_rate:.1f}%")
    
    with col3:
        fig = go.Figure(data=[go.Pie(
            labels=['Has Active VPS', 'No Active VPS'],
            values=[vps_users, no_vps],
            hole=0.4,
            marker_colors=['#F4A261', '#E0E0E0'],
            textinfo='percent+value',
            texttemplate='%{percent:.1%}<br>(%{value:,})',
            hovertemplate='<b>%{label}</b><br>Users: %{value:,}<br>%{percent:.1%}<extra></extra>'
        )])
        
        fig.update_layout(
            title=dict(text='VPS Status', x=0.5, xanchor='center'),
            margin=dict(l=10, r=10, t=40, b=10),
            height=300,
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=-0.15, xanchor="center", x=0.5),
            annotations=[dict(text=f'{total_users:,}<br>Total', x=0.5, y=0.5, font_size=12, showarrow=False)]
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        vps_rate = (vps_users / total_users * 100) if total_users > 0 else 0
        st.caption(f"**Active VPS Rate:** {vps_rate:.1f}%")
    
    # Summary row
    st.divider()
    st.markdown("### 📊 Demographics Summary")
    
    summary_col1, summary_col2, summary_col3, summary_col4 = st.columns(4)
    
    with summary_col1:
        st.metric(
            "Total Users (Period)",
            f"{total_users:,}",
            help=f"Users who signed up between {format_date_range_label(start_date, end_date)}"
        )
    
    with summary_col2:
        st.metric(
            "Verification Rate",
            f"{verification_rate:.1f}%",
            delta=f"{verified:,} verified",
            help="% of users who completed mobile verification"
        )
    
    with summary_col3:
        st.metric(
            "Conversion Rate",
            f"{conversion_rate:.1f}%",
            delta=f"{paying:,} paying",
            help="% of users who made at least one purchase"
        )
    
    with summary_col4:
        st.metric(
            "Product Adoption",
            f"{vps_rate:.1f}%",
            delta=f"{vps_users:,} with VPS",
            help="% of users with active VPS instances"
        )


# ============================================
# Component 5b: User Location Distribution
# ============================================

def render_user_location(duckdb_path: str, start_date: date, end_date: date):
    """
    Render a pie chart showing the geographic distribution of users
    based on their registration IP address (enriched to country).

    Data source: core.dim_user.registration_country_name
    The country is derived from registration_ip via the ip-api.com
    geolocation service (see scripts/enrich_ip_geo.py).

    Shows:
        - Pie chart of user count by country
        - Top countries table with counts and percentages
        - Small countries grouped into "Other" for readability
    """

    st.header("🌍 User Location")
    st.caption(
        f"*Geographic distribution of users who signed up "
        f"{format_date_range_label(start_date, end_date)}*"
    )

    with st.expander("ℹ️ About Location Data", expanded=False):
        st.markdown("""
        **How location is determined:**

        - Country is derived from the user's **registration IP address**
          using the ip-api.com geolocation service.
        - This reflects the user's location **at the time of sign-up**, not
          their current location.
        - VPN or proxy usage may result in an inaccurate country.

        **Why this matters:**
        - Identify your strongest geographic markets
        - Tailor marketing spend to high-value regions
        - Detect unexpected traffic sources
        """)

    # Format dates for SQL
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')

    # Query country distribution for users in the selected period
    location_query = f"""
    SELECT
        COALESCE(NULLIF(registration_country_name, ''), 'Unknown') AS country,
        COUNT(*) AS user_count
    FROM core.dim_user
    WHERE DATE(registration_ts) >= '{start_str}'
      AND DATE(registration_ts) <= '{end_str}'
    GROUP BY country
    ORDER BY user_count DESC
    """

    location_df = load_app_data(duckdb_path, location_query, suppress_error=True)

    if location_df is None or location_df.empty:
        st.info(
            f"No location data available for "
            f"{format_date_range_label(start_date, end_date)}. "
            "Run `python scripts/enrich_ip_geo.py` to populate country data."
        )
        return

    total_users = int(location_df["user_count"].sum())

    if total_users == 0:
        st.info("No users found in the selected period.")
        return

    # ── Group small countries into "Other" ──────────────────────
    # Keep the top 8 countries individually; merge the rest into "Other"
    TOP_N = 8
    if len(location_df) > TOP_N:
        top_df = location_df.head(TOP_N).copy()
        other_count = int(location_df.iloc[TOP_N:]["user_count"].sum())
        other_row = pd.DataFrame([{"country": "Other", "user_count": other_count}])
        chart_df = pd.concat([top_df, other_row], ignore_index=True)
    else:
        chart_df = location_df.copy()

    # Add percentage column
    chart_df["percentage"] = (chart_df["user_count"] / total_users * 100).round(1)

    # ── Layout: pie chart on the left, table on the right ───────
    col_chart, col_table = st.columns([3, 2])

    with col_chart:
        import plotly.graph_objects as go

        # Color palette — visually distinct, professional
        colors = [
            "#4A90D9", "#67B26F", "#F4A261", "#E76F51",
            "#9B59B6", "#1ABC9C", "#E74C3C", "#3498DB",
            "#E0E0E0",  # "Other" gets a neutral gray
        ]

        fig = go.Figure(data=[go.Pie(
            labels=chart_df["country"],
            values=chart_df["user_count"],
            hole=0.4,                           # donut style
            marker_colors=colors[: len(chart_df)],
            textinfo="percent+label",
            texttemplate="%{label}<br>%{percent:.1%}",
            hovertemplate=(
                "<b>%{label}</b><br>"
                "Users: %{value:,}<br>"
                "Share: %{percent:.1%}"
                "<extra></extra>"
            ),
            sort=False,                         # keep descending order
        )])

        fig.update_layout(
            title=dict(
                text="Users by Country",
                x=0.5,
                xanchor="center",
            ),
            margin=dict(l=10, r=10, t=50, b=10),
            height=420,
            showlegend=True,
            legend=dict(
                orientation="h",
                yanchor="top",
                y=-0.05,
                xanchor="center",
                x=0.5,
            ),
            annotations=[dict(
                text=f"{total_users:,}<br>Users",
                x=0.5, y=0.5,
                font_size=16,
                showarrow=False,
            )],
        )

        st.plotly_chart(fig, use_container_width=True)

    with col_table:
        st.markdown("#### Top Countries")

        # Build a clean display table from the FULL distribution
        # (not the grouped chart_df) so the user sees all countries
        table_df = location_df.copy()
        table_df["percentage"] = (
            table_df["user_count"] / total_users * 100
        ).round(1)
        table_df = table_df.rename(columns={
            "country": "Country",
            "user_count": "Users",
            "percentage": "Share %",
        })

        st.dataframe(
            table_df,
            hide_index=True,
            use_container_width=True,
            height=380,  # match pie chart height roughly
            column_config={
                "Country": st.column_config.TextColumn("Country"),
                "Users": st.column_config.NumberColumn("Users", format="%d"),
                "Share %": st.column_config.ProgressColumn(
                    "Share %",
                    min_value=0,
                    max_value=100,
                    format="%.1f%%",
                ),
            },
        )

    # ── Summary KPIs row ────────────────────────────────────────
    st.divider()
    st.markdown("### 📊 Location Summary")

    num_countries = len(location_df)
    top_country = location_df.iloc[0]["country"]
    top_country_pct = location_df.iloc[0]["user_count"] / total_users * 100
    # Concentration: what % of users come from the top 3 countries?
    top3_pct = (
        location_df.head(3)["user_count"].sum() / total_users * 100
    )

    summary_cols = st.columns(4)

    with summary_cols[0]:
        st.metric(
            "Total Countries",
            f"{num_countries}",
            help="Number of distinct countries in this period",
        )

    with summary_cols[1]:
        st.metric(
            "Top Country",
            top_country,
            delta=f"{top_country_pct:.1f}% of users",
            help="Country with the most signups",
        )

    with summary_cols[2]:
        st.metric(
            "Top 3 Concentration",
            f"{top3_pct:.1f}%",
            help="Share of signups from the top 3 countries",
        )

    with summary_cols[3]:
        st.metric(
            "Total Users",
            f"{total_users:,}",
            help="Users who signed up in the selected period",
        )


# ============================================
# Component 6: Top Users Table
# ============================================

def render_top_users(duckdb_path: str, start_date: date, end_date: date):
    """
    Render Top Users analysis filtered by date range.
    """
    
    st.header("🏆 Top Users")
    st.caption(f"*Power users who registered during {format_date_range_label(start_date, end_date)}*")
    
    with st.expander("ℹ️ Understanding Top Users", expanded=False):
        st.markdown("""
        **Top User Segments:**
        
        - **By Balance**: Users with highest unspent points (potential for future usage)
        - **By VPS**: Users with most active servers (highest engagement)
        - **Overview**: Summary stats for users in the selected period
        
        **What to watch:**
        - High balance + low VPS may indicate inactive valuable users
        - High VPS + low balance may need attention for churn prevention
        """)
    
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')
    
    tab1, tab2, tab3 = st.tabs(["💰 By Balance", "🖥️ By VPS", "📊 Overview"])
    
    with tab1:
        balance_query = f"""
        SELECT 
            u.user_id,
            u.registration_ts,
            u.mobile_verified,
            s.current_points_balance as balance,
            s.total_points_earned_paid as paid_earned,
            s.total_points_earned_free as free_earned,
            s.total_points_spent as spent,
            s.current_vps_live as live_vps
        FROM core.dim_user u
        JOIN core.user_account_state s ON u.user_id = s.user_id
        WHERE DATE(u.registration_ts) >= '{start_str}' 
          AND DATE(u.registration_ts) <= '{end_str}'
        ORDER BY s.current_points_balance DESC
        LIMIT 20
        """
        
        balance_df = load_app_data(duckdb_path, balance_query)
        
        if balance_df is not None and not balance_df.empty:
            display_df = balance_df.copy()
            display_df['balance'] = display_df['balance'].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "-")
            display_df['paid_earned'] = display_df['paid_earned'].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "-")
            display_df['mobile_verified'] = display_df['mobile_verified'].apply(lambda x: "✅" if x else "❌")
            display_df['user_id_short'] = display_df['user_id'].apply(lambda x: f"{x[:8]}..." if len(str(x)) > 8 else x)
            
            st.dataframe(
                display_df[['user_id_short', 'balance', 'paid_earned', 'live_vps', 'mobile_verified']],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "user_id_short": st.column_config.TextColumn("User ID", help="Truncated user identifier"),
                    "balance": st.column_config.TextColumn("Balance", help=get_metric_tooltip("points_balance")),
                    "paid_earned": st.column_config.TextColumn("Paid Earned", help=get_metric_tooltip("points_earned_paid")),
                    "live_vps": st.column_config.NumberColumn("Live VPS", help=get_metric_tooltip("live_vps")),
                    "mobile_verified": st.column_config.TextColumn("Verified", help=get_metric_tooltip("mobile_verified"))
                }
            )
        else:
            st.info(f"No users found who registered during {format_date_range_label(start_date, end_date)}.")
    
    with tab2:
        vps_query = f"""
        SELECT 
            u.user_id,
            s.current_vps_live as live_vps,
            s.total_launch_count as launches,
            s.total_terminate_count as terminates,
            s.current_points_balance as balance,
            s.total_points_earned_paid as paid_earned
        FROM core.dim_user u
        JOIN core.user_account_state s ON u.user_id = s.user_id
        WHERE s.current_vps_live > 0
          AND DATE(u.registration_ts) >= '{start_str}' 
          AND DATE(u.registration_ts) <= '{end_str}'
        ORDER BY s.current_vps_live DESC
        LIMIT 20
        """
        
        vps_df = load_app_data(duckdb_path, vps_query)
        
        if vps_df is not None and not vps_df.empty:
            display_df = vps_df.copy()
            display_df['balance'] = display_df['balance'].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "-")
            display_df['user_id_short'] = display_df['user_id'].apply(lambda x: f"{x[:8]}..." if len(str(x)) > 8 else x)
            
            st.dataframe(
                display_df[['user_id_short', 'live_vps', 'launches', 'terminates', 'balance']],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "user_id_short": st.column_config.TextColumn("User ID"),
                    "live_vps": st.column_config.NumberColumn("Live VPS", help=get_metric_tooltip("live_vps")),
                    "launches": st.column_config.NumberColumn("Total Launches", help=get_metric_tooltip("vps_created")),
                    "terminates": st.column_config.NumberColumn("Total Terminates", help=get_metric_tooltip("vps_terminated")),
                    "balance": st.column_config.TextColumn("Balance", help=get_metric_tooltip("points_balance"))
                }
            )
        else:
            st.info(f"No users with active VPS found who registered during {format_date_range_label(start_date, end_date)}.")
    
    with tab3:
        stats_query = f"""
        WITH period_users AS (
            SELECT user_id FROM core.dim_user
            WHERE DATE(registration_ts) >= '{start_str}' 
              AND DATE(registration_ts) <= '{end_str}'
        )
        SELECT 
            (SELECT COUNT(*) FROM period_users) as total_users,
            (SELECT COUNT(*) FROM core.dim_user u 
             JOIN period_users p ON u.user_id = p.user_id 
             WHERE u.mobile_verified) as verified_users,
            (SELECT COUNT(DISTINCT s.user_id) FROM core.user_account_state s 
             JOIN period_users p ON s.user_id = p.user_id 
             WHERE s.current_vps_live > 0) as users_with_vps,
            (SELECT COUNT(DISTINCT s.user_id) FROM core.user_account_state s 
             JOIN period_users p ON s.user_id = p.user_id 
             WHERE s.total_points_earned_paid > 0) as paying_users
        """
        
        stats_df = load_app_data(duckdb_path, stats_query)
        
        if stats_df is not None and not stats_df.empty:
            row = stats_df.iloc[0]
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                total = int(row['total_users']) if pd.notna(row['total_users']) else 0
                st.metric("Total Users", f"{total:,}",
                         help=f"Users registered during {format_date_range_label(start_date, end_date)}")
            
            with col2:
                verified = int(row['verified_users']) if pd.notna(row['verified_users']) else 0
                pct = (verified / total * 100) if total > 0 else 0
                st.metric("Verified Users", f"{verified:,}", delta=f"{pct:.1f}%",
                         help=get_metric_tooltip("mobile_verified"))
            
            with col3:
                vps_users = int(row['users_with_vps']) if pd.notna(row['users_with_vps']) else 0
                pct = (vps_users / total * 100) if total > 0 else 0
                st.metric("Users with VPS", f"{vps_users:,}", delta=f"{pct:.1f}%")
            
            with col4:
                paying = int(row['paying_users']) if pd.notna(row['paying_users']) else 0
                pct = (paying / total * 100) if total > 0 else 0
                st.metric("Paying Users", f"{paying:,}", delta=f"{pct:.1f}%",
                         help=get_metric_tooltip("paying_users"))


# ============================================
# Component 7: Weekly Overview
# ============================================

def render_weekly_overview(duckdb_path: str, start_date: date, end_date: date):
    """
    Render Weekly Overview table filtered by date range.
    """
    
    st.header("📅 Weekly Overview")
    st.caption(f"*Week-by-week performance for {format_date_range_label(start_date, end_date)}*")
    
    with st.expander("ℹ️ Reading the Weekly Table", expanded=False):
        st.markdown("""
        **Column Definitions:**
        
        - **Week Starting**: First day of the week (Monday)
        - **Signups**: New user registrations that week
        - **Active Users**: Sum of daily active users (may double-count)
        - **VPS Created**: New servers launched
        - **Top-ups**: Number of purchase transactions
        - **Revenue**: Total top-up revenue in SGD
        - **Points (Paid)**: Points from paid purchases
        - **Points Spent**: Points consumed
        
        **Week-over-Week (WoW) Change** shows the % change from the previous week.
        """)
    
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')
    
    weekly_query = f"""
    SELECT 
        week_start_date,
        COALESCE(new_signups, 0) as signups,
        COALESCE(active_users, 0) as active,
        COALESCE(new_vps_created, 0) as vps_created,
        COALESCE(topups_count, 0) as topups,
        COALESCE(topups_sum_sgd, 0) as revenue,
        COALESCE(payer_count, 0) as payers,
        COALESCE(points_earned_paid, 0) as points_paid,
        COALESCE(points_spent, 0) as points_spent
    FROM mart.platform_weekly_overview
    WHERE week_start_date >= '{start_str}' AND week_start_date <= '{end_str}'
    ORDER BY week_start_date DESC
    LIMIT 12
    """
    
    weekly_df = load_app_data(duckdb_path, weekly_query)
    
    if weekly_df is None or weekly_df.empty:
        st.info(f"No weekly data available for {format_date_range_label(start_date, end_date)}.")
        return
    
    # Format for display
    display_df = weekly_df.copy()
    display_df['week_start_date'] = pd.to_datetime(display_df['week_start_date']).dt.strftime('%Y-%m-%d')
    display_df['revenue'] = display_df['revenue'].apply(lambda x: f"${x:,.2f}")
    display_df['points_paid'] = display_df['points_paid'].apply(lambda x: f"{int(x):,}")
    display_df['points_spent'] = display_df['points_spent'].apply(lambda x: f"{int(x):,}")
    
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "week_start_date": st.column_config.TextColumn("Week Starting"),
            "signups": st.column_config.NumberColumn("Signups", help=get_metric_tooltip("new_signups")),
            "active": st.column_config.NumberColumn("Active Users", help=get_metric_tooltip("active_users")),
            "vps_created": st.column_config.NumberColumn("VPS Created", help=get_metric_tooltip("vps_created")),
            "topups": st.column_config.NumberColumn("Top-ups", help=get_metric_tooltip("topups")),
            "revenue": st.column_config.TextColumn("Revenue", help=get_metric_tooltip("revenue")),
            "payers": st.column_config.NumberColumn("Payers"),
            "points_paid": st.column_config.TextColumn("Points (Paid)", help=get_metric_tooltip("points_earned_paid")),
            "points_spent": st.column_config.TextColumn("Points Spent", help=get_metric_tooltip("points_spent"))
        }
    )
    
    # Week-over-week change calculation
    if len(weekly_df) >= 2:
        current_week = weekly_df.iloc[0]
        prev_week = weekly_df.iloc[1]
        
        st.divider()
        st.subheader("📊 Week-over-Week Change")
        st.caption("Comparing most recent week to the previous week")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            current = float(current_week['revenue']) if pd.notna(current_week['revenue']) else 0
            prev = float(prev_week['revenue']) if pd.notna(prev_week['revenue']) else 0
            change = calculate_percentage_change(current, prev)
            st.metric(
                "Revenue",
                format_currency(current),
                delta=f"{change:+.1f}%" if change is not None else None,
                help="Revenue change from previous week"
            )
        
        with col2:
            current = int(current_week['signups']) if pd.notna(current_week['signups']) else 0
            prev = int(prev_week['signups']) if pd.notna(prev_week['signups']) else 0
            change = calculate_percentage_change(current, prev)
            st.metric(
                "Signups",
                f"{current:,}",
                delta=f"{change:+.1f}%" if change is not None else None
            )
        
        with col3:
            current = int(current_week['vps_created']) if pd.notna(current_week['vps_created']) else 0
            prev = int(prev_week['vps_created']) if pd.notna(prev_week['vps_created']) else 0
            change = calculate_percentage_change(current, prev)
            st.metric(
                "VPS Created",
                f"{current:,}",
                delta=f"{change:+.1f}%" if change is not None else None
            )
        
        with col4:
            current = int(current_week['topups']) if pd.notna(current_week['topups']) else 0
            prev = int(prev_week['topups']) if pd.notna(prev_week['topups']) else 0
            change = calculate_percentage_change(current, prev)
            st.metric(
                "Top-ups",
                f"{current:,}",
                delta=f"{change:+.1f}%" if change is not None else None
            )


# ============================================
# Main App Analytics Dashboard
# ============================================

# ============================================
# Cohort Analysis (Phase 3)
# ============================================

def render_cohort_analysis(duckdb_path: str):
    """
    Render country-based cohort analysis.

    A "cohort" here is a country of registration, not signup week/month.
    This view helps compare market quality and monetization efficiency.
    """
    st.header("📊 Country Cohort Analysis")
    st.caption("*Compare lifecycle quality by country: conversion speed, retention, and revenue readiness.*")

    with st.expander("ℹ️ How to read this section", expanded=False):
        st.markdown("""
        **What is a country cohort?**
        - A cohort is all users who registered from the same country.
        - This reveals market-level differences in onboarding, activation, and monetization.

        **Core metrics:**
        - **Verified %**: users who completed mobile verification.
        - **First VPS %**: users who launched at least one VPS.
        - **First Paid %**: users who made at least one real payment.
        - **Median Days**: typical time from signup to each milestone.

        **Interpretation guide:**
        - High signup + low paid % => acquisition quality issue or pricing mismatch.
        - Fast early curves => healthier activation in that country.
        - Low retention by country => product-market fit or local UX friction.
        """)

    # Controls
    col_min, col_days, col_stage = st.columns([1, 1, 1])
    with col_min:
        min_size = st.number_input(
            "Min country size",
            min_value=5,
            max_value=500,
            value=20,
            step=5,
            key="country_cohort_min_size",
            help="Hide countries with fewer signups to reduce noise.",
        )
    with col_days:
        max_days = st.slider(
            "Max days to track",
            min_value=14,
            max_value=180,
            value=90,
            step=7,
            key="country_cohort_max_days",
            help="Window for time-to-convert analysis.",
        )
    with col_stage:
        stage_for_curve = st.selectbox(
            "Curve stage",
            options=["verify", "vps", "paid"],
            format_func=lambda x: {"verify": "Mobile Verified", "vps": "First VPS", "paid": "First Paid"}[x],
            key="country_cohort_stage",
        )

    # User-level milestones by country
    milestones_query = f"""
    WITH first_vps AS (
        SELECT user_id, MIN(event_ts) AS first_vps_at
        FROM core.fact_user_activity
        WHERE activity_type = 'LAUNCH_SERVER'
        GROUP BY user_id
    ),
    first_paid AS (
        SELECT user_id, MIN(payment_ts) AS first_paid_at
        FROM core.fact_payment_topup
        GROUP BY user_id
    )
    SELECT
        u.user_id,
        COALESCE(NULLIF(u.registration_country_name, ''), 'Unknown') AS country,
        u.registration_ts,
        u.mobile_verified_at AS verified_at,
        fv.first_vps_at,
        fp.first_paid_at,
        DATEDIFF('day', u.registration_ts, u.mobile_verified_at) AS days_to_verify,
        DATEDIFF('day', u.registration_ts, fv.first_vps_at) AS days_to_vps,
        DATEDIFF('day', u.registration_ts, fp.first_paid_at) AS days_to_paid
    FROM core.dim_user u
    LEFT JOIN first_vps fv ON u.user_id = fv.user_id
    LEFT JOIN first_paid fp ON u.user_id = fp.user_id
    WHERE u.registration_ts IS NOT NULL
    """
    user_df = load_app_data(duckdb_path, milestones_query, suppress_error=True)
    if user_df is None or user_df.empty:
        st.info("No cohort data found. Ensure user logs ETL has been run.")
        return

    # Aggregate country summary
    summary_df = (
        user_df.groupby("country", dropna=False)
        .agg(
            signups=("user_id", "count"),
            verified_count=("verified_at", lambda s: s.notna().sum()),
            first_vps_count=("first_vps_at", lambda s: s.notna().sum()),
            first_paid_count=("first_paid_at", lambda s: s.notna().sum()),
            median_days_to_verify=("days_to_verify", "median"),
            median_days_to_vps=("days_to_vps", "median"),
            median_days_to_paid=("days_to_paid", "median"),
        )
        .reset_index()
    )
    summary_df = summary_df[summary_df["signups"] >= min_size].copy()
    if summary_df.empty:
        st.warning("No countries meet the minimum cohort size. Lower the filter and try again.")
        return

    # Rate columns
    summary_df["verified_pct"] = (100 * summary_df["verified_count"] / summary_df["signups"]).round(1)
    summary_df["first_vps_pct"] = (100 * summary_df["first_vps_count"] / summary_df["signups"]).round(1)
    summary_df["first_paid_pct"] = (100 * summary_df["first_paid_count"] / summary_df["signups"]).round(1)
    summary_df = summary_df.sort_values(["first_paid_pct", "signups"], ascending=[False, False])

    # ── Tab layout ──────────────────────────────────────────
    tab1, tab2, tab3, tab4 = st.tabs([
        "Summary Table", "Conversion Benchmarks", "Time-to-Convert", "Retention by Country"
    ])

    # ── Tab 1: Country Summary ───────────────────────────────
    with tab1:
        st.markdown("#### Country Cohort Conversion Rates")
        display_df = summary_df.rename(columns={
            "country": "Country",
            "signups": "Signups",
            "verified_pct": "Verified %",
            "first_vps_pct": "First VPS %",
            "first_paid_pct": "First Paid %",
            "median_days_to_verify": "Med. Days -> Verify",
            "median_days_to_vps": "Med. Days -> VPS",
            "median_days_to_paid": "Med. Days -> Paid",
        })
        cols_to_show = [
            "Country", "Signups",
            "Verified %", "First VPS %", "First Paid %",
            "Med. Days -> Verify", "Med. Days -> VPS", "Med. Days -> Paid",
        ]
        st.dataframe(
            display_df[cols_to_show],
            hide_index=True,
            use_container_width=True,
            column_config={
                "Verified %": st.column_config.ProgressColumn(min_value=0, max_value=100, format="%.1f%%"),
                "First VPS %": st.column_config.ProgressColumn(min_value=0, max_value=100, format="%.1f%%"),
                "First Paid %": st.column_config.ProgressColumn(min_value=0, max_value=100, format="%.1f%%"),
            },
        )

    # ── Tab 2: Conversion Benchmarks ──────────────────────────
    with tab2:
        top_n = st.slider("Top countries to compare", 5, 15, 10, key="country_cohort_top_n")
        compare_df = summary_df.sort_values("signups", ascending=False).head(top_n).copy()
        fig = px.bar(
            compare_df.melt(
                id_vars=["country", "signups"],
                value_vars=["verified_pct", "first_vps_pct", "first_paid_pct"],
                var_name="metric",
                value_name="rate",
            ),
            x="country",
            y="rate",
            color="metric",
            barmode="group",
            labels={"country": "Country", "rate": "Conversion %", "metric": "Stage"},
            title="Country Conversion Benchmark (by signup volume)",
        )
        fig.update_layout(height=420, margin=dict(t=50, b=20))
        fig.update_traces(hovertemplate="<b>%{x}</b><br>%{fullData.name}: %{y:.1f}%<extra></extra>")
        st.plotly_chart(fig, use_container_width=True)

        best_paid = summary_df.iloc[0]
        worst_paid = summary_df.iloc[-1]
        col_a, col_b = st.columns(2)
        with col_a:
            st.success(
                f"Best paid conversion: **{best_paid['country']}** "
                f"({best_paid['first_paid_pct']:.1f}% from {int(best_paid['signups']):,} signups)"
            )
        with col_b:
            st.warning(
                f"Weakest paid conversion: **{worst_paid['country']}** "
                f"({worst_paid['first_paid_pct']:.1f}% from {int(worst_paid['signups']):,} signups)"
            )

    # ── Tab 3: Time-to-Convert by Country ─────────────────────
    with tab3:
        st.markdown("#### Cumulative Conversion Curves by Country")
        st.caption("X = days since signup, Y = cumulative % of users who reached stage")

        stage_col_map = {
            "verify": "days_to_verify",
            "vps": "days_to_vps",
            "paid": "days_to_paid",
        }
        stage_name_map = {
            "verify": "Mobile Verified",
            "vps": "First VPS",
            "paid": "First Paid",
        }
        stage_col = stage_col_map[stage_for_curve]
        stage_title = stage_name_map[stage_for_curve]

        eligible_countries = (
            summary_df.sort_values("signups", ascending=False)["country"]
            .head(8)
            .tolist()
        )
        curve_source = user_df[user_df["country"].isin(eligible_countries)].copy()
        if curve_source.empty:
            st.info("No data for conversion curves.")
        else:
            # Build cumulative % rows
            bucket_values = list(range(0, max_days + 1, 7))
            curve_rows = []
            for country in eligible_countries:
                country_users = curve_source[curve_source["country"] == country]
                cohort_size = len(country_users)
                if cohort_size == 0:
                    continue
                for day_bucket in bucket_values:
                    converted = (
                        country_users[stage_col].notna()
                        & (country_users[stage_col] <= day_bucket)
                    ).sum()
                    curve_rows.append(
                        {
                            "country": country,
                            "days_bucket": day_bucket,
                            "cumulative_pct": round(100 * converted / cohort_size, 1),
                            "cohort_size": cohort_size,
                        }
                    )
            curve_df = pd.DataFrame(curve_rows)
            if curve_df.empty:
                st.info("No conversions found for the selected stage.")
            else:
                fig = px.line(
                    curve_df,
                    x="days_bucket",
                    y="cumulative_pct",
                    color="country",
                    title=f"Time to {stage_title} by Country",
                    labels={
                        "days_bucket": "Days Since Signup",
                        "cumulative_pct": "Cumulative %",
                        "country": "Country",
                    },
                )
                fig.update_layout(height=420, margin=dict(t=50, b=20), legend=dict(orientation="h", y=-0.25))
                st.plotly_chart(fig, use_container_width=True)

    # ── Tab 4: Retention Matrix by Country ─────────────────────
    with tab4:
        st.markdown("#### Country Retention Matrix")
        st.caption("Rows = countries, Columns = month offset from signup, Values = retention %")

        ret_metric = st.selectbox(
            "Retention metric",
            options=["active", "paid"],
            format_func=lambda x: {"active": "Any Activity", "paid": "Made Payment"}[x],
            key="country_retention_metric",
        )
        retention_event_source = (
            "SELECT user_id, DATE(event_ts) AS event_date FROM core.fact_user_activity"
            if ret_metric == "active"
            else "SELECT user_id, DATE(payment_ts) AS event_date FROM core.fact_payment_topup"
        )
        retention_query = f"""
        WITH cohorts AS (
            SELECT
                user_id,
                COALESCE(NULLIF(registration_country_name, ''), 'Unknown') AS country,
                DATE(registration_ts) AS signup_date
            FROM core.dim_user
            WHERE registration_ts IS NOT NULL
        ),
        events AS (
            {retention_event_source}
        ),
        user_periods AS (
            SELECT DISTINCT
                c.user_id,
                c.country,
                DATEDIFF('month', c.signup_date, e.event_date) AS period_offset
            FROM cohorts c
            JOIN events e ON c.user_id = e.user_id
        ),
        cohort_sizes AS (
            SELECT country, COUNT(*) AS cohort_size
            FROM cohorts
            GROUP BY country
            HAVING COUNT(*) >= {int(min_size)}
        ),
        retention AS (
            SELECT
                up.country,
                up.period_offset,
                COUNT(DISTINCT up.user_id) AS user_count
            FROM user_periods up
            JOIN cohort_sizes cs ON up.country = cs.country
            WHERE up.period_offset >= 0 AND up.period_offset <= 12
            GROUP BY up.country, up.period_offset
        )
        SELECT
            r.country,
            r.period_offset,
            r.user_count,
            cs.cohort_size,
            ROUND(100.0 * r.user_count / cs.cohort_size, 1) AS retention_pct
        FROM retention r
        JOIN cohort_sizes cs ON r.country = cs.country
        ORDER BY r.country, r.period_offset
        """
        retention_df = load_app_data(duckdb_path, retention_query, suppress_error=True)
        if retention_df is None or retention_df.empty:
            st.info("No retention data available for current filters.")
        else:
            pivot = retention_df.pivot_table(
                index="country",
                columns="period_offset",
                values="retention_pct",
                aggfunc="first",
            ).fillna(0)
            pivot.columns = [f"Month {int(col)}" for col in pivot.columns]
            fig = px.imshow(
                pivot.values,
                x=pivot.columns.tolist(),
                y=pivot.index.tolist(),
                color_continuous_scale="RdYlGn",
                zmin=0,
                zmax=100,
                labels={"color": "Retention %", "x": "Month After Signup", "y": "Country"},
                text_auto=".0f",
                title=f"{'Activity' if ret_metric == 'active' else 'Payment'} Retention by Country",
            )
            fig.update_layout(height=max(320, len(pivot) * 30), margin=dict(t=50, b=20))
            st.plotly_chart(fig, use_container_width=True)


def render_app_analytics(duckdb_path: str):
    """
    Render the complete App Analytics Dashboard.
    
    This is the main entry point that ties all app analytics components together.
    """
    
    # Import date picker component
    from app.components.date_picker import render_date_range_picker
    
    # Page header
    st.title("📱 App Analytics Dashboard")
    st.markdown("""
    *Comprehensive app performance metrics from User Logs warehouse:*
    - **Revenue & Monetization** - Top-ups, transactions, points economy
    - **User Growth** - Signups, verification, demographics
    - **Product Usage** - VPS creation, activity patterns
    """)
    
    st.divider()
    
    # Check Data Availability
    table_status = check_mart_tables_exist(duckdb_path)
    
    if not any(table_status.values()):
        st.error("""
        **App Analytics data not available.**
        
        The User Logs warehouse tables are required for this dashboard.
        
        Run the User Logs ETL to populate data:
        ```bash
        python scripts/run_etl_user_logs.py
        ```
        """)
        return
    
    # Show data status
    with st.expander("📊 Data Source Status", expanded=False):
        col1, col2 = st.columns(2)
        with col1:
            for table, exists in list(table_status.items())[:4]:
                status = "✅" if exists else "⚠️"
                st.caption(f"{status} {table}")
        with col2:
            for table, exists in list(table_status.items())[4:]:
                status = "✅" if exists else "⚠️"
                st.caption(f"{status} {table}")
    
    # Date Range Selection (with comparison enabled for funnel)
    start_date, end_date, prev_start_date, prev_end_date = render_date_range_picker(
        key="app_analytics",
        default_days=30,
        max_days=365,
        show_comparison=True
    )
    
    # Display selected date range prominently
    date_info = f"📅 **Showing data for: {format_date_range_label(start_date, end_date)}** ({(end_date - start_date).days + 1} days)"
    if prev_start_date and prev_end_date:
        date_info += f" | Comparing to: {format_date_range_label(prev_start_date, prev_end_date)}"
    st.info(date_info)
    
    st.divider()
    
    # Component 0: Conversion Funnel (at the top)
    if table_status.get('core.dim_user', False) and table_status.get('core.user_account_state', False):
        with st.container():
            render_conversion_funnel(duckdb_path, start_date, end_date, prev_start_date, prev_end_date)
        st.divider()
    
    # Component 0.5: Cohort Funnel with Time-to-Convert (Phase 3)
    if (table_status.get('core.dim_user', False)
            and table_status.get('core.user_account_state', False)):
        with st.container():
            render_cohort_analysis(duckdb_path)
        st.divider()

    # Component 1: Platform KPIs
    if table_status.get('mart.platform_daily_overview', False):
        with st.container():
            render_platform_kpis(duckdb_path, start_date, end_date)
        st.divider()
    
    # Component 2: Revenue Trends
    if table_status.get('mart.platform_daily_overview', False):
        with st.container():
            render_revenue_trends(duckdb_path, start_date, end_date)
        st.divider()
    
    # Component 3: User Activity Trends
    if table_status.get('mart.platform_daily_overview', False):
        with st.container():
            render_user_activity_trends(duckdb_path, start_date, end_date)
        st.divider()
    
    # Component 4: Points Economy
    if table_status.get('mart.platform_daily_overview', False):
        with st.container():
            render_points_economy(duckdb_path, start_date, end_date)
        st.divider()
    
    # Component 5: User Demographics (NEW)
    if table_status.get('core.dim_user', False) and table_status.get('core.user_account_state', False):
        with st.container():
            render_user_demographics(duckdb_path, start_date, end_date)
        st.divider()
    
    # Component 5b: User Location Pie Chart
    if table_status.get('core.dim_user', False):
        with st.container():
            render_user_location(duckdb_path, start_date, end_date)
        st.divider()
    
    # Component 6: Top Users
    if table_status.get('core.user_account_state', False) and table_status.get('core.dim_user', False):
        with st.container():
            render_top_users(duckdb_path, start_date, end_date)
        st.divider()
    
    # Component 7: Weekly Overview
    if table_status.get('mart.platform_weekly_overview', False):
        with st.container():
            render_weekly_overview(duckdb_path, start_date, end_date)
        st.divider()
    
    # Footer with metric definitions
    with st.expander("📖 Metric Definitions Reference", expanded=False):
        st.markdown("### Complete Metric Glossary")
        
        categories = {
            "💰 Revenue Metrics": ["revenue", "topups", "avg_transaction"],
            "👥 User Metrics": ["new_signups", "mobile_verified", "active_users", "paying_users"],
            "🖥️ VPS Metrics": ["vps_created", "vps_terminated", "net_vps", "live_vps"],
            "💎 Points Metrics": ["points_earned_paid", "points_earned_free", "points_spent", "points_velocity", "points_balance"]
        }
        
        for category, metrics in categories.items():
            st.markdown(f"#### {category}")
            for metric_key in metrics:
                if metric_key in METRIC_DEFINITIONS:
                    m = METRIC_DEFINITIONS[metric_key]
                    st.markdown(f"""
                    **{m['name']}**
                    - *Definition*: {m['definition']}
                    - *Calculation*: `{m['calculation']}`
                    - *Interpretation*: {m['interpretation']}
                    """)
            st.markdown("---")
