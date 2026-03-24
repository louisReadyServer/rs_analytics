"""
GA4 Business Intelligence Dashboard Components

This module implements the comprehensive GA4 BI dashboard following the mental model:
    1. Are the right people coming? (acquisition quality)
    2. Are they doing what we expect? (behavior & friction)
    3. Where are we leaking value? (drop-offs & opportunities)

Dashboard Structure:
    1. GA4 Executive Summary (GA4-only KPIs)
    2. Acquisition Quality (source/medium analysis, campaign sanity check)
    3. Landing Page Performance (money page analysis + opportunity scoring)
    4. Funnel Health (explicit conversion funnel)
    5. Behavior & Engagement (event performance, engagement distribution)
    6. User Segments (New vs Returning, Mobile vs Desktop, Paid vs Organic)
    7. Geo & Device Reality Check
    8. Trend Diagnostics (GA4-only truth)
    9. What Changed (GA4-driven narrative)

Author: rs_analytics
Created: 2026-02-03
"""

from datetime import datetime, timedelta, date
from typing import Optional, Dict, Any, List, Tuple
import streamlit as st
import pandas as pd
import duckdb

from app.components.glossary import TERM_TOOLTIPS
from rs_analytics.utils.formatting import (
    safe_divide,
    calculate_delta as calculate_percentage_change,
)


# ============================================
# Data Loading Helpers
# ============================================

def load_ga4_data(duckdb_path: str, query: str, suppress_error: bool = False) -> Optional[pd.DataFrame]:
    """
    Load GA4 data from DuckDB with error handling.
    
    Args:
        duckdb_path: Path to DuckDB database file
        query: SQL query to execute
        suppress_error: If True, don't show error messages (for optional tables)
    
    Returns:
        DataFrame with query results, or None if error occurs
    
    Why this matters:
        Centralized error handling prevents crashes and provides
        meaningful feedback when data is unavailable.
    """
    try:
        conn = duckdb.connect(duckdb_path, read_only=True)
        df = conn.execute(query).fetchdf()
        conn.close()
        
        # Convert date columns to proper datetime if they exist
        if 'date' in df.columns and not df.empty:
            # Handle YYYYMMDD format (8-digit string)
            if df['date'].dtype == 'object' and len(str(df['date'].iloc[0])) == 8:
                df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
            else:
                df['date'] = pd.to_datetime(df['date'])
        
        return df
    except Exception as e:
        if not suppress_error:
            # Check if it's a "table not found" error
            error_msg = str(e).lower()
            if "does not exist" in error_msg or "not found" in error_msg:
                # Don't show error for missing tables - just return None
                pass
            else:
                st.error(f"❌ Query error: {e}")
        return None


def check_table_exists(duckdb_path: str, table_name: str) -> bool:
    """
    Check if a table exists in the DuckDB database.
    
    Args:
        duckdb_path: Path to DuckDB database file
        table_name: Name of the table to check
    
    Returns:
        True if table exists, False otherwise
    """
    try:
        conn = duckdb.connect(duckdb_path, read_only=True)
        result = conn.execute(f"""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_name = '{table_name}'
        """).fetchone()
        conn.close()
        return result[0] > 0 if result else False
    except Exception:
        return False


def get_available_ga4_tables(duckdb_path: str) -> Dict[str, bool]:
    """
    Get a dictionary of GA4 tables and their availability.
    
    Args:
        duckdb_path: Path to DuckDB database file
    
    Returns:
        Dictionary mapping table names to availability (True/False)
    """
    tables = {
        'ga4_sessions': False,
        'ga4_traffic_overview': False,
        'ga4_page_performance': False,
        'ga4_geographic_data': False,
        'ga4_technology_data': False,
        'ga4_event_data': False
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
    except Exception:
        pass
    
    return tables




# ============================================
# Component 1: GA4 Executive Summary
# ============================================

def render_ga4_executive_summary(
    duckdb_path: str,
    start_date: date,
    end_date: date,
    prev_start_date: Optional[date] = None,
    prev_end_date: Optional[date] = None
):
    """
    Render GA4 Executive Summary - KPIs for last 7/30 days with deltas.
    
    This isolates product/website health from paid media noise.
    
    KPIs displayed:
        - Sessions
        - New Users
        - Engaged Sessions
        - Engagement Rate
        - Conversion Rate (primary event)
        - Avg Conversions per User
    
    Why this matters:
        These metrics show if the website itself is healthy, separate
        from advertising performance. A drop here means product issues.
    """
    
    st.header("📊 GA4 Executive Summary")
    st.caption("*Website health metrics (GA4-only, no paid media noise)*")
    
    # Format dates for SQL (YYYYMMDD format based on schema)
    start_str = start_date.strftime('%Y%m%d')
    end_str = end_date.strftime('%Y%m%d')
    
    # Current period query
    current_query = f"""
    SELECT 
        CAST(SUM(CAST(sessions AS BIGINT)) AS BIGINT) as sessions,
        CAST(SUM(CAST(newUsers AS BIGINT)) AS BIGINT) as new_users,
        CAST(SUM(CAST(totalUsers AS BIGINT)) AS BIGINT) as total_users,
        AVG(CAST(bounceRate AS DOUBLE)) as avg_bounce_rate,
        COUNT(DISTINCT date) as days_count
    FROM ga4_sessions
    WHERE date >= '{start_str}' AND date <= '{end_str}'
    """
    
    current_df = load_ga4_data(duckdb_path, current_query)
    
    # Previous period query (if comparison enabled)
    prev_df = None
    if prev_start_date and prev_end_date:
        prev_start_str = prev_start_date.strftime('%Y%m%d')
        prev_end_str = prev_end_date.strftime('%Y%m%d')
        
        prev_query = f"""
        SELECT 
            CAST(SUM(CAST(sessions AS BIGINT)) AS BIGINT) as sessions,
            CAST(SUM(CAST(newUsers AS BIGINT)) AS BIGINT) as new_users
        FROM ga4_sessions
        WHERE date >= '{prev_start_str}' AND date <= '{prev_end_str}'
        """
        
        prev_df = load_ga4_data(duckdb_path, prev_query)
    
    if current_df is None or current_df.empty:
        st.warning("⚠️ No GA4 session data available for the selected period.")
        return
    
    row = current_df.iloc[0]
    
    # Extract current metrics (handle NA/None values safely)
    sessions = int(row['sessions']) if row['sessions'] is not None and pd.notna(row['sessions']) else 0
    new_users = int(row['new_users']) if row['new_users'] is not None and pd.notna(row['new_users']) else 0
    total_users = int(row['total_users']) if row['total_users'] is not None and pd.notna(row['total_users']) else 0
    bounce_rate = float(row['avg_bounce_rate']) if row['avg_bounce_rate'] is not None and pd.notna(row['avg_bounce_rate']) else 0
    engagement_rate = 1.0 - bounce_rate  # Engagement rate is inverse of bounce rate
    
    # Calculate previous period metrics for deltas
    prev_sessions = None
    prev_new_users = None
    
    if prev_df is not None and not prev_df.empty:
        prev_row = prev_df.iloc[0]
        # Handle NA/None values safely
        prev_sessions = int(prev_row['sessions']) if prev_row['sessions'] is not None and pd.notna(prev_row['sessions']) else 0
        prev_new_users = int(prev_row['new_users']) if prev_row['new_users'] is not None and pd.notna(prev_row['new_users']) else 0
    
    # Calculate deltas
    sessions_delta = calculate_percentage_change(sessions, prev_sessions)
    new_users_delta = calculate_percentage_change(new_users, prev_new_users)
    
    # ========================================
    # Display KPIs in 6 columns
    # ========================================
    
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    
    with col1:
        st.metric(
            label="🎯 Sessions",
            value=f"{sessions:,}",
            delta=f"{sessions_delta:+.1f}%" if sessions_delta is not None else None,
            help=TERM_TOOLTIPS.get("Sessions", "Total sessions in the selected period"),
        )
    
    with col2:
        st.metric(
            label="👤 New Users",
            value=f"{new_users:,}",
            delta=f"{new_users_delta:+.1f}%" if new_users_delta is not None else None,
            help=TERM_TOOLTIPS.get("New Users", "Users visiting for the first time"),
        )
    
    with col3:
        # For engaged sessions, use a heuristic: sessions with bounce rate < 50%
        # (This is an approximation; actual engaged sessions would come from GA4 API)
        engaged_sessions = int(sessions * engagement_rate)
        st.metric(
            label="✨ Engaged Sessions",
            value=f"{engaged_sessions:,}",
            help="Sessions where users actively engaged with content (bounce rate < 50%)",
        )
    
    with col4:
        st.metric(
            label="📈 Engagement Rate",
            value=f"{engagement_rate:.1%}",
            help=TERM_TOOLTIPS.get("Engagement Rate", "Percentage of sessions with engagement"),
        )
    
    with col5:
        # Conversion rate placeholder (would need conversion event data from ga4_event_data)
        # For now, show a placeholder
        st.metric(
            label="🎁 Conversion Rate",
            value="-",
            help=TERM_TOOLTIPS.get("Conversion Rate", "Conversions ÷ Clicks × 100"),
        )
    
    with col6:
        # Avg conversions per user (placeholder)
        st.metric(
            label="🔄 Avg Conv/User",
            value="-",
            help="Average conversions per user (requires event tracking)",
        )
    
    # ========================================
    # Quick Insight Box
    # ========================================
    
    st.divider()
    
    # Generate quick insights based on data
    insights = []
    
    if engagement_rate < 0.3:
        insights.append("⚠️ **Low Engagement**: Engagement rate below 30%. Users may not be finding what they need.")
    elif engagement_rate > 0.7:
        insights.append("✅ **Strong Engagement**: Engagement rate above 70%. Users are actively interacting with content.")
    
    if new_users_delta and new_users_delta < -10:
        insights.append(f"📉 **New User Drop**: New users down {abs(new_users_delta):.1f}%. Check acquisition channels.")
    elif new_users_delta and new_users_delta > 20:
        insights.append(f"📈 **New User Growth**: New users up {new_users_delta:.1f}%. Acquisition is working.")
    
    if bounce_rate > 0.7:
        insights.append("🔴 **High Bounce Rate**: >70% of sessions bounce. Landing page experience needs attention.")
    
    if insights:
        cols = st.columns(len(insights))
        for i, insight in enumerate(insights):
            with cols[i]:
                st.info(insight, icon="💡")


# ============================================
# Component 2: Acquisition Quality
# ============================================

def render_acquisition_quality(
    duckdb_path: str,
    start_date: date,
    end_date: date
):
    """
    Render Acquisition Quality analysis - THIS is where insight starts.
    
    Components:
        A. Traffic quality by source/medium (table/heatmap)
        B. Campaign intent sanity check
    
    What we're looking for:
        - High sessions + low engagement → junk traffic
        - Low sessions + high conv rate → scale candidate
    
    Why this matters:
        This tells you where to send MORE or LESS traffic,
        not just where traffic comes from.
    """
    
    st.header("🎯 Acquisition Quality")
    st.caption("*Where are the right people coming from?*")
    
    # Check if required table exists
    if not check_table_exists(duckdb_path, 'ga4_traffic_overview'):
        st.warning("""
        ⚠️ **Traffic overview data not available.**
        
        The `ga4_traffic_overview` table is required for this section.
        
        Run the GA4 ETL to populate this data:
        ```bash
        python scripts/run_etl_unified.py --source ga4 --comprehensive --lifetime
        ```
        """)
        return
    
    start_str = start_date.strftime('%Y%m%d')
    end_str = end_date.strftime('%Y%m%d')
    
    # ========================================
    # A. Traffic Quality by Source/Medium
    # ========================================
    
    st.subheader("A. Traffic Quality by Source / Medium")
    st.caption("*Identify junk traffic and scale candidates*")
    
    traffic_query = f"""
    SELECT 
        sessionSource as source,
        sessionMedium as medium,
        CAST(SUM(CAST(sessions AS BIGINT)) AS BIGINT) as sessions,
        CAST(SUM(CAST(totalUsers AS BIGINT)) AS BIGINT) as users,
        CAST(SUM(CAST(newUsers AS BIGINT)) AS BIGINT) as new_users,
        AVG(CAST(bounceRate AS DOUBLE)) as bounce_rate,
        CAST(SUM(CAST(screenPageViews AS BIGINT)) AS BIGINT) as page_views
    FROM ga4_traffic_overview
    WHERE date >= '{start_str}' AND date <= '{end_str}'
        AND sessionSource IS NOT NULL 
        AND sessionMedium IS NOT NULL
    GROUP BY sessionSource, sessionMedium
    ORDER BY sessions DESC
    LIMIT 20
    """
    
    traffic_df = load_ga4_data(duckdb_path, traffic_query)
    
    if traffic_df is not None and not traffic_df.empty:
        # Calculate engagement rate (inverse of bounce rate)
        traffic_df['engagement_rate'] = 1.0 - traffic_df['bounce_rate']
        
        # Calculate pages per session
        traffic_df['pages_per_session'] = traffic_df['page_views'] / traffic_df['sessions']
        
        # Create quality score: engagement_rate * pages_per_session
        # Normalize to 0-100 scale
        traffic_df['quality_score'] = (
            traffic_df['engagement_rate'] * traffic_df['pages_per_session'] * 20
        ).clip(upper=100)
        
        # Add insight column based on quality
        def categorize_traffic(row):
            if row['sessions'] > traffic_df['sessions'].quantile(0.75):
                # High volume traffic
                if row['quality_score'] > 50:
                    return "✅ Winner"
                elif row['quality_score'] > 30:
                    return "🟡 Monitor"
                else:
                    return "🔴 Junk"
            elif row['quality_score'] > 60:
                # Low volume but high quality
                return "🚀 Scale"
            else:
                return "⚪ Standard"
        
        traffic_df['insight'] = traffic_df.apply(categorize_traffic, axis=1)
        
        # Format for display
        display_df = traffic_df.copy()
        display_df['sessions'] = display_df['sessions'].apply(lambda x: f"{int(x):,}")
        display_df['users'] = display_df['users'].apply(lambda x: f"{int(x):,}")
        display_df['engagement_rate'] = display_df['engagement_rate'].apply(lambda x: f"{x:.1%}")
        display_df['pages_per_session'] = display_df['pages_per_session'].apply(lambda x: f"{x:.2f}")
        display_df['quality_score'] = display_df['quality_score'].apply(lambda x: f"{x:.0f}")
        
        # Display table
        st.dataframe(
            display_df[[
                'source', 'medium', 'sessions', 'users', 
                'engagement_rate', 'pages_per_session', 'quality_score', 'insight'
            ]],
            use_container_width=True,
            hide_index=True,
            column_config={
                "source": "Source",
                "medium": "Medium",
                "sessions": "Sessions",
                "users": "Users",
                "engagement_rate": "Engagement",
                "pages_per_session": "Pages/Session",
                "quality_score": "Quality Score",
                "insight": "Action"
            }
        )
        
        # Key insights
        st.markdown("**💡 Interpretation:**")
        st.markdown("""
        - **✅ Winner**: High volume + high quality → Keep investing
        - **🚀 Scale**: Low volume + high quality → Increase spend here
        - **🔴 Junk**: High volume + low quality → Reduce or cut
        - **🟡 Monitor**: Medium quality → Optimize landing pages
        """)
    else:
        st.warning("⚠️ No traffic data available for the selected period.")
    
    st.divider()
    
    # ========================================
    # B. Campaign Intent Sanity Check
    # ========================================
    
    st.subheader("B. Campaign Intent Sanity Check")
    st.caption("*Does campaign message match landing page behavior?*")
    
    campaign_query = f"""
    SELECT 
        sessionCampaignName as campaign,
        sessionMedium as medium,
        CAST(SUM(CAST(sessions AS BIGINT)) AS BIGINT) as sessions,
        AVG(CAST(bounceRate AS DOUBLE)) as bounce_rate,
        CAST(SUM(CAST(screenPageViews AS BIGINT)) AS BIGINT) as page_views
    FROM ga4_traffic_overview
    WHERE date >= '{start_str}' AND date <= '{end_str}'
        AND sessionCampaignName IS NOT NULL
        AND sessionCampaignName != '(not set)'
    GROUP BY sessionCampaignName, sessionMedium
    ORDER BY sessions DESC
    LIMIT 15
    """
    
    campaign_df = load_ga4_data(duckdb_path, campaign_query)
    
    if campaign_df is not None and not campaign_df.empty:
        # Calculate engagement
        campaign_df['engagement_rate'] = 1.0 - campaign_df['bounce_rate']
        campaign_df['pages_per_session'] = campaign_df['page_views'] / campaign_df['sessions']
        
        # Identify campaigns with poor intent match
        def intent_check(row):
            if row['bounce_rate'] > 0.7:
                return "❌ Poor Match"
            elif row['bounce_rate'] < 0.4:
                return "✅ Good Match"
            else:
                return "🟡 Fair"
        
        campaign_df['intent_match'] = campaign_df.apply(intent_check, axis=1)
        
        # Format for display
        display_df = campaign_df.copy()
        display_df['sessions'] = display_df['sessions'].apply(lambda x: f"{int(x):,}")
        display_df['bounce_rate'] = display_df['bounce_rate'].apply(lambda x: f"{x:.1%}")
        display_df['engagement_rate'] = display_df['engagement_rate'].apply(lambda x: f"{x:.1%}")
        display_df['pages_per_session'] = display_df['pages_per_session'].apply(lambda x: f"{x:.2f}")
        
        st.dataframe(
            display_df[[
                'campaign', 'medium', 'sessions', 'bounce_rate', 
                'engagement_rate', 'pages_per_session', 'intent_match'
            ]],
            use_container_width=True,
            hide_index=True,
            column_config={
                "campaign": "Campaign",
                "medium": "Medium",
                "sessions": "Sessions",
                "bounce_rate": "Bounce Rate",
                "engagement_rate": "Engagement",
                "pages_per_session": "Pages/Session",
                "intent_match": "Intent Match"
            }
        )
        
        # Campaign insights
        poor_campaigns = campaign_df[campaign_df['bounce_rate'] > 0.7]
        if not poor_campaigns.empty:
            st.warning(
                f"⚠️ **{len(poor_campaigns)} campaigns have >70% bounce rate**. "
                "Message-landing page mismatch or poor targeting."
            )
    else:
        st.info("No campaign data available for the selected period.")


# ============================================
# Component 3: Landing Page Performance
# ============================================

def render_landing_page_performance(
    duckdb_path: str,
    start_date: date,
    end_date: date
):
    """
    Render Landing Page Performance - Money Page Analysis.
    
    Components:
        A. Top landing pages by traffic
        B. Opportunity lens (THIS IS GOLD)
    
    Opportunity Score = Sessions × (Site Avg CVR − Page CVR)
    
    Why this matters:
        Shows which pages will deliver the BIGGEST LIFT if improved.
        This is how CRO work gets prioritized rationally.
    """
    
    st.header("💰 Landing Page Performance")
    st.caption("*Money page analysis + opportunity scoring*")
    
    # Check if required table exists
    if not check_table_exists(duckdb_path, 'ga4_page_performance'):
        st.warning("""
        ⚠️ **Page performance data not available.**
        
        The `ga4_page_performance` table is required for this section.
        
        Run the GA4 ETL to populate this data:
        ```bash
        python scripts/run_etl_unified.py --source ga4 --comprehensive --lifetime
        ```
        """)
        return
    
    start_str = start_date.strftime('%Y%m%d')
    end_str = end_date.strftime('%Y%m%d')
    
    # ========================================
    # A. Top Landing Pages by Traffic
    # ========================================
    
    st.subheader("A. Top Landing Pages")
    
    pages_query = f"""
    SELECT 
        pagePath as page,
        pageTitle as title,
        CAST(SUM(CAST(sessions AS BIGINT)) AS BIGINT) as sessions,
        CAST(SUM(CAST(screenPageViews AS BIGINT)) AS BIGINT) as page_views,
        AVG(CAST(bounceRate AS DOUBLE)) as bounce_rate,
        AVG(CAST(averageSessionDuration AS DOUBLE)) as avg_duration
    FROM ga4_page_performance
    WHERE date >= '{start_str}' AND date <= '{end_str}'
        AND pagePath IS NOT NULL
    GROUP BY pagePath, pageTitle
    ORDER BY sessions DESC
    LIMIT 25
    """
    
    pages_df = load_ga4_data(duckdb_path, pages_query)
    
    if pages_df is not None and not pages_df.empty:
        # Calculate engagement rate
        pages_df['engagement_rate'] = 1.0 - pages_df['bounce_rate']
        
        # Format for display
        display_df = pages_df.copy()
        
        # Shorten long page paths for display
        display_df['page_short'] = display_df['page'].apply(
            lambda x: x if len(x) <= 50 else f"...{x[-47:]}"
        )
        
        display_df['sessions'] = display_df['sessions'].apply(lambda x: f"{int(x):,}")
        display_df['page_views'] = display_df['page_views'].apply(lambda x: f"{int(x):,}")
        display_df['bounce_rate'] = display_df['bounce_rate'].apply(lambda x: f"{x:.1%}")
        display_df['engagement_rate'] = display_df['engagement_rate'].apply(lambda x: f"{x:.1%}")
        display_df['avg_duration'] = display_df['avg_duration'].apply(lambda x: f"{x:.0f}s")
        
        st.dataframe(
            display_df[[
                'page_short', 'sessions', 'page_views', 
                'bounce_rate', 'engagement_rate', 'avg_duration'
            ]],
            use_container_width=True,
            hide_index=True,
            column_config={
                "page_short": "Page",
                "sessions": "Sessions",
                "page_views": "Page Views",
                "bounce_rate": "Bounce Rate",
                "engagement_rate": "Engagement",
                "avg_duration": "Avg Duration"
            }
        )
    else:
        st.warning("⚠️ No page performance data available.")
    
    st.divider()
    
    # ========================================
    # B. Opportunity Lens (GOLD)
    # ========================================
    
    st.subheader("B. 🏆 Opportunity Scoring")
    st.caption("*Pages that will deliver the biggest lift if improved*")
    
    if pages_df is not None and not pages_df.empty:
        # Calculate site average engagement rate
        site_avg_engagement = pages_df['engagement_rate'].mean()
        
        # Calculate opportunity score
        # Opportunity = Sessions × (Site Avg Engagement − Page Engagement)
        # Higher score = more opportunity to improve
        pages_df['opportunity_score'] = pages_df['sessions'] * (
            site_avg_engagement - pages_df['engagement_rate']
        )
        
        # Only show pages with positive opportunity (below average performance)
        opportunity_df = pages_df[pages_df['opportunity_score'] > 0].copy()
        
        if not opportunity_df.empty:
            # Sort by opportunity score
            opportunity_df = opportunity_df.sort_values('opportunity_score', ascending=False).head(10)
            
            # Format for display
            display_df = opportunity_df.copy()
            display_df['page_short'] = display_df['page'].apply(
                lambda x: x if len(x) <= 50 else f"...{x[-47:]}"
            )
            display_df['sessions'] = display_df['sessions'].apply(lambda x: f"{int(x):,}")
            display_df['engagement_rate'] = display_df['engagement_rate'].apply(lambda x: f"{x:.1%}")
            display_df['opportunity_score'] = display_df['opportunity_score'].apply(lambda x: f"{x:.0f}")
            
            st.dataframe(
                display_df[[
                    'page_short', 'sessions', 'engagement_rate', 'opportunity_score'
                ]],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "page_short": "Page",
                    "sessions": "Sessions",
                    "engagement_rate": "Current Engagement",
                    "opportunity_score": "🎯 Opportunity Score"
                }
            )
            
            st.info(
                f"💡 **Site average engagement: {site_avg_engagement:.1%}**. "
                f"Pages above have below-average performance with high traffic → biggest impact if improved."
            )
        else:
            st.success("✅ All high-traffic pages perform above site average. No major opportunities detected.")
    

# ============================================
# Component 4: Funnel Health
# ============================================

def render_funnel_health(
    duckdb_path: str,
    start_date: date,
    end_date: date
):
    """
    Render Funnel Health - Explicit conversion funnel.
    
    Example funnel:
        Session → Landing Page View → Key CTA Click → Conversion
    
    For each step:
        - Users
        - Step-to-step conversion %
        - Drop-off %
    
    Why this matters:
        Shows WHERE EXACTLY users abandon. Without this, your GA4 dashboard is incomplete.
        Reveals whether issue is copy, UX, speed, or intent mismatch.
    """
    
    st.header("🎯 Funnel Health")
    st.caption("*Where exactly are users dropping off?*")
    
    start_str = start_date.strftime('%Y%m%d')
    end_str = end_date.strftime('%Y%m%d')
    
    # Note: This requires properly configured GA4 events
    # For demonstration, we'll create a basic funnel using available data
    
    # Step 1: Get total sessions (funnel entry)
    sessions_query = f"""
    SELECT 
        CAST(SUM(CAST(sessions AS BIGINT)) AS BIGINT) as sessions
    FROM ga4_sessions
    WHERE date >= '{start_str}' AND date <= '{end_str}'
    """
    
    sessions_df = load_ga4_data(duckdb_path, sessions_query)
    total_sessions = int(sessions_df.iloc[0]['sessions']) if sessions_df is not None and not sessions_df.empty else 0
    
    # Step 2: Page views (users viewing content)
    pageviews_query = f"""
    SELECT 
        COUNT(DISTINCT date || '-' || pagePath) as page_interactions
    FROM ga4_page_performance
    WHERE date >= '{start_str}' AND date <= '{end_str}'
    """
    
    pageviews_df = load_ga4_data(duckdb_path, pageviews_query)
    page_interactions = int(pageviews_df.iloc[0]['page_interactions']) if pageviews_df is not None and not pageviews_df.empty else 0
    
    # Step 3: Engaged sessions (proxy for CTA interaction)
    engaged_query = f"""
    SELECT 
        CAST(SUM(CAST(sessions AS BIGINT)) AS BIGINT) as sessions,
        AVG(CAST(bounceRate AS DOUBLE)) as avg_bounce
    FROM ga4_sessions
    WHERE date >= '{start_str}' AND date <= '{end_str}'
    """
    
    engaged_df = load_ga4_data(duckdb_path, engaged_query)
    
    if engaged_df is not None and not engaged_df.empty:
        avg_bounce = float(engaged_df.iloc[0]['avg_bounce']) if engaged_df.iloc[0]['avg_bounce'] else 0.5
        engaged_sessions = int(total_sessions * (1 - avg_bounce))
    else:
        engaged_sessions = 0
    
    # Step 4: Conversions (would need event data - placeholder)
    conversions = 0  # Placeholder - needs ga4_event_data with conversion events
    
    # ========================================
    # Build Funnel Data
    # ========================================
    
    funnel_steps = [
        {
            'step': 1,
            'name': 'Session Started',
            'users': total_sessions,
            'icon': '👥'
        },
        {
            'step': 2,
            'name': 'Content Viewed',
            'users': page_interactions,
            'icon': '👁️'
        },
        {
            'step': 3,
            'name': 'Engaged (CTA Click)',
            'users': engaged_sessions,
            'icon': '🎯'
        },
        {
            'step': 4,
            'name': 'Conversion',
            'users': conversions,
            'icon': '✅'
        }
    ]
    
    # Calculate conversion rates and drop-offs
    for i in range(1, len(funnel_steps)):
        prev_users = funnel_steps[i-1]['users']
        current_users = funnel_steps[i]['users']
        
        if prev_users > 0:
            conversion_rate = (current_users / prev_users) * 100
            dropoff_rate = 100 - conversion_rate
        else:
            conversion_rate = 0
            dropoff_rate = 0
        
        funnel_steps[i]['conversion_rate'] = conversion_rate
        funnel_steps[i]['dropoff_rate'] = dropoff_rate
    
    # ========================================
    # Display Funnel Visualization
    # ========================================
    
    # Display as columns
    cols = st.columns(len(funnel_steps))
    
    for i, step in enumerate(funnel_steps):
        with cols[i]:
            st.markdown(f"### {step['icon']} Step {step['step']}")
            st.markdown(f"**{step['name']}**")
            st.metric(
                label="Users",
                value=f"{step['users']:,}",
                delta=None
            )
            
            if i > 0:
                st.caption(f"✅ {step['conversion_rate']:.1f}% converted")
                st.caption(f"❌ {step['dropoff_rate']:.1f}% dropped")
    
    st.divider()
    
    # ========================================
    # Funnel Insights
    # ========================================
    
    st.markdown("**💡 Funnel Insights:**")
    
    # Identify biggest drop-off point
    if len(funnel_steps) > 1:
        dropoffs = [(step['name'], step.get('dropoff_rate', 0)) for step in funnel_steps if 'dropoff_rate' in step]
        if dropoffs:
            biggest_dropoff = max(dropoffs, key=lambda x: x[1])
            
            if biggest_dropoff[1] > 70:
                st.error(
                    f"🔴 **Critical Drop-off**: {biggest_dropoff[1]:.0f}% of users drop off at '{biggest_dropoff[0]}'. "
                    "This is your primary optimization target."
                )
            elif biggest_dropoff[1] > 50:
                st.warning(
                    f"⚠️ **High Drop-off**: {biggest_dropoff[1]:.0f}% drop at '{biggest_dropoff[0]}'. "
                    "Consider A/B testing this step."
                )
    
    # Overall funnel health
    if total_sessions > 0:
        overall_conversion = (conversions / total_sessions) * 100 if conversions > 0 else 0
        st.info(
            f"📊 **Overall Funnel Conversion**: {overall_conversion:.2f}% of sessions convert. "
            f"Industry benchmark: 2-5% (depends on business model)."
        )
    
    # Configuration note
    st.info(
        "⚙️ **Note**: This funnel uses basic session data. For accurate conversion tracking, "
        "configure GA4 custom events for CTAs and conversion actions."
    )


# ============================================
# Component 5: Behavior & Engagement
# ============================================

def render_behavior_engagement(
    duckdb_path: str,
    start_date: date,
    end_date: date
):
    """
    Render Behavior & Engagement analysis.
    
    Components:
        A. Event performance (only important events)
        B. Engagement distribution
    
    What we're looking for:
        - Are users trying but failing? (high event count, low conversions)
        - Or not even engaging? (low event count)
    
    Why this matters:
        Reveals if users are confused (trying but failing) or 
        uninterested (not engaging at all). Different fixes needed.
    """
    
    st.header("🎮 Behavior & Engagement")
    st.caption("*Are users trying but failing, or not even engaging?*")
    
    # Check for required tables
    has_event_data = check_table_exists(duckdb_path, 'ga4_event_data')
    has_page_data = check_table_exists(duckdb_path, 'ga4_page_performance')
    
    if not has_event_data and not has_page_data:
        st.warning("""
        ⚠️ **Behavior data not available.**
        
        The `ga4_event_data` and `ga4_page_performance` tables are required for this section.
        
        Run the GA4 ETL to populate this data:
        ```bash
        python scripts/run_etl_unified.py --source ga4 --comprehensive --lifetime
        ```
        """)
        return
    
    start_str = start_date.strftime('%Y%m%d')
    end_str = end_date.strftime('%Y%m%d')
    
    # ========================================
    # A. Event Performance (Important Events Only)
    # ========================================
    
    st.subheader("A. Event Performance")
    st.caption("*Key user interactions (CTA clicks, form starts, scrolls)*")
    
    if has_event_data:
        events_query = f"""
        SELECT 
            eventName as event,
            CAST(SUM(CAST(eventCount AS BIGINT)) AS BIGINT) as event_count,
            CAST(SUM(CAST(totalUsers AS BIGINT)) AS BIGINT) as users,
            COUNT(DISTINCT date) as days
        FROM ga4_event_data
        WHERE date >= '{start_str}' AND date <= '{end_str}'
            AND eventName IS NOT NULL
        GROUP BY eventName
        ORDER BY event_count DESC
        LIMIT 20
        """
        
        events_df = load_ga4_data(duckdb_path, events_query, suppress_error=True)
    else:
        events_df = None
    
    if events_df is not None and not events_df.empty:
        # Calculate events per user
        events_df['events_per_user'] = events_df['event_count'] / events_df['users']
        
        # Format for display
        display_df = events_df.copy()
        display_df['event_count'] = display_df['event_count'].apply(lambda x: f"{int(x):,}")
        display_df['users'] = display_df['users'].apply(lambda x: f"{int(x):,}")
        display_df['events_per_user'] = display_df['events_per_user'].apply(lambda x: f"{x:.2f}")
        
        st.dataframe(
            display_df[['event', 'event_count', 'users', 'events_per_user']],
            use_container_width=True,
            hide_index=True,
            column_config={
                "event": "Event Name",
                "event_count": "Total Events",
                "users": "Users",
                "events_per_user": "Events/User"
            }
        )
        
        # Event insights
        st.markdown("**💡 What to look for:**")
        st.markdown("""
        - **High events/user on form_start but low on form_submit** → Form UX issue
        - **High scroll events but low CTA clicks** → Call-to-action not compelling
        - **Low events overall** → Users aren't engaging at all (content issue)
        """)
    else:
        st.info("⚙️ No custom event data available. Configure GA4 events to track CTA clicks, form interactions, and key actions.")
    
    st.divider()
    
    # ========================================
    # B. Engagement Distribution
    # ========================================
    
    st.subheader("B. Engagement Distribution")
    st.caption("*How long are users actually spending on site?*")
    
    if not has_page_data:
        st.info("⚙️ Session duration data requires `ga4_page_performance` table. Run GA4 ETL to populate.")
        return
    
    # Query session duration distribution
    # Note: This is simplified - would need custom event data for accurate bucketing
    duration_query = f"""
    SELECT 
        CASE 
            WHEN CAST(averageSessionDuration AS DOUBLE) < 10 THEN '0-10s'
            WHEN CAST(averageSessionDuration AS DOUBLE) < 30 THEN '10-30s'
            WHEN CAST(averageSessionDuration AS DOUBLE) < 60 THEN '30-60s'
            WHEN CAST(averageSessionDuration AS DOUBLE) < 120 THEN '1-2min'
            WHEN CAST(averageSessionDuration AS DOUBLE) < 300 THEN '2-5min'
            ELSE '5min+'
        END as duration_bucket,
        COUNT(*) as session_count
    FROM ga4_page_performance
    WHERE date >= '{start_str}' AND date <= '{end_str}'
        AND averageSessionDuration IS NOT NULL
    GROUP BY duration_bucket
    ORDER BY 
        CASE 
            WHEN duration_bucket = '0-10s' THEN 1
            WHEN duration_bucket = '10-30s' THEN 2
            WHEN duration_bucket = '30-60s' THEN 3
            WHEN duration_bucket = '1-2min' THEN 4
            WHEN duration_bucket = '2-5min' THEN 5
            ELSE 6
        END
    """
    
    duration_df = load_ga4_data(duckdb_path, duration_query, suppress_error=True)
    
    if duration_df is not None and not duration_df.empty:
        # Create bar chart
        st.bar_chart(duration_df.set_index('duration_bucket')['session_count'])
        
        # Interpretation
        total_sessions = duration_df['session_count'].sum()
        short_sessions = duration_df[duration_df['duration_bucket'] == '0-10s']['session_count'].sum()
        short_pct = (short_sessions / total_sessions * 100) if total_sessions > 0 else 0
        
        if short_pct > 50:
            st.warning(
                f"⚠️ **{short_pct:.0f}% of sessions are under 10 seconds**. "
                "Users are skimming, not engaging. Check page load speed and content relevance."
            )
        elif short_pct < 20:
            st.success(
                f"✅ Only {short_pct:.0f}% of sessions are under 10 seconds. "
                "Users are engaging with your content."
            )
        
        st.markdown("**💡 Interpretation:**")
        st.markdown("""
        - **High 0-10s sessions** → Skimmers or page load issues
        - **Peak at 30-60s or 1-2min** → Good engagement, users reading/watching
        - **Long tail (5min+)** → Deep engagement, serious users
        """)
    else:
        st.info("Session duration data not available for the selected period.")


# ============================================
# Component 6: User Segments
# ============================================

def render_user_segments(
    duckdb_path: str,
    start_date: date,
    end_date: date
):
    """
    Render User Segments analysis.
    
    3-4 opinionated segments:
        - New vs Returning
        - Mobile vs Desktop
        - Paid vs Organic
        - High-intent sessions (engaged + key event)
    
    Compare:
        - Engagement rate
        - Conversion rate
        - Funnel completion
    
    Why this matters:
        Shows which users deserve optimization attention,
        and which are already "good enough".
    """
    
    st.header("👥 User Segments")
    st.caption("*Which users deserve optimization attention?*")
    
    # Check for available tables
    available_tables = get_available_ga4_tables(duckdb_path)
    
    if not available_tables.get('ga4_sessions', False):
        st.warning("⚠️ **Session data not available.** Run GA4 ETL to populate data.")
        return
    
    start_str = start_date.strftime('%Y%m%d')
    end_str = end_date.strftime('%Y%m%d')
    
    # ========================================
    # Segment 1: New vs Returning Users
    # ========================================
    
    st.subheader("1. New vs Returning Users")
    
    # Calculate new vs returning (approximation using newUsers vs totalUsers)
    new_returning_query = f"""
    SELECT 
        CAST(SUM(CAST(newUsers AS BIGINT)) AS BIGINT) as new_users,
        CAST(SUM(CAST(totalUsers AS BIGINT)) AS BIGINT) as total_users,
        CAST(SUM(CAST(sessions AS BIGINT)) AS BIGINT) as sessions,
        AVG(CAST(bounceRate AS DOUBLE)) as bounce_rate
    FROM ga4_sessions
    WHERE date >= '{start_str}' AND date <= '{end_str}'
    """
    
    nr_df = load_ga4_data(duckdb_path, new_returning_query)
    
    if nr_df is not None and not nr_df.empty:
        row = nr_df.iloc[0]
        new_users = int(row['new_users']) if row['new_users'] else 0
        total_users = int(row['total_users']) if row['total_users'] else 0
        returning_users = max(total_users - new_users, 0)
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("👶 New Users", f"{new_users:,}")
            st.progress(new_users / total_users if total_users > 0 else 0)
        
        with col2:
            st.metric("🔄 Returning Users", f"{returning_users:,}")
            st.progress(returning_users / total_users if total_users > 0 else 0)
        
        with col3:
            new_pct = (new_users / total_users * 100) if total_users > 0 else 0
            st.metric("% New", f"{new_pct:.1f}%")
            
            if new_pct > 80:
                st.caption("⚠️ High new user %")
            elif new_pct < 30:
                st.caption("✅ Strong retention")
    
    st.divider()
    
    # ========================================
    # Segment 2: Mobile vs Desktop
    # ========================================
    
    st.subheader("2. Mobile vs Desktop Performance")
    
    if not available_tables.get('ga4_technology_data', False):
        st.info("⚙️ Device data requires `ga4_technology_data` table. Run GA4 ETL to populate.")
        device_df = None
    else:
        device_query = f"""
        SELECT 
            deviceCategory as device,
            CAST(SUM(CAST(sessions AS BIGINT)) AS BIGINT) as sessions,
            CAST(SUM(CAST(totalUsers AS BIGINT)) AS BIGINT) as users,
            CAST(SUM(CAST(screenPageViews AS BIGINT)) AS BIGINT) as page_views
        FROM ga4_technology_data
        WHERE date >= '{start_str}' AND date <= '{end_str}'
            AND deviceCategory IS NOT NULL
        GROUP BY deviceCategory
        ORDER BY sessions DESC
        """
        
        device_df = load_ga4_data(duckdb_path, device_query, suppress_error=True)
    
    if device_df is not None and not device_df.empty:
        # Calculate pages per session
        device_df['pages_per_session'] = device_df['page_views'] / device_df['sessions']
        
        # Display as metrics
        cols = st.columns(len(device_df))
        
        for i, (_, row) in enumerate(device_df.iterrows()):
            with cols[i]:
                device_icon = {
                    'desktop': '💻',
                    'mobile': '📱',
                    'tablet': '📲'
                }.get(row['device'].lower(), '📟')
                
                st.metric(
                    f"{device_icon} {row['device'].title()}",
                    f"{int(row['sessions']):,} sessions"
                )
                st.caption(f"{row['pages_per_session']:.2f} pages/session")
        
        # Device insights
        mobile_sessions = device_df[device_df['device'].str.lower() == 'mobile']['sessions'].sum()
        total_sessions = device_df['sessions'].sum()
        mobile_pct = (mobile_sessions / total_sessions * 100) if total_sessions > 0 else 0
        
        if mobile_pct > 60:
            st.info(f"📱 **{mobile_pct:.0f}% mobile traffic**. Mobile UX is critical.")
        elif mobile_pct < 20:
            st.info(f"💻 **{mobile_pct:.0f}% mobile traffic**. Desktop-focused audience.")
    
    st.divider()
    
    # ========================================
    # Segment 3: Paid vs Organic
    # ========================================
    
    st.subheader("3. Paid vs Organic Traffic")
    
    if not available_tables.get('ga4_traffic_overview', False):
        st.info("⚙️ Traffic source data requires `ga4_traffic_overview` table. Run GA4 ETL to populate.")
        po_df = None
    else:
        paid_organic_query = f"""
        SELECT 
            CASE 
                WHEN sessionMedium IN ('cpc', 'ppc', 'paidsearch', 'paid') THEN 'Paid'
                WHEN sessionMedium IN ('organic', 'referral', 'direct', '(none)') THEN 'Organic'
                ELSE 'Other'
            END as traffic_type,
            CAST(SUM(CAST(sessions AS BIGINT)) AS BIGINT) as sessions,
            AVG(CAST(bounceRate AS DOUBLE)) as bounce_rate
        FROM ga4_traffic_overview
        WHERE date >= '{start_str}' AND date <= '{end_str}'
            AND sessionMedium IS NOT NULL
        GROUP BY traffic_type
        ORDER BY sessions DESC
        """
        
        po_df = load_ga4_data(duckdb_path, paid_organic_query, suppress_error=True)
    
    if po_df is not None and not po_df.empty:
        # Calculate engagement rate
        po_df['engagement_rate'] = 1.0 - po_df['bounce_rate']
        
        # Display comparison
        cols = st.columns(len(po_df))
        
        for i, (_, row) in enumerate(po_df.iterrows()):
            with cols[i]:
                icon = {'Paid': '💰', 'Organic': '🔍', 'Other': '🌐'}.get(row['traffic_type'], '📊')
                
                st.metric(
                    f"{icon} {row['traffic_type']}",
                    f"{int(row['sessions']):,}"
                )
                st.caption(f"{row['engagement_rate']:.1%} engaged")
        
        # Comparison insight
        if len(po_df) >= 2:
            paid_row = po_df[po_df['traffic_type'] == 'Paid']
            organic_row = po_df[po_df['traffic_type'] == 'Organic']
            
            if not paid_row.empty and not organic_row.empty:
                paid_eng = paid_row.iloc[0]['engagement_rate']
                organic_eng = organic_row.iloc[0]['engagement_rate']
                
                if paid_eng < organic_eng - 0.1:
                    st.warning(
                        f"⚠️ **Paid traffic engagement ({paid_eng:.1%}) lags organic ({organic_eng:.1%})**. "
                        "Review ad targeting and landing pages."
                    )
                elif paid_eng > organic_eng + 0.1:
                    st.success(
                        f"✅ **Paid traffic engagement ({paid_eng:.1%}) beats organic ({organic_eng:.1%})**. "
                        "Strong ad targeting."
                    )


# ============================================
# Component 7: Geo & Device Reality Check
# ============================================

def render_geo_device_check(
    duckdb_path: str,
    start_date: date,
    end_date: date
):
    """
    Render Geo & Device Reality Check (light touch).
    
    Show only:
        - Sessions
        - Conversion rate
    
    By:
        - Country
        - Device category
    
    Why this matters:
        Spot misaligned geo targeting and catch mobile UX disasters early.
        Anything deeper belongs elsewhere.
    """
    
    st.header("🌍 Geo & Device Reality Check")
    st.caption("*Quick check for targeting issues and mobile UX disasters*")
    
    # Check for available tables
    available_tables = get_available_ga4_tables(duckdb_path)
    has_geo = available_tables.get('ga4_geographic_data', False)
    has_tech = available_tables.get('ga4_technology_data', False)
    
    if not has_geo and not has_tech:
        st.warning("""
        ⚠️ **Geographic and device data not available.**
        
        Run the GA4 ETL to populate this data:
        ```bash
        python scripts/run_etl_unified.py --source ga4 --comprehensive --lifetime
        ```
        """)
        return
    
    start_str = start_date.strftime('%Y%m%d')
    end_str = end_date.strftime('%Y%m%d')
    
    col1, col2 = st.columns(2)
    
    # ========================================
    # Geographic Performance
    # ========================================
    
    with col1:
        st.subheader("🌎 Geographic Distribution")
        
        if not has_geo:
            st.info("⚙️ Geographic data requires `ga4_geographic_data` table.")
            geo_df = None
        else:
            # Get ALL countries (not just top 10) for better visualization
            geo_query = f"""
            SELECT 
                country,
                CAST(SUM(CAST(sessions AS BIGINT)) AS BIGINT) as sessions,
                CAST(SUM(CAST(totalUsers AS BIGINT)) AS BIGINT) as users
            FROM ga4_geographic_data
            WHERE date >= '{start_str}' AND date <= '{end_str}'
                AND country IS NOT NULL
                AND country != '(not set)'
            GROUP BY country
            ORDER BY sessions DESC
            """
            
            geo_df = load_ga4_data(duckdb_path, geo_query, suppress_error=True)
        
        if geo_df is not None and not geo_df.empty:
            import plotly.express as px
            
            # Create choropleth world map
            fig_map = px.choropleth(
                geo_df,
                locations="country",
                locationmode="country names",
                color="sessions",
                hover_name="country",
                hover_data={"sessions": ":,", "users": ":,"},
                color_continuous_scale="Blues",
                title="Sessions by Country"
            )
            
            fig_map.update_layout(
                geo=dict(
                    showframe=False,
                    showcoastlines=True,
                    coastlinecolor="lightgray",
                    projection_type='natural earth'
                ),
                margin=dict(l=0, r=0, t=40, b=0),
                height=350,
                coloraxis_colorbar=dict(title="Sessions")
            )
            
            st.plotly_chart(fig_map, use_container_width=True)
            
            # Geographic concentration check
            top_country_sessions = int(geo_df.iloc[0]['sessions'])
            total_sessions = int(geo_df['sessions'].sum())
            concentration = (top_country_sessions / total_sessions * 100) if total_sessions > 0 else 0
            
            if concentration > 80:
                st.info(f"📍 {concentration:.0f}% traffic from {geo_df.iloc[0]['country']}. Highly concentrated.")
        else:
            st.info("No geographic data available.")
    
    # ========================================
    # Device Performance + Country Pie Chart
    # ========================================
    
    with col2:
        # Top Countries Pie Chart
        st.subheader("🥧 Top Countries Distribution")
        
        if geo_df is not None and not geo_df.empty:
            import plotly.express as px
            
            # Get top 8 countries + "Others"
            top_countries = geo_df.head(8).copy()
            other_sessions = geo_df.iloc[8:]['sessions'].sum() if len(geo_df) > 8 else 0
            
            if other_sessions > 0:
                other_row = pd.DataFrame([{'country': 'Others', 'sessions': other_sessions, 'users': 0}])
                top_countries = pd.concat([top_countries, other_row], ignore_index=True)
            
            # Create pie chart
            fig_pie = px.pie(
                top_countries,
                values='sessions',
                names='country',
                title='Sessions by Country',
                hole=0.4,  # Makes it a donut chart
                color_discrete_sequence=px.colors.qualitative.Set2
            )
            
            fig_pie.update_layout(
                margin=dict(l=0, r=0, t=40, b=0),
                height=300,
                showlegend=True,
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=-0.3,
                    xanchor="center",
                    x=0.5
                )
            )
            
            fig_pie.update_traces(
                textposition='inside',
                textinfo='percent+label',
                hovertemplate='<b>%{label}</b><br>Sessions: %{value:,}<extra></extra>'
            )
            
            st.plotly_chart(fig_pie, use_container_width=True)
        else:
            st.info("⚙️ Run comprehensive GA4 ETL to see geo distribution.")
        
        st.divider()
        
        # Device Performance
        st.subheader("📱 Device Performance")
        
        if not has_tech:
            st.info("⚙️ Device data requires `ga4_technology_data` table.")
            device_df = None
        else:
            device_query = f"""
            SELECT 
                deviceCategory as device,
                CAST(SUM(CAST(sessions AS BIGINT)) AS BIGINT) as sessions,
                CAST(SUM(CAST(totalUsers AS BIGINT)) AS BIGINT) as users
            FROM ga4_technology_data
            WHERE date >= '{start_str}' AND date <= '{end_str}'
                AND deviceCategory IS NOT NULL
            GROUP BY deviceCategory
            ORDER BY sessions DESC
            """
            
            device_df = load_ga4_data(duckdb_path, device_query, suppress_error=True)
        
        if device_df is not None and not device_df.empty:
            import plotly.express as px
            
            # Create pie chart for devices
            fig_device = px.pie(
                device_df,
                values='sessions',
                names='device',
                title='Sessions by Device',
                hole=0.4,
                color_discrete_map={
                    'desktop': '#4A90D9',
                    'mobile': '#67B26F',
                    'tablet': '#FFC75F'
                }
            )
            
            fig_device.update_layout(
                margin=dict(l=0, r=0, t=40, b=0),
                height=250,
                showlegend=True,
                legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5)
            )
            
            fig_device.update_traces(
                textposition='inside',
                textinfo='percent+label',
                hovertemplate='<b>%{label}</b><br>Sessions: %{value:,}<extra></extra>'
            )
            
            st.plotly_chart(fig_device, use_container_width=True)
            
            # Mobile UX check
            mobile_sessions = device_df[device_df['device'].str.lower() == 'mobile']['sessions'].sum()
            total_sessions = device_df['sessions'].sum()
            mobile_pct = (mobile_sessions / total_sessions * 100) if total_sessions > 0 else 0
            
            if mobile_pct > 50:
                st.warning(
                    f"📱 **{mobile_pct:.0f}% mobile traffic**. Test mobile UX rigorously."
                )


# ============================================
# Component 8: Trend Diagnostics
# ============================================

def render_trend_diagnostics(
    duckdb_path: str,
    start_date: date,
    end_date: date
):
    """
    Render Trend Diagnostics (GA-only truth).
    
    One clean time series:
        - Sessions
        - Engagement rate
        - Conversion rate
    
    Over 30-90 days.
    
    Interpretation patterns:
        - Sessions ↑ but CVR ↓ → intent problem
        - Engagement ↓ before sessions ↓ → UX/content regression
    
    Why this matters:
        Shows the TRUE story of your website health,
        separate from ad spend fluctuations.
    """
    
    st.header("📈 Trend Diagnostics")
    st.caption("*GA4-only truth: website health over time*")
    
    start_str = start_date.strftime('%Y%m%d')
    end_str = end_date.strftime('%Y%m%d')
    
    # Query daily trends
    trend_query = f"""
    SELECT 
        date,
        CAST(SUM(CAST(sessions AS BIGINT)) AS BIGINT) as sessions,
        AVG(CAST(bounceRate AS DOUBLE)) as bounce_rate,
        CAST(SUM(CAST(totalUsers AS BIGINT)) AS BIGINT) as users
    FROM ga4_sessions
    WHERE date >= '{start_str}' AND date <= '{end_str}'
    GROUP BY date
    ORDER BY date
    """
    
    trend_df = load_ga4_data(duckdb_path, trend_query)
    
    if trend_df is not None and not trend_df.empty:
        # Calculate engagement rate (inverse of bounce rate)
        trend_df['engagement_rate'] = (1.0 - trend_df['bounce_rate']) * 100
        
        # Convert date to datetime for proper plotting
        if trend_df['date'].dtype == 'object':
            trend_df['date'] = pd.to_datetime(trend_df['date'], format='%Y%m%d')
        
        # Create tabs for different views
        tab1, tab2, tab3 = st.tabs(["📊 Sessions", "✨ Engagement Rate", "📈 Combined View"])
        
        with tab1:
            st.line_chart(trend_df.set_index('date')['sessions'], use_container_width=True)
            st.caption("Daily sessions over time")
        
        with tab2:
            st.line_chart(trend_df.set_index('date')['engagement_rate'], use_container_width=True)
            st.caption("Daily engagement rate (%) over time")
        
        with tab3:
            # Normalize both metrics to 0-100 scale for comparison
            normalized_df = trend_df.set_index('date').copy()
            normalized_df['sessions_norm'] = (
                (normalized_df['sessions'] - normalized_df['sessions'].min()) / 
                (normalized_df['sessions'].max() - normalized_df['sessions'].min()) * 100
            )
            normalized_df['engagement_norm'] = normalized_df['engagement_rate']
            
            st.line_chart(
                normalized_df[['sessions_norm', 'engagement_norm']].rename(columns={
                    'sessions_norm': 'Sessions (normalized)',
                    'engagement_norm': 'Engagement Rate (%)'
                }),
                use_container_width=True
            )
            st.caption("Sessions and engagement rate on comparable scales")
        
        # ========================================
        # Trend Analysis
        # ========================================
        
        st.divider()
        st.subheader("📊 Trend Analysis")
        
        # Compare first half vs second half of period
        midpoint = len(trend_df) // 2
        first_half = trend_df.iloc[:midpoint]
        second_half = trend_df.iloc[midpoint:]
        
        first_sessions_avg = first_half['sessions'].mean()
        second_sessions_avg = second_half['sessions'].mean()
        sessions_change = calculate_percentage_change(second_sessions_avg, first_sessions_avg)
        
        first_engagement_avg = first_half['engagement_rate'].mean()
        second_engagement_avg = second_half['engagement_rate'].mean()
        engagement_change = calculate_percentage_change(second_engagement_avg, first_engagement_avg)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric(
                "Sessions Trend",
                f"{second_sessions_avg:.0f}/day",
                delta=f"{sessions_change:+.1f}%" if sessions_change else None
            )
        
        with col2:
            st.metric(
                "Engagement Trend",
                f"{second_engagement_avg:.1f}%",
                delta=f"{engagement_change:+.1f}%" if engagement_change else None
            )
        
        # Diagnostic insights
        st.markdown("**💡 Diagnostic Patterns:**")
        
        if sessions_change and engagement_change:
            if sessions_change > 10 and engagement_change < -5:
                st.warning(
                    "⚠️ **Sessions increasing but engagement dropping** → Intent problem. "
                    "You're getting more traffic, but it's lower quality."
                )
            elif sessions_change < -10 and engagement_change < -5:
                st.error(
                    "🔴 **Both sessions and engagement declining** → UX/content regression detected. "
                    "Check recent changes to site."
                )
            elif sessions_change > 10 and engagement_change > 5:
                st.success(
                    "✅ **Healthy growth**: Both sessions and engagement improving. "
                    "Keep doing what you're doing."
                )
            elif sessions_change < 0 and engagement_change > 0:
                st.info(
                    "📊 **Quality over quantity**: Sessions down but engagement up. "
                    "You're attracting fewer but better-qualified visitors."
                )
    else:
        st.warning("⚠️ No trend data available for the selected period.")


# ============================================
# Component 9: What Changed (Auto-Insights)
# ============================================

def render_what_changed(
    duckdb_path: str,
    start_date: date,
    end_date: date,
    prev_start_date: Optional[date],
    prev_end_date: Optional[date]
):
    """
    Render "What Changed" - GA-driven narrative.
    
    Auto-generated bullets based on logic:
        - "Mobile engagement ↓ 18% WoW on /download page"
        - "Paid traffic CVR ↓ while organic stable → landing mismatch"
        - "Returning users CVR ↑ 22% after pricing page update"
    
    Why this matters:
        This is where GA becomes INSIGHTFUL, not descriptive.
        Tells you what to investigate or double-down on.
    """
    
    st.header("🔍 What Changed")
    st.caption("*Auto-generated insights from GA4 data*")
    
    if not prev_start_date or not prev_end_date:
        st.info("📊 Enable comparison period in date picker to see change analysis.")
        return
    
    start_str = start_date.strftime('%Y%m%d')
    end_str = end_date.strftime('%Y%m%d')
    prev_start_str = prev_start_date.strftime('%Y%m%d')
    prev_end_str = prev_end_date.strftime('%Y%m%d')
    
    insights = []
    
    # ========================================
    # 1. Overall Traffic Change
    # ========================================
    
    traffic_query = f"""
    SELECT 
        CAST(SUM(CAST(sessions AS BIGINT)) AS BIGINT) as sessions,
        CAST(SUM(CAST(newUsers AS BIGINT)) AS BIGINT) as new_users,
        AVG(CAST(bounceRate AS DOUBLE)) as bounce_rate
    FROM ga4_sessions
    WHERE date >= '{{start}}' AND date <= '{{end}}'
    """
    
    current_traffic_df = load_ga4_data(duckdb_path, traffic_query.format(start=start_str, end=end_str))
    prev_traffic_df = load_ga4_data(duckdb_path, traffic_query.format(start=prev_start_str, end=prev_end_str))
    
    if current_traffic_df is not None and prev_traffic_df is not None:
        if not current_traffic_df.empty and not prev_traffic_df.empty:
            current_sessions = int(current_traffic_df.iloc[0]['sessions'] or 0)
            prev_sessions = int(prev_traffic_df.iloc[0]['sessions'] or 0)
            sessions_change = calculate_percentage_change(current_sessions, prev_sessions)
            
            current_bounce = float(current_traffic_df.iloc[0]['bounce_rate'] or 0)
            prev_bounce = float(prev_traffic_df.iloc[0]['bounce_rate'] or 0)
            bounce_change = calculate_percentage_change(current_bounce, prev_bounce)
            
            if sessions_change and abs(sessions_change) > 10:
                direction = "up" if sessions_change > 0 else "down"
                icon = "📈" if sessions_change > 0 else "📉"
                insights.append({
                    'icon': icon,
                    'title': f"Sessions {direction} {abs(sessions_change):.1f}%",
                    'detail': f"From {prev_sessions:,} to {current_sessions:,}",
                    'severity': 'success' if sessions_change > 0 else 'warning'
                })
            
            if bounce_change and abs(bounce_change) > 5:
                direction = "increased" if bounce_change > 0 else "decreased"
                icon = "⚠️" if bounce_change > 0 else "✅"
                insights.append({
                    'icon': icon,
                    'title': f"Bounce rate {direction} {abs(bounce_change):.1f}%",
                    'detail': f"From {prev_bounce:.1%} to {current_bounce:.1%}",
                    'severity': 'warning' if bounce_change > 0 else 'success'
                })
    
    # ========================================
    # 2. Source/Medium Changes
    # ========================================
    
    source_query = f"""
    SELECT 
        sessionSource || ' / ' || sessionMedium as source_medium,
        CAST(SUM(CAST(sessions AS BIGINT)) AS BIGINT) as sessions,
        AVG(CAST(bounceRate AS DOUBLE)) as bounce_rate
    FROM ga4_traffic_overview
    WHERE date >= '{{start}}' AND date <= '{{end}}'
        AND sessionSource IS NOT NULL 
        AND sessionMedium IS NOT NULL
    GROUP BY sessionSource, sessionMedium
    ORDER BY sessions DESC
    LIMIT 5
    """
    
    # Use suppress_error since ga4_traffic_overview may not exist
    current_source_df = load_ga4_data(duckdb_path, source_query.format(start=start_str, end=end_str), suppress_error=True)
    prev_source_df = load_ga4_data(duckdb_path, source_query.format(start=prev_start_str, end=prev_end_str), suppress_error=True)
    
    if current_source_df is not None and prev_source_df is not None:
        if not current_source_df.empty and not prev_source_df.empty:
            # Check for significant changes in top sources
            for _, current_row in current_source_df.iterrows():
                source_medium = current_row['source_medium']
                current_sessions = int(current_row['sessions'])
                
                prev_row = prev_source_df[prev_source_df['source_medium'] == source_medium]
                if not prev_row.empty:
                    prev_sessions = int(prev_row.iloc[0]['sessions'])
                    change = calculate_percentage_change(current_sessions, prev_sessions)
                    
                    if change and abs(change) > 20:
                        direction = "up" if change > 0 else "down"
                        icon = "🚀" if change > 0 else "📉"
                        insights.append({
                            'icon': icon,
                            'title': f"{source_medium} traffic {direction} {abs(change):.1f}%",
                            'detail': f"From {prev_sessions:,} to {current_sessions:,} sessions",
                            'severity': 'info'
                        })
    
    # ========================================
    # 3. Device Performance Changes
    # ========================================
    
    device_query = f"""
    SELECT 
        deviceCategory as device,
        CAST(SUM(CAST(sessions AS BIGINT)) AS BIGINT) as sessions
    FROM ga4_technology_data
    WHERE date >= '{{start}}' AND date <= '{{end}}'
        AND deviceCategory IS NOT NULL
    GROUP BY deviceCategory
    """
    
    # Use suppress_error since ga4_technology_data may not exist
    current_device_df = load_ga4_data(duckdb_path, device_query.format(start=start_str, end=end_str), suppress_error=True)
    prev_device_df = load_ga4_data(duckdb_path, device_query.format(start=prev_start_str, end=prev_end_str), suppress_error=True)
    
    if current_device_df is not None and prev_device_df is not None:
        if not current_device_df.empty and not prev_device_df.empty:
            for _, current_row in current_device_df.iterrows():
                device = current_row['device']
                current_sessions = int(current_row['sessions'])
                
                prev_row = prev_device_df[prev_device_df['device'] == device]
                if not prev_row.empty:
                    prev_sessions = int(prev_row.iloc[0]['sessions'])
                    change = calculate_percentage_change(current_sessions, prev_sessions)
                    
                    if change and abs(change) > 15:
                        direction = "up" if change > 0 else "down"
                        device_icon = {'desktop': '💻', 'mobile': '📱', 'tablet': '📲'}.get(device.lower(), '📟')
                        insights.append({
                            'icon': device_icon,
                            'title': f"{device.title()} traffic {direction} {abs(change):.1f}%",
                            'detail': f"From {prev_sessions:,} to {current_sessions:,} sessions",
                            'severity': 'info'
                        })
    
    # ========================================
    # Display Insights
    # ========================================
    
    if insights:
        # Display in a grid
        num_insights = len(insights)
        cols = st.columns(min(num_insights, 3))
        
        for i, insight in enumerate(insights[:6]):  # Show top 6 insights
            with cols[i % 3]:
                severity_colors = {
                    'success': ('success', insight['icon']),
                    'warning': ('warning', insight['icon']),
                    'error': ('error', insight['icon']),
                    'info': ('info', insight['icon'])
                }
                
                msg_type, icon = severity_colors.get(insight['severity'], ('info', insight['icon']))
                
                if msg_type == 'success':
                    st.success(f"{icon} **{insight['title']}**\n\n{insight['detail']}")
                elif msg_type == 'warning':
                    st.warning(f"{icon} **{insight['title']}**\n\n{insight['detail']}")
                elif msg_type == 'error':
                    st.error(f"{icon} **{insight['title']}**\n\n{insight['detail']}")
                else:
                    st.info(f"{icon} **{insight['title']}**\n\n{insight['detail']}")
    else:
        st.info("📊 No significant changes detected in this period. Data is stable.")
    
    # Summary recommendation
    st.divider()
    
    if insights:
        st.markdown("**🎯 Recommended Actions:**")
        
        # Generate action items based on insights
        actions = []
        
        for insight in insights:
            if 'bounce rate increased' in insight['title'].lower():
                actions.append("• Investigate recent website changes that may have degraded UX")
            elif 'traffic down' in insight['title'].lower():
                actions.append(f"• Review {insight['title'].split()[0]} acquisition strategy")
            elif 'traffic up' in insight['title'].lower():
                actions.append(f"• Scale successful {insight['title'].split()[0]} channel")
        
        if actions:
            for action in list(set(actions))[:5]:  # Unique actions, max 5
                st.markdown(action)
        else:
            st.markdown("• Continue monitoring metrics")
            st.markdown("• Test new optimization hypotheses")
    else:
        st.markdown("**✅ Current Status:**")
        st.markdown("• Metrics are stable")
        st.markdown("• No immediate action required")
        st.markdown("• Focus on proactive experimentation")


# ============================================
# Main GA4 Dashboard Integration
# ============================================

def render_ga4_bi_dashboard(duckdb_path: str):
    """
    Render the complete GA4 Business Intelligence Dashboard.
    
    This is the main entry point that ties all GA4 components together
    following the mental model:
        1. Are the right people coming? (acquisition quality)
        2. Are they doing what we expect? (behavior & friction)
        3. Where are we leaking value? (drop-offs & opportunities)
    
    Args:
        duckdb_path: Path to DuckDB database
    
    Usage:
        from app.components.ga4_analytics import render_ga4_bi_dashboard
        render_ga4_bi_dashboard(duckdb_path)
    """
    
    # Import date picker component
    from app.components.date_picker import render_date_range_picker
    
    # Page header
    st.title("📊 GA4 Business Intelligence Dashboard")
    st.markdown("""
    *GA4 data answers three business questions:*
    - **Are the right people coming?** (acquisition quality)
    - **Are they doing what we expect?** (behavior & friction)
    - **Where are we leaking value?** (drop-offs & opportunities)
    """)
    
    st.divider()
    
    # ========================================
    # Date Range Selection
    # ========================================
    
    start_date, end_date, prev_start_date, prev_end_date = render_date_range_picker(
        key="ga4_dashboard",
        default_days=30,
        max_days=365,
        show_comparison=True
    )
    
    st.divider()
    
    # ========================================
    # Component 1: GA4 Executive Summary
    # ========================================
    
    with st.container():
        render_ga4_executive_summary(
            duckdb_path,
            start_date,
            end_date,
            prev_start_date,
            prev_end_date
        )
    
    st.divider()
    
    # ========================================
    # Component 2: Acquisition Quality
    # ========================================
    
    with st.container():
        render_acquisition_quality(
            duckdb_path,
            start_date,
            end_date
        )
    
    st.divider()
    
    # ========================================
    # Component 3: Landing Page Performance
    # ========================================
    
    with st.container():
        render_landing_page_performance(
            duckdb_path,
            start_date,
            end_date
        )
    
    st.divider()
    
    # ========================================
    # Component 4: Funnel Health
    # ========================================
    
    with st.container():
        render_funnel_health(
            duckdb_path,
            start_date,
            end_date
        )
    
    st.divider()
    
    # ========================================
    # Component 5: Behavior & Engagement
    # ========================================
    
    with st.container():
        render_behavior_engagement(
            duckdb_path,
            start_date,
            end_date
        )
    
    st.divider()
    
    # ========================================
    # Component 6: User Segments
    # ========================================
    
    with st.container():
        render_user_segments(
            duckdb_path,
            start_date,
            end_date
        )
    
    st.divider()
    
    # ========================================
    # Component 7: Geo & Device Reality Check
    # ========================================
    
    with st.container():
        render_geo_device_check(
            duckdb_path,
            start_date,
            end_date
        )
    
    st.divider()
    
    # ========================================
    # Component 8: Trend Diagnostics
    # ========================================
    
    with st.container():
        render_trend_diagnostics(
            duckdb_path,
            start_date,
            end_date
        )
    
    st.divider()
    
    # ========================================
    # Component 9: What Changed
    # ========================================
    
    with st.container():
        render_what_changed(
            duckdb_path,
            start_date,
            end_date,
            prev_start_date,
            prev_end_date
        )
    
    st.divider()
    
    # ========================================
    # Footer: Dashboard Principles
    # ========================================
    
    with st.expander("💡 Dashboard Design Principles", expanded=False):
        st.markdown("""
        ### What Makes This Dashboard Different
        
        **This is NOT just "what happened"** — it's **what broke, what worked, and what to fix next**.
        
        #### Mental Model
        
        1. **Are the right people coming?** (Acquisition Quality)
           - High sessions + low engagement = junk traffic → reduce spend
           - Low sessions + high conversion = scale candidate → increase spend
        
        2. **Are they doing what we expect?** (Behavior & Friction)
           - High event count but low conversion = trying but failing → fix UX
           - Low event count overall = not engaging → fix content/value prop
        
        3. **Where are we leaking value?** (Drop-offs & Opportunities)
           - Funnel shows exact drop-off points → prioritize fixes
           - Opportunity scoring shows which pages deliver biggest lift if improved
        
        #### What's Excluded (Deliberately)
        
        - ❌ Average session duration (misleading metric)
        - ❌ Pageviews as primary KPI (vanity metric)
        - ❌ All events (only important events shown)
        - ❌ All dimensions (only actionable ones)
        - ❌ Raw exploration tables (too much noise)
        - ❌ Anything you can't act on
        
        #### Key Principles
        
        1. **GA4 BI is about insight, not description**
        2. **Every widget must answer: what broke, what worked, or what to fix**
        3. **Quality over quantity**: Few meaningful metrics > many vanity metrics
        4. **Actionable**: Each section leads to a clear next action
        5. **Contextual**: Always compare periods to spot changes early
        
        #### Interpretation Guide
        
        **Traffic Quality Insights:**
        - ✅ Winner: High volume + high quality → Keep investing
        - 🚀 Scale: Low volume + high quality → Increase spend
        - 🔴 Junk: High volume + low quality → Reduce or cut
        - 🟡 Monitor: Medium quality → Optimize landing pages
        
        **Funnel Health:**
        - Drop-off >70% at any step = critical issue
        - Drop-off 50-70% = high-priority optimization
        - Drop-off <50% = healthy, test for marginal gains
        
        **Engagement Distribution:**
        - Spike at 0-10s = skimmers or speed issues
        - Peak at 30-60s or 1-2min = good engagement
        - Long tail (5min+) = deep engagement, serious users
        
        **Trend Patterns:**
        - Sessions ↑ + CVR ↓ = intent problem (wrong audience)
        - Engagement ↓ before sessions ↓ = UX regression (fix before it gets worse)
        - Both ↑ = healthy growth (scale what's working)
        - Sessions ↓ + engagement ↑ = quality over quantity (acceptable trade-off)
        
        ---
        
        *Dashboard built following GA4 BI best practices for actionable insights.*
        """)


# ============================================
# Utility Functions for Testing
# ============================================

def check_ga4_data_availability(duckdb_path: str) -> Dict[str, bool]:
    """
    Check which GA4 tables have data.
    
    Returns:
        Dictionary mapping table names to availability (True/False)
    
    Useful for:
        - Debugging ETL issues
        - Determining which dashboard sections can be shown
    """
    
    tables = {
        'ga4_sessions': False,
        'ga4_traffic_overview': False,
        'ga4_page_performance': False,
        'ga4_geographic_data': False,
        'ga4_technology_data': False,
        'ga4_event_data': False
    }
    
    try:
        conn = duckdb.connect(duckdb_path, read_only=True)
        
        # Check each table
        for table in tables.keys():
            try:
                result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                tables[table] = result[0] > 0 if result else False
            except:
                tables[table] = False
        
        conn.close()
    except:
        pass
    
    return tables
