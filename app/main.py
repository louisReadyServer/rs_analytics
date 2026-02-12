"""
rs_analytics Streamlit Dashboard

Main entry point for the Streamlit analytics dashboard.
This app provides:
- GA4 Analytics data visualization
- Google Search Console (SEO) data visualization
- Google Ads (PPC) data visualization
- Meta (Facebook) Ads visualization
- Twitter/X analytics
- ETL Control Panel (manual data pulls)
- ETL status monitoring
- Configuration validation
- Connection testing

Usage:
    streamlit run app/main.py
    
Security:
    - Configuration is validated at startup
    - Credentials are NEVER displayed in the UI
    - All sensitive data handling follows security best practices
"""

import sys
import subprocess
import threading
import queue
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple

import streamlit as st
import pandas as pd
import duckdb

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Import dashboard components
from app.components.executive_dashboard import render_executive_dashboard
from app.components.app_analytics import render_app_analytics
from app.components.appsflyer_dashboard import render_appsflyer_dashboard
from app.components.glossary import TERM_TOOLTIPS

# Lifecycle mega-pages (Phase 3)
from app.components.lifecycle_acquire import render_acquire_page
from app.components.lifecycle_activate import render_activate_page
from app.components.lifecycle_monetize import render_monetize_page

# ============================================
# Page Configuration
# ============================================
st.set_page_config(
    page_title="rs_analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)


# ============================================
# Configuration Loading
# ============================================
@st.cache_resource
def load_ga4_configuration():
    """Load and validate GA4 configuration."""
    try:
        from etl.config import get_config, ConfigurationError
        config = get_config()
        return config, None
    except Exception as e:
        return None, str(e)


@st.cache_resource
def load_gsc_configuration():
    """Load and validate GSC configuration."""
    try:
        from etl.gsc_config import get_gsc_config
        config = get_gsc_config()
        return config, None
    except Exception as e:
        return None, str(e)


@st.cache_resource
def load_gads_configuration():
    """Load and validate Google Ads configuration."""
    try:
        from etl.gads_config import get_gads_config
        config = get_gads_config()
        return config, None
    except Exception as e:
        return None, str(e)


@st.cache_resource
def load_meta_configuration():
    """Load and validate Meta Ads configuration."""
    try:
        from etl.meta_config import get_meta_config
        config = get_meta_config()
        return config, None
    except Exception as e:
        return None, str(e)


# ============================================
# Data Loading Functions
# ============================================
@st.cache_data(ttl=300)
def load_duckdb_data(duckdb_path: str, query: str) -> Optional[pd.DataFrame]:
    """Load data from DuckDB with caching."""
    try:
        conn = duckdb.connect(duckdb_path, read_only=True)
        df = conn.execute(query).fetchdf()
        conn.close()
        return df
    except Exception as e:
        return None


@st.cache_resource
def initialize_views(duckdb_path: str) -> bool:
    """
    Initialize silver/gold views in the database if they don't exist.
    
    Returns:
        True if views were initialized successfully
    """
    views_sql_path = project_root / 'data' / 'views' / 'schema_views.sql'
    
    if not views_sql_path.exists():
        return False
    
    try:
        conn = duckdb.connect(duckdb_path)
        
        # Check if views already exist
        existing_views = set()
        try:
            result = conn.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_type = 'VIEW'
            """).fetchall()
            existing_views = {row[0] for row in result}
        except:
            pass
        
        # If we already have silver views, skip initialization
        if 'ga4_sessions_v' in existing_views or 'gsc_daily_totals_v' in existing_views:
            conn.close()
            return True
        
        # Read and execute views SQL
        with open(views_sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Split into individual statements and execute
        # Remove comments
        lines = []
        for line in sql_content.split('\n'):
            stripped = line.strip()
            if not stripped.startswith('--'):
                lines.append(line)
        
        content = '\n'.join(lines)
        
        # Find and execute CREATE VIEW statements
        import re
        view_pattern = re.compile(
            r'CREATE\s+OR\s+REPLACE\s+VIEW\s+\w+\s+AS\s+SELECT.*?;',
            re.IGNORECASE | re.DOTALL
        )
        
        for match in view_pattern.finditer(content):
            stmt = match.group(0)
            try:
                conn.execute(stmt)
            except Exception as e:
                # View creation failed, likely because source table doesn't exist
                pass
        
        conn.close()
        return True
        
    except Exception as e:
        return False


def check_views_exist(duckdb_path: str) -> bool:
    """Check if silver views exist in the database."""
    try:
        conn = duckdb.connect(duckdb_path, read_only=True)
        result = conn.execute("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_type = 'VIEW' AND table_name LIKE '%_v'
        """).fetchone()
        conn.close()
        return result[0] > 0 if result else False
    except:
        return False


def get_table_info(duckdb_path: str) -> dict:
    """Get information about all tables in the database."""
    try:
        conn = duckdb.connect(duckdb_path, read_only=True)
        tables_df = conn.execute("SHOW TABLES").fetchdf()
        
        table_info = {}
        for table in tables_df['name'].tolist():
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                table_info[table] = count
            except:
                table_info[table] = 0
        
        conn.close()
        return table_info
    except:
        return {}


def check_gsc_data_exists(duckdb_path: str) -> Tuple[bool, int, list]:
    """Check if GSC data exists in the database."""
    gsc_tables = [
        'gsc_queries', 'gsc_pages', 'gsc_countries', 'gsc_devices',
        'gsc_search_appearance', 'gsc_query_page', 'gsc_daily_totals'
    ]
    
    try:
        conn = duckdb.connect(duckdb_path, read_only=True)
        tables_df = conn.execute("SHOW TABLES").fetchdf()
        existing_tables = tables_df['name'].tolist()
        
        found_tables = [t for t in gsc_tables if t in existing_tables]
        
        total_rows = 0
        for table in found_tables:
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                total_rows += count
            except:
                pass
        
        conn.close()
        return len(found_tables) > 0, total_rows, found_tables
    except:
        return False, 0, []


def check_gads_data_exists(duckdb_path: str) -> Tuple[bool, int, list]:
    """Check if Google Ads data exists in the database."""
    gads_tables = [
        'gads_daily_summary', 'gads_campaigns', 'gads_ad_groups', 
        'gads_keywords', 'gads_ads', 'gads_devices', 
        'gads_geographic', 'gads_hourly', 'gads_conversions'
    ]
    
    try:
        conn = duckdb.connect(duckdb_path, read_only=True)
        tables_df = conn.execute("SHOW TABLES").fetchdf()
        existing_tables = tables_df['name'].tolist()
        
        found_tables = [t for t in gads_tables if t in existing_tables]
        
        total_rows = 0
        for table in found_tables:
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                total_rows += count
            except:
                pass
        
        conn.close()
        return len(found_tables) > 0, total_rows, found_tables
    except:
        return False, 0, []


def check_meta_data_exists(duckdb_path: str) -> Tuple[bool, int, list]:
    """Check if Meta Ads data exists in the database."""
    meta_tables = [
        'meta_daily_account', 'meta_campaigns', 'meta_campaign_insights',
        'meta_adsets', 'meta_adset_insights', 'meta_ads', 'meta_ad_insights',
        'meta_geographic', 'meta_devices', 'meta_demographics'
    ]
    
    try:
        conn = duckdb.connect(duckdb_path, read_only=True)
        tables_df = conn.execute("SHOW TABLES").fetchdf()
        existing_tables = tables_df['name'].tolist()
        
        found_tables = [t for t in meta_tables if t in existing_tables]
        
        total_rows = 0
        for table in found_tables:
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                total_rows += count
            except:
                pass
        
        conn.close()
        return len(found_tables) > 0, total_rows, found_tables
    except:
        return False, 0, []


def check_twitter_data_exists(duckdb_path: str) -> Tuple[bool, int, list]:
    """Check if Twitter data exists in the database."""
    twitter_tables = ['twitter_profile', 'twitter_tweets', 'twitter_daily_metrics']
    
    try:
        conn = duckdb.connect(duckdb_path, read_only=True)
        tables_df = conn.execute("SHOW TABLES").fetchdf()
        existing_tables = tables_df['name'].tolist()
        
        found_tables = [t for t in twitter_tables if t in existing_tables]
        
        total_rows = 0
        for table in found_tables:
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                total_rows += count
            except:
                pass
        
        conn.close()
        return len(found_tables) > 0, total_rows, found_tables
    except:
        return False, 0, []


@st.cache_resource
def load_twitter_configuration():
    """Load Twitter configuration (cached)."""
    try:
        from etl.twitter_config import get_twitter_config
        return get_twitter_config(), None
    except Exception as e:
        return None, str(e)


# ============================================
# GA4 Dashboard Page
# ============================================
def render_ga4_dashboard(config, duckdb_path: str):
    """
    Render the GA4 Business Intelligence Dashboard.
    
    This uses the new comprehensive GA4 BI dashboard with:
        - Executive Summary (GA4-only KPIs)
        - Acquisition Quality Analysis
        - Landing Page Performance with Opportunity Scoring
        - Funnel Health Visualization
        - Behavior & Engagement Analysis
        - User Segment Comparison
        - Geo & Device Reality Check
        - Trend Diagnostics
        - Auto-generated "What Changed" Insights
    """
    
    # Check if data exists
    table_info = get_table_info(duckdb_path)
    ga4_tables = [t for t in table_info.keys() if t.startswith('ga4_')]
    
    if not ga4_tables or sum(table_info.get(t, 0) for t in ga4_tables) == 0:
        st.info("""
        **No GA4 data available yet.**
        
        Run the ETL pipeline to populate the database:
        ```bash
        python scripts/run_etl_unified.py --source ga4 --lookback-days 30
        ```
        
        Or for comprehensive data:
        ```bash
        python scripts/run_etl_unified.py --source ga4 --comprehensive --lifetime
        ```
        """)
        return
    
    # Import and render the new comprehensive GA4 dashboard
    from app.components.ga4_analytics import render_ga4_bi_dashboard
    
    render_ga4_bi_dashboard(duckdb_path)


# ============================================
# GSC Dashboard Page
# ============================================
def render_gsc_dashboard(gsc_config, duckdb_path: str):
    """Render the Google Search Console dashboard."""
    
    st.header("🔍 Google Search Console Dashboard")
    
    # Check if GSC data exists
    has_data, total_rows, gsc_tables = check_gsc_data_exists(duckdb_path)
    
    if not has_data:
        st.info("""
        **No GSC data available yet.**
        
        Run the GSC ETL pipeline to populate the database:
        ```bash
        python scripts/run_etl_gsc.py --lifetime
        ```
        
        First, test your GSC connection:
        ```bash
        python scripts/test_gsc_connection.py
        ```
        """)
        
        if gsc_config:
            st.caption(f"Configured site: {gsc_config.site_url}")
        return
    
    # Show available data summary
    st.success(f"GSC data loaded: {total_rows:,} total rows across {len(gsc_tables)} tables")
    
    # Import date picker component
    from app.components.date_picker import render_date_range_picker
    
    # Date range filter using calendar picker
    start_date, end_date, _, _ = render_date_range_picker(
        key="gsc_dashboard",
        default_days=30,
        max_days=365,
        show_comparison=False
    )
    
    # Convert to string for SQL
    date_cutoff = start_date.strftime('%Y-%m-%d')
    
    st.divider()
    
    # Key SEO Metrics
    st.subheader("📈 Key SEO Metrics")
    
    if 'gsc_daily_totals' in gsc_tables:
        totals_query = f"""
        SELECT 
            SUM(clicks) as total_clicks,
            SUM(impressions) as total_impressions,
            AVG(ctr) as avg_ctr,
            AVG(avg_position) as avg_position
        FROM gsc_daily_totals_v
        WHERE date_day >= '{date_cutoff}'
        """
        
        totals_df = load_duckdb_data(duckdb_path, totals_query)
        
        if totals_df is not None and not totals_df.empty:
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Clicks", f"{int(totals_df['total_clicks'].iloc[0] or 0):,}",
                         help=TERM_TOOLTIPS.get("Clicks"))
            with col2:
                st.metric("Total Impressions", f"{int(totals_df['total_impressions'].iloc[0] or 0):,}",
                         help=TERM_TOOLTIPS.get("Impressions"))
            with col3:
                st.metric("Average CTR", f"{float(totals_df['avg_ctr'].iloc[0] or 0):.2%}",
                         help=TERM_TOOLTIPS.get("CTR"))
            with col4:
                st.metric("Avg Position", f"{float(totals_df['avg_position'].iloc[0] or 0):.1f}",
                         help=TERM_TOOLTIPS.get("Average Position"))
    
    st.divider()
    
    # Performance Over Time
    st.subheader("📊 Performance Over Time")
    
    if 'gsc_daily_totals' in gsc_tables:
        time_query = f"""
        SELECT date_day as date, clicks, impressions
        FROM gsc_daily_totals_v WHERE date_day >= '{date_cutoff}' ORDER BY date_day
        """
        time_df = load_duckdb_data(duckdb_path, time_query)
        if time_df is not None and not time_df.empty:
            st.line_chart(time_df.set_index('date'))
    
    st.divider()
    
    # Top Queries and Pages
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🔑 Top Search Queries")
        if 'gsc_queries' in gsc_tables:
            queries_query = f"""
            SELECT query, SUM(clicks) as clicks, SUM(impressions) as impressions
            FROM gsc_queries_v WHERE date_day >= '{date_cutoff}' AND query IS NOT NULL
            GROUP BY query ORDER BY clicks DESC LIMIT 15
            """
            queries_df = load_duckdb_data(duckdb_path, queries_query)
            if queries_df is not None and not queries_df.empty:
                st.dataframe(queries_df, use_container_width=True, hide_index=True)
    
    with col2:
        st.subheader("📄 Top Pages")
        if 'gsc_pages' in gsc_tables:
            pages_query = f"""
            SELECT page, SUM(clicks) as clicks, SUM(impressions) as impressions
            FROM gsc_pages_v WHERE date_day >= '{date_cutoff}' AND page IS NOT NULL
            GROUP BY page ORDER BY clicks DESC LIMIT 15
            """
            pages_df = load_duckdb_data(duckdb_path, pages_query)
            if pages_df is not None and not pages_df.empty:
                display_df = pages_df.copy()
                display_df['page'] = display_df['page'].apply(lambda x: x.split('/')[-1] if x and len(x) > 40 else x)
                st.dataframe(display_df, use_container_width=True, hide_index=True)
    
    # Raw Data Explorer
    with st.expander("📋 Explore Raw GSC Data"):
        table_choice = st.selectbox("Select Table", options=gsc_tables, key="gsc_table_choice")
        if table_choice:
            raw_df = load_duckdb_data(duckdb_path, f"SELECT * FROM {table_choice} LIMIT 1000")
            if raw_df is not None:
                st.dataframe(raw_df, use_container_width=True)


# ============================================
# Google Ads Dashboard Page
# ============================================
def render_gads_dashboard(gads_config, duckdb_path: str):
    """
    Render the Google Ads (PPC) dashboard with comprehensive analytics.
    
    Features:
    - Key PPC metrics with period comparison
    - Campaign performance with efficiency scoring (A-F grades)
    - Top keywords analysis with ROAS and CPA
    - Device performance breakdown
    - Ad group performance
    - Geographic performance with world map
    - Unified trend charts with tabs
    """
    
    st.header("💰 Google Ads Dashboard")
    
    # Check if Google Ads data exists
    has_data, total_rows, gads_tables = check_gads_data_exists(duckdb_path)
    
    if not has_data:
        st.info("""
        **No Google Ads data available yet.**
        
        Run the Google Ads ETL pipeline to populate the database:
        ```bash
        python scripts/run_etl_gads.py --lifetime
        ```
        
        First, test your Google Ads connection:
        ```bash
        python scripts/test_gads_connection.py
        ```
        """)
        
        if gads_config:
            st.caption(f"Configured Customer ID: {gads_config.customer_id}")
        return
    
    # Show available data summary
    st.success(f"Google Ads data loaded: {total_rows:,} total rows across {len(gads_tables)} tables")
    
    # Import date picker component
    from app.components.date_picker import render_date_range_picker
    
    # Date range filter using calendar picker
    start_date, end_date, _, _ = render_date_range_picker(
        key="gads_dashboard",
        default_days=30,
        max_days=365,
        show_comparison=False
    )
    
    # Convert to strings for SQL
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    # Calculate previous period for comparison
    date_range_days = (end_date - start_date).days + 1
    prev_start = start_date - timedelta(days=date_range_days)
    prev_end = start_date - timedelta(days=1)
    prev_start_str = prev_start.strftime('%Y-%m-%d')
    prev_end_str = prev_end.strftime('%Y-%m-%d')
    
    st.divider()
    
    # ========================================
    # Helper function to query with view fallback
    # ========================================
    def query_gads_table(base_table: str, query_template: str, date_col: str = 'date') -> pd.DataFrame:
        """
        Query Google Ads data with fallback from view to base table.
        
        Args:
            base_table: Base table name (e.g., 'gads_daily_summary')
            query_template: SQL query with {table} and {date_col} placeholders
            date_col: Column name for date filtering
        
        Returns:
            DataFrame with query results
        """
        # Try view first, then base table
        view_name = f"{base_table}_v"
        
        # First try the view (assumes date_day column)
        try:
            view_query = query_template.format(table=view_name, date_col='date_day')
            result = load_duckdb_data(duckdb_path, view_query)
            if result is not None and not result.empty:
                return result
        except Exception:
            pass
        
        # Fallback to base table (uses 'date' column)
        try:
            base_query = query_template.format(table=base_table, date_col=date_col)
            return load_duckdb_data(duckdb_path, base_query)
        except Exception:
            return None
    
    # ========================================
    # 📈 Key PPC Metrics
    # ========================================
    st.subheader("📈 Key PPC Metrics")
    
    if 'gads_daily_summary' in gads_tables:
        # Current period query
        summary_query = f"""
        SELECT 
            SUM(impressions) as total_impressions,
            SUM(clicks) as total_clicks,
            SUM(cost) as total_cost,
            CASE WHEN SUM(impressions) > 0 THEN SUM(clicks) * 1.0 / SUM(impressions) ELSE 0 END as avg_ctr,
            SUM(conversions) as total_conversions,
            SUM(conversions_value) as total_conversion_value,
            CASE WHEN SUM(clicks) > 0 THEN SUM(cost) / SUM(clicks) ELSE 0 END as avg_cpc,
            CASE WHEN SUM(impressions) > 0 THEN SUM(cost) / SUM(impressions) * 1000 ELSE 0 END as avg_cpm,
            CASE WHEN SUM(conversions) > 0 THEN SUM(cost) / SUM(conversions) ELSE 0 END as avg_cpa,
            CASE WHEN SUM(cost) > 0 THEN SUM(conversions_value) / SUM(cost) ELSE 0 END as roas
        FROM gads_daily_summary
        WHERE date >= '{start_date_str}' AND date <= '{end_date_str}'
        """
        
        # Previous period query for delta
        prev_query = f"""
        SELECT 
            SUM(impressions) as total_impressions,
            SUM(clicks) as total_clicks,
            SUM(cost) as total_cost,
            CASE WHEN SUM(impressions) > 0 THEN SUM(clicks) * 1.0 / SUM(impressions) ELSE 0 END as avg_ctr,
            SUM(conversions) as total_conversions,
            SUM(conversions_value) as total_conversion_value
        FROM gads_daily_summary
        WHERE date >= '{prev_start_str}' AND date <= '{prev_end_str}'
        """
        
        summary_df = load_duckdb_data(duckdb_path, summary_query)
        prev_df = load_duckdb_data(duckdb_path, prev_query)
        
        if summary_df is not None and not summary_df.empty:
            # Extract current values
            impressions = int(summary_df['total_impressions'].iloc[0] or 0)
            clicks = int(summary_df['total_clicks'].iloc[0] or 0)
            cost = float(summary_df['total_cost'].iloc[0] or 0)
            ctr = float(summary_df['avg_ctr'].iloc[0] or 0)
            conversions = float(summary_df['total_conversions'].iloc[0] or 0)
            conv_value = float(summary_df['total_conversion_value'].iloc[0] or 0)
            avg_cpc = float(summary_df['avg_cpc'].iloc[0] or 0)
            avg_cpm = float(summary_df['avg_cpm'].iloc[0] or 0)
            avg_cpa = float(summary_df['avg_cpa'].iloc[0] or 0)
            roas = float(summary_df['roas'].iloc[0] or 0)
            
            # Calculate deltas if previous period data exists
            prev_impressions = int(prev_df['total_impressions'].iloc[0] or 0) if prev_df is not None and not prev_df.empty else 0
            prev_clicks = int(prev_df['total_clicks'].iloc[0] or 0) if prev_df is not None and not prev_df.empty else 0
            prev_cost = float(prev_df['total_cost'].iloc[0] or 0) if prev_df is not None and not prev_df.empty else 0
            prev_ctr = float(prev_df['avg_ctr'].iloc[0] or 0) if prev_df is not None and not prev_df.empty else 0
            prev_conversions = float(prev_df['total_conversions'].iloc[0] or 0) if prev_df is not None and not prev_df.empty else 0
            prev_conv_value = float(prev_df['total_conversion_value'].iloc[0] or 0) if prev_df is not None and not prev_df.empty else 0
            
            # Calculate percentage changes
            def calc_delta(current, previous):
                if previous > 0:
                    return ((current - previous) / previous) * 100
                return None
            
            delta_impressions = calc_delta(impressions, prev_impressions)
            delta_clicks = calc_delta(clicks, prev_clicks)
            delta_cost = calc_delta(cost, prev_cost)
            delta_ctr = calc_delta(ctr, prev_ctr)
            delta_conversions = calc_delta(conversions, prev_conversions)
            delta_conv_value = calc_delta(conv_value, prev_conv_value)
            
            # Row 1: Volume metrics
            col1, col2, col3, col4, col5, col6 = st.columns(6)
            
            with col1:
                delta_str = f"{delta_impressions:+.1f}%" if delta_impressions is not None else None
                st.metric("Impressions", f"{impressions:,}", delta=delta_str, help=TERM_TOOLTIPS.get("Impressions"))
            
            with col2:
                delta_str = f"{delta_clicks:+.1f}%" if delta_clicks is not None else None
                st.metric("Clicks", f"{clicks:,}", delta=delta_str, help=TERM_TOOLTIPS.get("Clicks"))
            
            with col3:
                delta_str = f"{delta_cost:+.1f}%" if delta_cost is not None else None
                st.metric("Spend", f"${cost:,.2f}", delta=delta_str, delta_color="inverse", help=TERM_TOOLTIPS.get("Spend"))
            
            with col4:
                delta_str = f"{delta_ctr:+.1f}%" if delta_ctr is not None else None
                st.metric("CTR", f"{ctr:.2%}", delta=delta_str, help=TERM_TOOLTIPS.get("CTR"))
            
            with col5:
                delta_str = f"{delta_conversions:+.1f}%" if delta_conversions is not None else None
                st.metric("Conversions", f"{conversions:,.1f}", delta=delta_str, help=TERM_TOOLTIPS.get("Conversions"))
            
            with col6:
                delta_str = f"{delta_conv_value:+.1f}%" if delta_conv_value is not None else None
                st.metric("Conv. Value", f"${conv_value:,.2f}", delta=delta_str, help=TERM_TOOLTIPS.get("Conv. Value"))
            
            # Row 2: Efficiency metrics
            st.markdown("##### Efficiency Metrics")
            eff_col1, eff_col2, eff_col3, eff_col4 = st.columns(4)
            
            with eff_col1:
                st.metric("Avg CPC", f"${avg_cpc:.2f}", help="Average Cost Per Click")
            
            with eff_col2:
                st.metric("Avg CPM", f"${avg_cpm:.2f}", help="Cost Per 1,000 Impressions")
            
            with eff_col3:
                cpa_display = f"${avg_cpa:.2f}" if avg_cpa > 0 else "—"
                st.metric("Avg CPA", cpa_display, help=TERM_TOOLTIPS.get("CPA"))
            
            with eff_col4:
                roas_display = f"{roas:.2f}x" if roas > 0 else "—"
                st.metric("ROAS", roas_display, help=TERM_TOOLTIPS.get("ROAS"))
        else:
            st.info("No data available for the selected date range.")
    else:
        st.warning("Daily summary table not found. Please run the Google Ads ETL.")
    
    st.divider()
    
    # ========================================
    # 📊 Performance Trends (Combined Line Graphs)
    # ========================================
    st.subheader("📊 Performance Trends")
    
    if 'gads_daily_summary' in gads_tables:
        # Fetch daily trend data
        trend_query = f"""
        SELECT 
            date,
            SUM(impressions) as impressions,
            SUM(clicks) as clicks,
            SUM(cost) as cost,
            SUM(conversions) as conversions,
            SUM(conversions_value) as revenue,
            CASE WHEN SUM(impressions) > 0 THEN SUM(clicks) * 1.0 / SUM(impressions) ELSE 0 END as ctr,
            CASE WHEN SUM(clicks) > 0 THEN SUM(cost) / SUM(clicks) ELSE 0 END as cpc,
            CASE WHEN SUM(conversions) > 0 THEN SUM(cost) / SUM(conversions) ELSE NULL END as cpa,
            CASE WHEN SUM(cost) > 0 THEN SUM(conversions_value) / SUM(cost) ELSE 0 END as roas
        FROM gads_daily_summary
        WHERE date >= '{start_date_str}' AND date <= '{end_date_str}'
        GROUP BY date
        ORDER BY date
        """
        
        trend_df = load_duckdb_data(duckdb_path, trend_query)
        
        if trend_df is not None and not trend_df.empty:
            import plotly.express as px
            import plotly.graph_objects as go
            from plotly.subplots import make_subplots
            
            # Unified tabbed interface for all trends
            trend_tabs = st.tabs([
                "📈 Spend & Revenue", 
                "👆 Clicks & Conversions", 
                "📊 CTR & CPC",
                "💰 ROAS & CPA",
                "👁️ Impressions"
            ])
            
            with trend_tabs[0]:
                # Spend & Revenue dual-axis chart
                fig = make_subplots(specs=[[{"secondary_y": True}]])
                
                fig.add_trace(
                    go.Scatter(x=trend_df['date'], y=trend_df['cost'], name="Spend", 
                              line=dict(color='#FF6B6B', width=2), fill='tozeroy', fillcolor='rgba(255, 107, 107, 0.1)'),
                    secondary_y=False
                )
                fig.add_trace(
                    go.Scatter(x=trend_df['date'], y=trend_df['revenue'], name="Revenue",
                              line=dict(color='#4ECDC4', width=2), fill='tozeroy', fillcolor='rgba(78, 205, 196, 0.1)'),
                    secondary_y=True
                )
                
                fig.update_xaxes(title_text="Date")
                fig.update_yaxes(title_text="Spend ($)", secondary_y=False, tickformat="$,.0f")
                fig.update_yaxes(title_text="Revenue ($)", secondary_y=True, tickformat="$,.0f")
                fig.update_layout(title="Daily Spend vs Revenue", hovermode="x unified", height=400)
                
                st.plotly_chart(fig, use_container_width=True)
            
            with trend_tabs[1]:
                # Clicks & Conversions
                fig = make_subplots(specs=[[{"secondary_y": True}]])
                
                fig.add_trace(
                    go.Bar(x=trend_df['date'], y=trend_df['clicks'], name="Clicks",
                          marker_color='#5C7AEA', opacity=0.7),
                    secondary_y=False
                )
                fig.add_trace(
                    go.Scatter(x=trend_df['date'], y=trend_df['conversions'], name="Conversions",
                              line=dict(color='#FF6B6B', width=3), mode='lines+markers'),
                    secondary_y=True
                )
                
                fig.update_xaxes(title_text="Date")
                fig.update_yaxes(title_text="Clicks", secondary_y=False)
                fig.update_yaxes(title_text="Conversions", secondary_y=True)
                fig.update_layout(title="Daily Clicks & Conversions", hovermode="x unified", height=400)
                
                st.plotly_chart(fig, use_container_width=True)
            
            with trend_tabs[2]:
                # CTR & CPC
                fig = make_subplots(specs=[[{"secondary_y": True}]])
                
                # Convert CTR to percentage for display
                trend_df['ctr_pct'] = trend_df['ctr'] * 100
                
                fig.add_trace(
                    go.Scatter(x=trend_df['date'], y=trend_df['ctr_pct'], name="CTR (%)",
                              line=dict(color='#9B59B6', width=2), mode='lines+markers'),
                    secondary_y=False
                )
                fig.add_trace(
                    go.Scatter(x=trend_df['date'], y=trend_df['cpc'], name="CPC ($)",
                              line=dict(color='#E67E22', width=2), mode='lines+markers'),
                    secondary_y=True
                )
                
                fig.update_xaxes(title_text="Date")
                fig.update_yaxes(title_text="CTR (%)", secondary_y=False, tickformat=".2f")
                fig.update_yaxes(title_text="CPC ($)", secondary_y=True, tickformat="$.2f")
                fig.update_layout(title="Daily CTR & CPC", hovermode="x unified", height=400)
                
                st.plotly_chart(fig, use_container_width=True)
            
            with trend_tabs[3]:
                # ROAS & CPA
                fig = make_subplots(specs=[[{"secondary_y": True}]])
                
                fig.add_trace(
                    go.Scatter(x=trend_df['date'], y=trend_df['roas'], name="ROAS",
                              line=dict(color='#2ECC71', width=2), fill='tozeroy', fillcolor='rgba(46, 204, 113, 0.1)'),
                    secondary_y=False
                )
                
                # CPA (filter out nulls for better visualization)
                cpa_filtered = trend_df[trend_df['cpa'].notna()]
                fig.add_trace(
                    go.Scatter(x=cpa_filtered['date'], y=cpa_filtered['cpa'], name="CPA ($)",
                              line=dict(color='#E74C3C', width=2, dash='dot'), mode='lines+markers'),
                    secondary_y=True
                )
                
                fig.update_xaxes(title_text="Date")
                fig.update_yaxes(title_text="ROAS (x)", secondary_y=False, tickformat=".2f")
                fig.update_yaxes(title_text="CPA ($)", secondary_y=True, tickformat="$.2f")
                fig.update_layout(title="Daily ROAS & CPA", hovermode="x unified", height=400)
                
                st.plotly_chart(fig, use_container_width=True)
            
            with trend_tabs[4]:
                # Impressions
                fig = px.area(
                    trend_df, x='date', y='impressions',
                    title="Daily Impressions",
                    color_discrete_sequence=['#3498DB']
                )
                fig.update_layout(hovermode="x unified", height=400)
                fig.update_yaxes(tickformat=",")
                
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No trend data available for the selected date range.")
    
    st.divider()
    
    # ========================================
    # 🎯 Campaign Performance & Efficiency
    # ========================================
    st.subheader("🎯 Campaign Performance & Efficiency")
    
    if 'gads_campaigns' in gads_tables:
        # Enhanced query with all efficiency metrics (using base table)
        campaigns_query = f"""
        SELECT 
            campaign_name,
            campaign_type,
            campaign_status,
            SUM(impressions) as impressions,
            SUM(clicks) as clicks,
            SUM(cost) as cost,
            CASE WHEN SUM(impressions) > 0 THEN SUM(clicks) * 1.0 / SUM(impressions) ELSE 0 END as ctr,
            SUM(conversions) as conversions,
            SUM(conversions_value) as conversions_value,
            -- Calculated efficiency metrics
            CASE WHEN SUM(clicks) > 0 THEN SUM(conversions) / SUM(clicks) ELSE 0 END as conv_rate,
            CASE WHEN SUM(conversions) > 0 THEN SUM(cost) / SUM(conversions) ELSE NULL END as cpa,
            CASE WHEN SUM(clicks) > 0 THEN SUM(cost) / SUM(clicks) ELSE 0 END as cpc,
            CASE WHEN SUM(cost) > 0 THEN SUM(conversions_value) / SUM(cost) ELSE 0 END as roas
        FROM gads_campaigns
        WHERE date >= '{start_date_str}' AND date <= '{end_date_str}' AND campaign_name IS NOT NULL
        GROUP BY campaign_name, campaign_type, campaign_status
        ORDER BY cost DESC
        LIMIT 20
        """
        
        campaigns_df = load_duckdb_data(duckdb_path, campaigns_query)
        
        if campaigns_df is not None and not campaigns_df.empty:
            import plotly.graph_objects as go
            
            # Calculate efficiency score (0-100) using weighted multi-factor scoring
            # Factors: ROAS (35%), Conversion Rate (30%), CTR (20%), CPA efficiency (15%)
            avg_cpa = campaigns_df[campaigns_df['cpa'].notna() & (campaigns_df['cpa'] > 0)]['cpa'].mean() if any(campaigns_df['cpa'].notna() & (campaigns_df['cpa'] > 0)) else 1
            avg_conv_rate = campaigns_df['conv_rate'].mean() if campaigns_df['conv_rate'].mean() > 0 else 0.01
            avg_ctr = campaigns_df['ctr'].mean() if campaigns_df['ctr'].mean() > 0 else 0.01
            avg_roas = campaigns_df['roas'].mean() if campaigns_df['roas'].mean() > 0 else 1
            
            def calc_campaign_efficiency_score(row):
                """
                Calculate composite efficiency score (0-100).
                
                Scoring breakdown:
                - ROAS: 35 points (higher is better, capped at 2x average)
                - Conversion Rate: 30 points (higher is better, capped at 2x average)  
                - CTR: 20 points (higher is better, capped at 2x average)
                - CPA Efficiency: 15 points (lower is better, capped at 0.5x average)
                """
                score = 0
                
                # ROAS score (35 points max) - most important for profitability
                if avg_roas > 0 and row['roas']:
                    roas_ratio = min(row['roas'] / avg_roas, 2.0)
                    score += roas_ratio * 17.5  # Max 35 points
                
                # Conversion Rate score (30 points max)
                if avg_conv_rate > 0:
                    conv_ratio = min(row['conv_rate'] / avg_conv_rate, 2.0)
                    score += conv_ratio * 15  # Max 30 points
                
                # CTR score (20 points max)
                if avg_ctr > 0 and row['ctr']:
                    ctr_ratio = min(row['ctr'] / avg_ctr, 2.0)
                    score += ctr_ratio * 10  # Max 20 points
                
                # CPA Efficiency score (15 points max) - lower is better
                if row['cpa'] and row['cpa'] > 0 and avg_cpa > 0:
                    cpa_ratio = min(avg_cpa / row['cpa'], 2.0)  # Inverted - lower CPA is better
                    score += cpa_ratio * 7.5  # Max 15 points
                
                return min(round(score), 100)
            
            campaigns_df['efficiency_score'] = campaigns_df.apply(calc_campaign_efficiency_score, axis=1)
            
            # Grade based on score with descriptive labels
            def get_campaign_grade(score):
                if score >= 80: return "A"
                elif score >= 65: return "B"
                elif score >= 50: return "C"
                elif score >= 35: return "D"
                else: return "F"
            
            campaigns_df['grade'] = campaigns_df['efficiency_score'].apply(get_campaign_grade)
            
            # Display KPI summary cards
            st.markdown("##### Campaign Portfolio Overview")
            kpi_col1, kpi_col2, kpi_col3, kpi_col4, kpi_col5, kpi_col6 = st.columns(6)
            
            total_cost = campaigns_df['cost'].sum()
            total_clicks = campaigns_df['clicks'].sum()
            total_conversions = campaigns_df['conversions'].sum()
            total_revenue = campaigns_df['conversions_value'].sum()
            overall_cpa = total_cost / total_conversions if total_conversions > 0 else 0
            overall_roas = total_revenue / total_cost if total_cost > 0 else 0
            
            with kpi_col1:
                st.metric("Total Spend", f"${total_cost:,.2f}", help=TERM_TOOLTIPS.get("Spend"))
            with kpi_col2:
                st.metric("Conversions", f"{total_conversions:,.1f}", help=TERM_TOOLTIPS.get("Conversions"))
            with kpi_col3:
                st.metric("Revenue", f"${total_revenue:,.2f}", help="Total conversion value")
            with kpi_col4:
                st.metric("Avg CPA", f"${overall_cpa:,.2f}" if overall_cpa else "—", help=TERM_TOOLTIPS.get("CPA"))
            with kpi_col5:
                st.metric("Portfolio ROAS", f"{overall_roas:.2f}x" if overall_roas else "—", help=TERM_TOOLTIPS.get("ROAS"))
            with kpi_col6:
                avg_efficiency = campaigns_df['efficiency_score'].mean()
                st.metric("Avg Score", f"{avg_efficiency:.0f}/100", help="Portfolio efficiency score")
            
            st.markdown("---")
            
            # Grade distribution visualization
            grade_col1, grade_col2 = st.columns([1, 2])
            
            with grade_col1:
                st.markdown("##### Grade Distribution")
                grade_counts = campaigns_df['grade'].value_counts().reindex(['A', 'B', 'C', 'D', 'F'], fill_value=0)
                grade_colors = ['#27ae60', '#2ecc71', '#f39c12', '#e67e22', '#e74c3c']
                
                fig_grade = go.Figure(data=[
                    go.Bar(
                        x=grade_counts.index,
                        y=grade_counts.values,
                        marker_color=grade_colors,
                        text=grade_counts.values,
                        textposition='auto'
                    )
                ])
                fig_grade.update_layout(
                    xaxis_title="Grade",
                    yaxis_title="# Campaigns",
                    height=250,
                    margin=dict(l=20, r=20, t=20, b=40),
                    showlegend=False
                )
                st.plotly_chart(fig_grade, use_container_width=True)
            
            with grade_col2:
                st.markdown("##### Spend vs Conversions by Efficiency")
                # Bubble chart: spend vs conversions, sized by efficiency
                fig_bubble = px.scatter(
                    campaigns_df,
                    x='cost',
                    y='conversions',
                    size='efficiency_score',
                    color='grade',
                    color_discrete_map={'A': '#27ae60', 'B': '#2ecc71', 'C': '#f39c12', 'D': '#e67e22', 'F': '#e74c3c'},
                    hover_name='campaign_name',
                    hover_data={
                        'cost': ':$,.2f',
                        'conversions': ':,.1f',
                        'roas': ':.2f',
                        'efficiency_score': True,
                        'grade': True
                    },
                    labels={'cost': 'Spend ($)', 'conversions': 'Conversions', 'efficiency_score': 'Score'},
                    size_max=40
                )
                fig_bubble.update_layout(
                    height=250,
                    margin=dict(l=20, r=20, t=20, b=40),
                    legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1, title='Grade')
                )
                st.plotly_chart(fig_bubble, use_container_width=True)
            
            # Format for display table
            display_df = campaigns_df.copy()
            
            # Format campaign_type to be more readable
            campaign_type_map = {
                'SEARCH': 'Search',
                'PERFORMANCE_MAX': 'PMax',
                'DISPLAY': 'Display',
                'VIDEO': 'Video',
                'SHOPPING': 'Shopping',
                'SMART': 'Smart',
                'LOCAL': 'Local',
                'DISCOVERY': 'Discovery',
                'DEMAND_GEN': 'Demand Gen',
                'APP': 'App',
                'HOTEL': 'Hotel',
                'LOCAL_SERVICES': 'Local Svc',
                'MULTI_CHANNEL': 'Multi-Ch',
            }
            display_df['campaign_type'] = display_df['campaign_type'].apply(
                lambda x: campaign_type_map.get(x, x.replace('_', ' ').title() if x else 'Unknown') if x else 'Unknown'
            )
            
            # Truncate long campaign names
            display_df['campaign_name'] = display_df['campaign_name'].apply(lambda x: x[:40] + '...' if len(str(x)) > 40 else x)
            
            display_df = display_df.rename(columns={
                'campaign_name': 'Campaign',
                'campaign_type': 'Type',
                'campaign_status': 'Status',
                'impressions': 'Impr.',
                'clicks': 'Clicks',
                'cost': 'Cost',
                'ctr': 'CTR',
                'conversions': 'Conv.',
                'conv_rate': 'Conv Rate',
                'cpa': 'CPA',
                'cpc': 'CPC',
                'roas': 'ROAS',
                'efficiency_score': 'Score',
                'grade': 'Grade'
            })
            
            # Format columns for display
            display_df['Impr.'] = display_df['Impr.'].apply(lambda x: f"{int(x):,}" if x else "0")
            display_df['Clicks'] = display_df['Clicks'].apply(lambda x: f"{int(x):,}" if x else "0")
            display_df['Cost'] = display_df['Cost'].apply(lambda x: f"${x:,.2f}" if x else "$0.00")
            display_df['CTR'] = display_df['CTR'].apply(lambda x: f"{x:.2%}" if x else "0%")
            display_df['Conv.'] = display_df['Conv.'].apply(lambda x: f"{x:.1f}" if x else "0")
            display_df['Conv Rate'] = display_df['Conv Rate'].apply(lambda x: f"{x:.2%}" if x else "0%")
            display_df['CPA'] = display_df['CPA'].apply(lambda x: f"${x:,.2f}" if x and x > 0 else "—")
            display_df['CPC'] = display_df['CPC'].apply(lambda x: f"${x:.2f}" if x else "$0.00")
            display_df['ROAS'] = display_df['ROAS'].apply(lambda x: f"{x:.2f}x" if x else "—")
            
            # Drop intermediate columns
            display_df = display_df.drop(columns=['conversions_value'], errors='ignore')
            
            # Reorder columns for better readability
            column_order = ['Campaign', 'Type', 'Status', 'Cost', 'Clicks', 'Conv.', 'Conv Rate', 'CPA', 'ROAS', 'CTR', 'Score', 'Grade']
            display_df = display_df[[c for c in column_order if c in display_df.columns]]
            
            st.markdown("##### Campaign Performance Table")
            st.dataframe(display_df, use_container_width=True, hide_index=True)
            
            # Efficiency insights
            st.markdown("##### 💡 Campaign Insights")
            
            insights_col1, insights_col2 = st.columns(2)
            
            with insights_col1:
                # Top performers
                top_campaigns = campaigns_df.nlargest(3, 'efficiency_score')
                st.markdown("**🏆 Top Performers:**")
                for _, row in top_campaigns.iterrows():
                    roas_str = f"ROAS {row['roas']:.2f}x" if row['roas'] else ""
                    conv_str = f"Conv Rate {row['conv_rate']:.1%}" if row['conv_rate'] else ""
                    name_display = str(row['campaign_name'])[:35] + '...' if len(str(row['campaign_name'])) > 35 else row['campaign_name']
                    st.success(f"**{name_display}** - Score: {row['efficiency_score']} ({row['grade']}) | {conv_str} | {roas_str}")
            
            with insights_col2:
                # Needs attention (campaigns with spend but low efficiency)
                needs_attention = campaigns_df[
                    (campaigns_df['cost'] > campaigns_df['cost'].quantile(0.25)) & 
                    (campaigns_df['efficiency_score'] < 50)
                ].nsmallest(3, 'efficiency_score')
                
                if not needs_attention.empty:
                    st.markdown("**⚠️ Needs Attention:**")
                    for _, row in needs_attention.iterrows():
                        issue = []
                        if row['conv_rate'] < avg_conv_rate * 0.5:
                            issue.append("low conv rate")
                        if row['cpa'] and row['cpa'] > avg_cpa * 1.5:
                            issue.append("high CPA")
                        if row['roas'] and row['roas'] < avg_roas * 0.5:
                            issue.append("low ROAS")
                        issues_str = ", ".join(issue) if issue else "underperforming"
                        name_display = str(row['campaign_name'])[:35] + '...' if len(str(row['campaign_name'])) > 35 else row['campaign_name']
                        st.warning(f"**{name_display}** - Score: {row['efficiency_score']} | Issues: {issues_str}")
                else:
                    st.info("All campaigns with significant spend are performing adequately!")
        else:
            st.info("No campaign data available for the selected date range.")
    else:
        st.warning("Campaigns table not found. Please run the Google Ads ETL.")
    
    st.divider()
    
    # ========================================
    # 🔑 Top Keywords & 📱 Device Performance (Side by Side)
    # ========================================
    kw_dev_col1, kw_dev_col2 = st.columns(2)
    
    with kw_dev_col1:
        st.subheader("🔑 Top Keywords")
        
        if 'gads_keywords' in gads_tables:
            keywords_query = f"""
            SELECT 
                keyword_text,
                keyword_match_type,
                SUM(impressions) as impressions,
                SUM(clicks) as clicks,
                SUM(cost) as cost,
                SUM(conversions) as conversions,
                SUM(conversions_value) as conversions_value,
                CASE WHEN SUM(impressions) > 0 THEN SUM(clicks) * 1.0 / SUM(impressions) ELSE 0 END as ctr,
                CASE WHEN SUM(clicks) > 0 THEN SUM(conversions) / SUM(clicks) ELSE 0 END as conv_rate,
                CASE WHEN SUM(conversions) > 0 THEN SUM(cost) / SUM(conversions) ELSE NULL END as cpa,
                CASE WHEN SUM(cost) > 0 THEN SUM(conversions_value) / SUM(cost) ELSE 0 END as roas
            FROM gads_keywords
            WHERE date >= '{start_date_str}' AND date <= '{end_date_str}' AND keyword_text IS NOT NULL
            GROUP BY keyword_text, keyword_match_type
            ORDER BY cost DESC
            LIMIT 15
            """
            
            keywords_df = load_duckdb_data(duckdb_path, keywords_query)
            
            if keywords_df is not None and not keywords_df.empty:
                # Summary metrics
                kw_total_spend = keywords_df['cost'].sum()
                kw_total_conv = keywords_df['conversions'].sum()
                kw_avg_cpa = kw_total_spend / kw_total_conv if kw_total_conv > 0 else 0
                
                kw_m1, kw_m2, kw_m3 = st.columns(3)
                with kw_m1:
                    st.metric("Keyword Spend", f"${kw_total_spend:,.2f}")
                with kw_m2:
                    st.metric("Conversions", f"{kw_total_conv:,.1f}")
                with kw_m3:
                    st.metric("Avg CPA", f"${kw_avg_cpa:.2f}" if kw_avg_cpa > 0 else "—")
                
                # Format display
                display_df = keywords_df.copy()
                display_df['keyword_text'] = display_df['keyword_text'].apply(lambda x: x[:30] + '...' if len(str(x)) > 30 else x)
                display_df['cost'] = display_df['cost'].apply(lambda x: f"${x:,.2f}" if x else "$0.00")
                display_df['ctr'] = display_df['ctr'].apply(lambda x: f"{x:.2%}" if x else "0%")
                display_df['conv_rate'] = display_df['conv_rate'].apply(lambda x: f"{x:.1%}" if x else "0%")
                display_df['cpa'] = display_df['cpa'].apply(lambda x: f"${x:.2f}" if x and x > 0 else "—")
                display_df['roas'] = display_df['roas'].apply(lambda x: f"{x:.2f}x" if x else "—")
                display_df = display_df.drop(columns=['conversions_value', 'impressions'], errors='ignore')
                display_df = display_df.rename(columns={
                    'keyword_text': 'Keyword',
                    'keyword_match_type': 'Match',
                    'clicks': 'Clicks',
                    'cost': 'Cost',
                    'conversions': 'Conv.',
                    'ctr': 'CTR',
                    'conv_rate': 'Conv%',
                    'cpa': 'CPA',
                    'roas': 'ROAS'
                })
                
                # Reorder columns
                col_order = ['Keyword', 'Match', 'Cost', 'Clicks', 'Conv.', 'Conv%', 'CPA', 'ROAS']
                display_df = display_df[[c for c in col_order if c in display_df.columns]]
                
                st.dataframe(display_df, use_container_width=True, hide_index=True, height=400)
            else:
                st.info("No keyword data available for the selected date range.")
        else:
            st.warning("Keywords table not found. Please run the Google Ads ETL.")
    
    with kw_dev_col2:
        st.subheader("📱 Device Performance")
        
        if 'gads_devices' in gads_tables:
            devices_query = f"""
            SELECT 
                device,
                SUM(impressions) as impressions,
                SUM(clicks) as clicks,
                SUM(cost) as cost,
                CASE WHEN SUM(impressions) > 0 THEN SUM(clicks) * 1.0 / SUM(impressions) ELSE 0 END as ctr,
                SUM(conversions) as conversions,
                SUM(conversions_value) as conversions_value,
                CASE WHEN SUM(clicks) > 0 THEN SUM(conversions) / SUM(clicks) ELSE 0 END as conv_rate,
                CASE WHEN SUM(conversions) > 0 THEN SUM(cost) / SUM(conversions) ELSE NULL END as cpa,
                CASE WHEN SUM(cost) > 0 THEN SUM(conversions_value) / SUM(cost) ELSE 0 END as roas
            FROM gads_devices
            WHERE date >= '{start_date_str}' AND date <= '{end_date_str}' AND device IS NOT NULL
            GROUP BY device
            ORDER BY cost DESC
            """
            
            devices_df = load_duckdb_data(duckdb_path, devices_query)
            
            if devices_df is not None and not devices_df.empty:
                # Summary metrics
                dev_total_spend = devices_df['cost'].sum()
                dev_total_conv = devices_df['conversions'].sum()
                top_device = devices_df.iloc[0]['device'] if not devices_df.empty else "—"
                
                dev_m1, dev_m2, dev_m3 = st.columns(3)
                with dev_m1:
                    st.metric("Device Spend", f"${dev_total_spend:,.2f}")
                with dev_m2:
                    st.metric("Conversions", f"{dev_total_conv:,.1f}")
                with dev_m3:
                    st.metric("Top Device", top_device)
                
                # Pie chart for device spend distribution
                fig_device = px.pie(
                    devices_df,
                    values='cost',
                    names='device',
                    title='Spend by Device',
                    hole=0.4,
                    color_discrete_sequence=px.colors.qualitative.Set2
                )
                fig_device.update_layout(
                    height=200,
                    margin=dict(l=10, r=10, t=40, b=10),
                    showlegend=True,
                    legend=dict(orientation='h', yanchor='bottom', y=-0.2, xanchor='center', x=0.5)
                )
                st.plotly_chart(fig_device, use_container_width=True)
                
                # Detailed table with efficiency metrics
                display_df = devices_df.copy()
                display_df['impressions'] = display_df['impressions'].apply(lambda x: f"{int(x):,}" if x else "0")
                display_df['clicks'] = display_df['clicks'].apply(lambda x: f"{int(x):,}" if x else "0")
                display_df['cost'] = display_df['cost'].apply(lambda x: f"${x:,.2f}" if x else "$0.00")
                display_df['ctr'] = display_df['ctr'].apply(lambda x: f"{x:.2%}" if x else "0%")
                display_df['conversions'] = display_df['conversions'].apply(lambda x: f"{x:.1f}" if x else "0")
                display_df['conv_rate'] = display_df['conv_rate'].apply(lambda x: f"{x:.1%}" if x else "0%")
                display_df['cpa'] = display_df['cpa'].apply(lambda x: f"${x:.2f}" if x and x > 0 else "—")
                display_df['roas'] = display_df['roas'].apply(lambda x: f"{x:.2f}x" if x else "—")
                display_df = display_df.drop(columns=['conversions_value'], errors='ignore')
                display_df = display_df.rename(columns={
                    'device': 'Device',
                    'impressions': 'Impr.',
                    'clicks': 'Clicks',
                    'cost': 'Cost',
                    'ctr': 'CTR',
                    'conversions': 'Conv.',
                    'conv_rate': 'Conv%',
                    'cpa': 'CPA',
                    'roas': 'ROAS'
                })
                st.dataframe(display_df, use_container_width=True, hide_index=True)
            else:
                st.info("No device data available for the selected date range.")
        else:
            st.warning("Devices table not found. Please run the Google Ads ETL.")
    
    st.divider()
    
    # ========================================
    # 📂 Ad Group Performance
    # ========================================
    st.subheader("📂 Ad Group Performance")
    
    if 'gads_ad_groups' in gads_tables:
        ad_groups_query = f"""
        SELECT 
            campaign_name,
            ad_group_name,
            ad_group_status,
            SUM(impressions) as impressions,
            SUM(clicks) as clicks,
            SUM(cost) as cost,
            CASE WHEN SUM(impressions) > 0 THEN SUM(clicks) * 1.0 / SUM(impressions) ELSE 0 END as ctr,
            SUM(conversions) as conversions,
            SUM(conversions_value) as conversions_value,
            CASE WHEN SUM(clicks) > 0 THEN SUM(conversions) / SUM(clicks) ELSE 0 END as conv_rate,
            CASE WHEN SUM(conversions) > 0 THEN SUM(cost) / SUM(conversions) ELSE NULL END as cpa,
            CASE WHEN SUM(cost) > 0 THEN SUM(conversions_value) / SUM(cost) ELSE 0 END as roas
        FROM gads_ad_groups
        WHERE date >= '{start_date_str}' AND date <= '{end_date_str}' AND ad_group_name IS NOT NULL
        GROUP BY campaign_name, ad_group_name, ad_group_status
        ORDER BY cost DESC
        LIMIT 20
        """
        
        ad_groups_df = load_duckdb_data(duckdb_path, ad_groups_query)
        
        if ad_groups_df is not None and not ad_groups_df.empty:
            # Summary metrics
            ag_total_spend = ad_groups_df['cost'].sum()
            ag_total_conv = ad_groups_df['conversions'].sum()
            ag_count = len(ad_groups_df)
            ag_avg_cpa = ag_total_spend / ag_total_conv if ag_total_conv > 0 else 0
            
            ag_m1, ag_m2, ag_m3, ag_m4 = st.columns(4)
            with ag_m1:
                st.metric("Ad Groups", f"{ag_count}")
            with ag_m2:
                st.metric("Total Spend", f"${ag_total_spend:,.2f}")
            with ag_m3:
                st.metric("Conversions", f"{ag_total_conv:,.1f}")
            with ag_m4:
                st.metric("Avg CPA", f"${ag_avg_cpa:.2f}" if ag_avg_cpa > 0 else "—")
            
            # Format display
            display_df = ad_groups_df.copy()
            display_df['campaign_name'] = display_df['campaign_name'].apply(lambda x: str(x)[:25] + '...' if len(str(x)) > 25 else x)
            display_df['ad_group_name'] = display_df['ad_group_name'].apply(lambda x: str(x)[:25] + '...' if len(str(x)) > 25 else x)
            display_df['impressions'] = display_df['impressions'].apply(lambda x: f"{int(x):,}" if x else "0")
            display_df['clicks'] = display_df['clicks'].apply(lambda x: f"{int(x):,}" if x else "0")
            display_df['cost'] = display_df['cost'].apply(lambda x: f"${x:,.2f}" if x else "$0.00")
            display_df['ctr'] = display_df['ctr'].apply(lambda x: f"{x:.2%}" if x else "0%")
            display_df['conversions'] = display_df['conversions'].apply(lambda x: f"{x:.1f}" if x else "0")
            display_df['conv_rate'] = display_df['conv_rate'].apply(lambda x: f"{x:.1%}" if x else "0%")
            display_df['cpa'] = display_df['cpa'].apply(lambda x: f"${x:.2f}" if x and x > 0 else "—")
            display_df['roas'] = display_df['roas'].apply(lambda x: f"{x:.2f}x" if x else "—")
            display_df = display_df.drop(columns=['conversions_value'], errors='ignore')
            display_df = display_df.rename(columns={
                'campaign_name': 'Campaign',
                'ad_group_name': 'Ad Group',
                'ad_group_status': 'Status',
                'impressions': 'Impr.',
                'clicks': 'Clicks',
                'cost': 'Cost',
                'ctr': 'CTR',
                'conversions': 'Conv.',
                'conv_rate': 'Conv%',
                'cpa': 'CPA',
                'roas': 'ROAS'
            })
            
            # Reorder columns
            col_order = ['Campaign', 'Ad Group', 'Status', 'Cost', 'Clicks', 'Conv.', 'Conv%', 'CPA', 'ROAS', 'CTR']
            display_df = display_df[[c for c in col_order if c in display_df.columns]]
            
            st.dataframe(display_df, use_container_width=True, hide_index=True)
        else:
            st.info("No ad group data available for the selected date range.")
    else:
        st.warning("Ad Groups table not found. Please run the Google Ads ETL.")
    
    st.divider()
    
    # ========================================
    # Geographic Performance
    # ========================================
    st.subheader("🌍 Geographic Performance")
    
    # Google Ads Country Criterion ID to Country Name mapping
    # These are the most common geo target IDs used in Google Ads
    GADS_COUNTRY_MAP = {
        2004: "Afghanistan", 2008: "Albania", 2012: "Algeria", 2020: "Andorra",
        2024: "Angola", 2028: "Antigua and Barbuda", 2032: "Argentina", 2051: "Armenia",
        2036: "Australia", 2040: "Austria", 2031: "Azerbaijan", 2044: "Bahamas",
        2048: "Bahrain", 2050: "Bangladesh", 2052: "Barbados", 2112: "Belarus",
        2056: "Belgium", 2084: "Belize", 2204: "Benin", 2064: "Bhutan",
        2068: "Bolivia", 2070: "Bosnia and Herzegovina", 2072: "Botswana", 2076: "Brazil",
        2096: "Brunei", 2100: "Bulgaria", 2854: "Burkina Faso", 2108: "Burundi",
        2116: "Cambodia", 2120: "Cameroon", 2124: "Canada", 2132: "Cape Verde",
        2140: "Central African Republic", 2148: "Chad", 2152: "Chile", 2156: "China",
        2170: "Colombia", 2174: "Comoros", 2178: "Congo", 2180: "DR Congo",
        2188: "Costa Rica", 2384: "Ivory Coast", 2191: "Croatia", 2192: "Cuba",
        2196: "Cyprus", 2203: "Czech Republic", 2208: "Denmark", 2262: "Djibouti",
        2212: "Dominica", 2214: "Dominican Republic", 2218: "Ecuador", 2818: "Egypt",
        2222: "El Salvador", 2226: "Equatorial Guinea", 2232: "Eritrea", 2233: "Estonia",
        2231: "Ethiopia", 2242: "Fiji", 2246: "Finland", 2250: "France",
        2266: "Gabon", 2270: "Gambia", 2268: "Georgia", 2276: "Germany",
        2288: "Ghana", 2300: "Greece", 2308: "Grenada", 2320: "Guatemala",
        2324: "Guinea", 2624: "Guinea-Bissau", 2328: "Guyana", 2332: "Haiti",
        2340: "Honduras", 2344: "Hong Kong", 2348: "Hungary", 2352: "Iceland",
        2356: "India", 2360: "Indonesia", 2364: "Iran", 2368: "Iraq",
        2372: "Ireland", 2376: "Israel", 2380: "Italy", 2388: "Jamaica",
        2392: "Japan", 2400: "Jordan", 2398: "Kazakhstan", 2404: "Kenya",
        2296: "Kiribati", 2408: "North Korea", 2410: "South Korea", 2414: "Kuwait",
        2417: "Kyrgyzstan", 2418: "Laos", 2428: "Latvia", 2422: "Lebanon",
        2426: "Lesotho", 2430: "Liberia", 2434: "Libya", 2438: "Liechtenstein",
        2440: "Lithuania", 2442: "Luxembourg", 2446: "Macau", 2807: "North Macedonia",
        2450: "Madagascar", 2454: "Malawi", 2458: "Malaysia", 2462: "Maldives",
        2466: "Mali", 2470: "Malta", 2584: "Marshall Islands", 2478: "Mauritania",
        2480: "Mauritius", 2484: "Mexico", 2583: "Micronesia", 2498: "Moldova",
        2492: "Monaco", 2496: "Mongolia", 2499: "Montenegro", 2504: "Morocco",
        2508: "Mozambique", 2104: "Myanmar", 2516: "Namibia", 2520: "Nauru",
        2524: "Nepal", 2528: "Netherlands", 2554: "New Zealand", 2558: "Nicaragua",
        2562: "Niger", 2566: "Nigeria", 2578: "Norway", 2512: "Oman",
        2586: "Pakistan", 2585: "Palau", 2275: "Palestine", 2591: "Panama",
        2598: "Papua New Guinea", 2600: "Paraguay", 2604: "Peru", 2608: "Philippines",
        2616: "Poland", 2620: "Portugal", 2634: "Qatar", 2642: "Romania",
        2643: "Russia", 2646: "Rwanda", 2659: "Saint Kitts and Nevis",
        2662: "Saint Lucia", 2670: "Saint Vincent", 2882: "Samoa", 2674: "San Marino",
        2678: "Sao Tome and Principe", 2682: "Saudi Arabia", 2686: "Senegal",
        2688: "Serbia", 2690: "Seychelles", 2694: "Sierra Leone", 2702: "Singapore",
        2703: "Slovakia", 2705: "Slovenia", 2090: "Solomon Islands", 2706: "Somalia",
        2710: "South Africa", 2724: "Spain", 2144: "Sri Lanka", 2736: "Sudan",
        2740: "Suriname", 2748: "Eswatini", 2752: "Sweden", 2756: "Switzerland",
        2760: "Syria", 2158: "Taiwan", 2762: "Tajikistan", 2834: "Tanzania",
        2764: "Thailand", 2626: "Timor-Leste", 2768: "Togo", 2776: "Tonga",
        2780: "Trinidad and Tobago", 2788: "Tunisia", 2792: "Turkey", 2795: "Turkmenistan",
        2798: "Tuvalu", 2800: "Uganda", 2804: "Ukraine", 2784: "UAE",
        2826: "United Kingdom", 2840: "United States", 2858: "Uruguay", 2860: "Uzbekistan",
        2548: "Vanuatu", 2336: "Vatican City", 2862: "Venezuela", 2704: "Vietnam",
        2887: "Yemen", 2894: "Zambia", 2716: "Zimbabwe"
    }
    
    if 'gads_geographic' in gads_tables:
        geo_query = f"""
        SELECT 
            country_criterion_id,
            SUM(impressions) as impressions,
            SUM(clicks) as clicks,
            SUM(cost) as cost,
            SUM(conversions) as conversions
        FROM gads_geographic
        WHERE date >= '{start_date_str}' AND date <= '{end_date_str}' AND country_criterion_id IS NOT NULL
        GROUP BY country_criterion_id
        ORDER BY clicks DESC
        LIMIT 15
        """
        
        geo_df = load_duckdb_data(duckdb_path, geo_query)
        
        if geo_df is not None and not geo_df.empty:
            import plotly.express as px
            
            # Map criterion IDs to country names
            geo_df['country'] = geo_df['country_criterion_id'].apply(
                lambda x: GADS_COUNTRY_MAP.get(int(x), f"Unknown ({x})") if pd.notna(x) else "Unknown"
            )
            
            # Create two columns for visualizations
            geo_col1, geo_col2 = st.columns(2)
            
            with geo_col1:
                # Choropleth world map
                fig_map = px.choropleth(
                    geo_df,
                    locations="country",
                    locationmode="country names",
                    color="clicks",
                    hover_name="country",
                    hover_data={
                        "clicks": ":,",
                        "impressions": ":,",
                        "cost": "$.2f",
                        "conversions": ":,.1f",
                        "country_criterion_id": False
                    },
                    color_continuous_scale="Blues",
                    title="Clicks by Country"
                )
                
                fig_map.update_layout(
                    geo=dict(
                        showframe=False,
                        showcoastlines=True,
                        projection_type='natural earth'
                    ),
                    margin=dict(l=0, r=0, t=40, b=0),
                    height=350
                )
                
                st.plotly_chart(fig_map, use_container_width=True)
            
            with geo_col2:
                # Pie chart for top countries
                top_countries = geo_df.head(8).copy()
                other_clicks = geo_df.iloc[8:]['clicks'].sum() if len(geo_df) > 8 else 0
                
                if other_clicks > 0:
                    other_row = pd.DataFrame([{
                        'country': 'Others',
                        'clicks': other_clicks,
                        'impressions': 0,
                        'cost': 0,
                        'conversions': 0
                    }])
                    top_countries = pd.concat([top_countries, other_row], ignore_index=True)
                
                fig_pie = px.pie(
                    top_countries,
                    values='clicks',
                    names='country',
                    title='Clicks Distribution by Country',
                    hole=0.4,
                    color_discrete_sequence=px.colors.qualitative.Set2
                )
                
                fig_pie.update_layout(
                    margin=dict(l=0, r=0, t=40, b=0),
                    height=350,
                    showlegend=True,
                    legend=dict(orientation="h", yanchor="bottom", y=-0.3, xanchor="center", x=0.5)
                )
                
                fig_pie.update_traces(
                    textposition='inside',
                    textinfo='percent+label',
                    hovertemplate='<b>%{label}</b><br>Clicks: %{value:,}<extra></extra>'
                )
                
                st.plotly_chart(fig_pie, use_container_width=True)
            
            # Data table with country names
            st.caption("**Top Countries by Performance**")
            display_df = geo_df[['country', 'clicks', 'impressions', 'cost', 'conversions']].copy()
            display_df['cost'] = display_df['cost'].apply(lambda x: f"${x:,.2f}" if pd.notna(x) else "$0.00")
            display_df['clicks'] = display_df['clicks'].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "0")
            display_df['impressions'] = display_df['impressions'].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "0")
            display_df['conversions'] = display_df['conversions'].apply(lambda x: f"{x:,.1f}" if pd.notna(x) else "0")
            
            st.dataframe(
                display_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "country": "Country",
                    "clicks": "Clicks",
                    "impressions": "Impressions",
                    "cost": "Cost",
                    "conversions": "Conversions"
                }
            )
    else:
        st.info("No geographic data available. Run Google Ads ETL to populate.")
    
    # ========================================
    # 🕐 Hourly Performance
    # ========================================
    st.subheader("🕐 Hourly Performance")
    
    if 'gads_hourly' in gads_tables:
        hourly_query = f"""
        SELECT 
            hour as hour,
            SUM(impressions) as impressions,
            SUM(clicks) as clicks,
            SUM(cost) as cost,
            SUM(conversions) as conversions,
            CASE WHEN SUM(impressions) > 0 THEN SUM(clicks) * 1.0 / SUM(impressions) ELSE 0 END as ctr
        FROM gads_hourly
        WHERE date >= '{start_date_str}' AND date <= '{end_date_str}' AND hour IS NOT NULL
        GROUP BY hour
        ORDER BY hour
        """
        
        hourly_df = load_duckdb_data(duckdb_path, hourly_query)
        
        if hourly_df is not None and not hourly_df.empty:
            # Summary metrics
            peak_hour = hourly_df.loc[hourly_df['clicks'].idxmax(), 'hour'] if not hourly_df.empty else 0
            peak_clicks = hourly_df['clicks'].max()
            best_ctr_hour = hourly_df.loc[hourly_df['ctr'].idxmax(), 'hour'] if not hourly_df.empty else 0
            
            hr_m1, hr_m2, hr_m3 = st.columns(3)
            with hr_m1:
                st.metric("Peak Hour (Clicks)", f"{int(peak_hour)}:00", help=f"Hour with most clicks ({int(peak_clicks):,})")
            with hr_m2:
                st.metric("Best CTR Hour", f"{int(best_ctr_hour)}:00", help="Hour with highest click-through rate")
            with hr_m3:
                total_hourly_clicks = hourly_df['clicks'].sum()
                st.metric("Total Clicks", f"{int(total_hourly_clicks):,}")
            
            # Enhanced hourly chart with Plotly
            import plotly.graph_objects as go
            from plotly.subplots import make_subplots
            
            fig_hourly = make_subplots(specs=[[{"secondary_y": True}]])
            
            fig_hourly.add_trace(
                go.Bar(x=hourly_df['hour'], y=hourly_df['clicks'], name="Clicks",
                      marker_color='#5C7AEA', opacity=0.7),
                secondary_y=False
            )
            fig_hourly.add_trace(
                go.Scatter(x=hourly_df['hour'], y=hourly_df['conversions'], name="Conversions",
                          line=dict(color='#FF6B6B', width=3), mode='lines+markers'),
                secondary_y=True
            )
            
            fig_hourly.update_xaxes(title_text="Hour of Day", tickmode='linear', dtick=2)
            fig_hourly.update_yaxes(title_text="Clicks", secondary_y=False)
            fig_hourly.update_yaxes(title_text="Conversions", secondary_y=True)
            fig_hourly.update_layout(
                title="Performance by Hour of Day",
                hovermode="x unified",
                height=350,
                legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
            )
            
            st.plotly_chart(fig_hourly, use_container_width=True)
        else:
            st.info("No hourly data available for the selected date range.")
    
    # ========================================
    # Raw Data Explorer
    # ========================================
    with st.expander("📋 Explore Raw Google Ads Data"):
        table_choice = st.selectbox(
            "Select Table",
            options=gads_tables,
            key="gads_table_choice"
        )
        
        if table_choice:
            raw_df = load_duckdb_data(duckdb_path, f"SELECT * FROM {table_choice} LIMIT 1000")
            if raw_df is not None:
                st.dataframe(raw_df, use_container_width=True)


# ============================================
# Meta Ads Dashboard Page (MBA-Level Marketing Analytics)
# ============================================
def render_meta_dashboard(meta_config, duckdb_path: str):
    """
    Render the Meta (Facebook) Ads MBA-level marketing dashboard.
    
    Features comprehensive marketing analytics including:
    - Executive KPIs with period comparisons
    - Campaign performance analysis
    - Ad Set (targeting) effectiveness
    - Creative performance analysis
    - Geographic and demographic insights
    - ROI and efficiency metrics
    - Budget pacing and optimization recommendations
    """
    
    st.header("📘 Meta Ads - Marketing Analytics Dashboard")
    
    # Check if Meta data exists
    has_data, total_rows, meta_tables = check_meta_data_exists(duckdb_path)
    
    if not has_data:
        st.info("""
        **No Meta Ads data available yet.**
        
        Run the Meta Ads ETL pipeline to populate the database:
        ```bash
        python scripts/run_etl_meta.py --lifetime
        ```
        
        First, test your Meta connection:
        ```bash
        python scripts/test_meta_connection.py
        ```
        """)
        
        if meta_config:
            st.caption(f"Configured accounts: {', '.join(meta_config.ad_account_ids)}")
        return
    
    # Show data summary
    st.success(f"📊 Meta Ads data loaded: **{total_rows:,}** total rows across **{len(meta_tables)}** tables")
    
    # ========================================
    # Date Range Filter
    # ========================================
    
    # Import date picker component
    from app.components.date_picker import render_date_range_picker
    
    # Date range filter using calendar picker
    start_date, end_date, prev_start_date, prev_end_date = render_date_range_picker(
        key="meta_dashboard",
        default_days=30,
        max_days=365,
        show_comparison=True
    )
    
    # Convert to strings for SQL
    date_cutoff = start_date.strftime('%Y-%m-%d')
    prev_date_cutoff = prev_start_date.strftime('%Y-%m-%d') if prev_start_date else date_cutoff
    
    # Get account selector if multiple accounts
    accounts_query = "SELECT DISTINCT account_id as ad_account_id FROM meta_daily_account_v"
    accounts_df = load_duckdb_data(duckdb_path, accounts_query)
    
    if accounts_df is not None and len(accounts_df) > 1:
        account_options = ["All Accounts"] + accounts_df['ad_account_id'].tolist()
        selected_account = st.selectbox("📋 Select Account", account_options, key="meta_account")
        account_filter = "" if selected_account == "All Accounts" else f"AND ad_account_id = '{selected_account}'"
    else:
        selected_account = "All Accounts"
        account_filter = ""
    
    st.divider()
    
    # ========================================
    # SECTION 1: EXECUTIVE KPI DASHBOARD
    # ========================================
    st.subheader("🎯 Executive Summary")
    
    # Current period metrics (using silver view)
    kpi_query = f"""
    SELECT 
        SUM(impressions) as impressions,
        SUM(reach) as reach,
        SUM(clicks) as clicks,
        SUM(spend) as spend,
        CASE WHEN SUM(impressions) > 0 THEN SUM(clicks) * 100.0 / SUM(impressions) ELSE 0 END as ctr,
        CASE WHEN SUM(clicks) > 0 THEN SUM(spend) / SUM(clicks) ELSE 0 END as cpc,
        CASE WHEN SUM(impressions) > 0 THEN SUM(spend) * 1000.0 / SUM(impressions) ELSE 0 END as cpm,
        CASE WHEN SUM(reach) > 0 THEN SUM(impressions) * 1.0 / SUM(reach) ELSE 0 END as frequency,
        SUM(app_installs) as app_installs,
        SUM(purchases) as purchases,
        SUM(revenue) as revenue,
        CASE WHEN SUM(app_installs) > 0 THEN SUM(spend) / SUM(app_installs) ELSE 0 END as cpi
    FROM meta_daily_account_v
    WHERE date_day >= '{date_cutoff}' {account_filter.replace('ad_account_id', 'account_id')}
    """
    
    # Previous period metrics for comparison (using silver view)
    prev_kpi_query = f"""
    SELECT 
        SUM(impressions) as impressions,
        SUM(spend) as spend,
        SUM(clicks) as clicks,
        SUM(app_installs) as app_installs
    FROM meta_daily_account_v
    WHERE date_day >= '{prev_date_cutoff}' AND date_day < '{date_cutoff}' {account_filter.replace('ad_account_id', 'account_id')}
    """
    
    kpi_df = load_duckdb_data(duckdb_path, kpi_query)
    prev_kpi_df = load_duckdb_data(duckdb_path, prev_kpi_query)
    
    if kpi_df is not None and not kpi_df.empty and kpi_df['spend'].iloc[0]:
        row = kpi_df.iloc[0]
        prev_row = prev_kpi_df.iloc[0] if prev_kpi_df is not None and not prev_kpi_df.empty else None
        
        # Calculate deltas
        def calc_delta(current, previous):
            if previous and previous > 0:
                return ((current - previous) / previous) * 100
            return None
        
        # Row 1: Core metrics
        col1, col2, col3, col4, col5, col6 = st.columns(6)
        
        with col1:
            spend = row['spend'] or 0
            prev_spend = prev_row['spend'] if prev_row is not None else None
            delta = calc_delta(spend, prev_spend)
            st.metric(
                "💰 Total Spend",
                f"${spend:,.2f}",
                delta=f"{delta:+.1f}%" if delta else None,
                delta_color="inverse",
                help=TERM_TOOLTIPS.get("Spend"),
            )
        
        with col2:
            impressions = int(row['impressions'] or 0)
            delta = calc_delta(impressions, prev_row['impressions'] if prev_row is not None else None)
            st.metric(
                "👁️ Impressions",
                f"{impressions:,}",
                delta=f"{delta:+.1f}%" if delta else None,
                help=TERM_TOOLTIPS.get("Impressions"),
            )
        
        with col3:
            reach = int(row['reach'] or 0)
            st.metric("👥 Unique Reach", f"{reach:,}", help=TERM_TOOLTIPS.get("Reach"))
        
        with col4:
            clicks = int(row['clicks'] or 0)
            delta = calc_delta(clicks, prev_row['clicks'] if prev_row is not None else None)
            st.metric(
                "🖱️ Clicks",
                f"{clicks:,}",
                delta=f"{delta:+.1f}%" if delta else None,
                help=TERM_TOOLTIPS.get("Clicks"),
            )
        
        with col5:
            ctr = row['ctr'] or 0
            st.metric("📈 CTR", f"{ctr:.2f}%", help=TERM_TOOLTIPS.get("CTR"))
        
        with col6:
            cpc = row['cpc'] or 0
            st.metric("💵 CPC", f"${cpc:.2f}", help=TERM_TOOLTIPS.get("CPC"))
        
        # Row 2: Performance & Conversion metrics
        col1, col2, col3, col4, col5, col6 = st.columns(6)
        
        with col1:
            cpm = row['cpm'] or 0
            st.metric("📊 CPM", f"${cpm:.2f}", help=TERM_TOOLTIPS.get("CPM"))
        
        with col2:
            frequency = row['frequency'] or 0
            st.metric("🔄 Frequency", f"{frequency:.2f}", help=TERM_TOOLTIPS.get("Frequency"))
        
        with col3:
            installs = int(row['app_installs'] or 0)
            delta = calc_delta(installs, prev_row['app_installs'] if prev_row is not None else None)
            st.metric(
                "📱 App Installs",
                f"{installs:,}",
                delta=f"{delta:+.1f}%" if delta else None,
                help=TERM_TOOLTIPS.get("Installs"),
            )
        
        with col4:
            cpi = row['cpi'] or 0
            st.metric("💳 Cost/Install", f"${cpi:.2f}", help=TERM_TOOLTIPS.get("CPI"))
        
        with col5:
            purchases = int(row['purchases'] or 0)
            st.metric("🛒 Purchases", f"{purchases:,}", help="Completed purchases")
        
        with col6:
            revenue = row['revenue'] or 0
            roas = (revenue / spend * 100) if spend > 0 else 0
            st.metric("📈 ROAS", f"{roas:.1f}%", help=TERM_TOOLTIPS.get("ROAS"))
    
    st.divider()
    
    # ========================================
    # SECTION 2: PERFORMANCE TRENDS
    # ========================================
    st.subheader("📈 Performance Trends")
    
    trend_query = f"""
    SELECT 
        date_day as date,
        SUM(impressions) as impressions,
        SUM(clicks) as clicks,
        SUM(spend) as spend,
        SUM(app_installs) as app_installs,
        CASE WHEN SUM(impressions) > 0 THEN SUM(clicks) * 100.0 / SUM(impressions) ELSE 0 END as ctr,
        CASE WHEN SUM(clicks) > 0 THEN SUM(spend) / SUM(clicks) ELSE 0 END as cpc
    FROM meta_daily_account_v
    WHERE date_day >= '{date_cutoff}' {account_filter.replace('ad_account_id', 'account_id')}
    GROUP BY date_day
    ORDER BY date_day
    """
    
    trend_df = load_duckdb_data(duckdb_path, trend_query)
    
    if trend_df is not None and not trend_df.empty:
        tab1, tab2, tab3, tab4 = st.tabs(["📊 Spend & Clicks", "👁️ Impressions", "📱 Conversions", "📈 Efficiency"])
        
        with tab1:
            col1, col2 = st.columns(2)
            with col1:
                st.line_chart(trend_df.set_index('date')['spend'], use_container_width=True)
                st.caption("Daily Spend ($)")
            with col2:
                st.line_chart(trend_df.set_index('date')['clicks'], use_container_width=True)
                st.caption("Daily Clicks")
        
        with tab2:
            st.area_chart(trend_df.set_index('date')['impressions'], use_container_width=True)
            st.caption("Daily Impressions")
        
        with tab3:
            st.bar_chart(trend_df.set_index('date')['app_installs'], use_container_width=True)
            st.caption("Daily App Installs")
        
        with tab4:
            col1, col2 = st.columns(2)
            with col1:
                st.line_chart(trend_df.set_index('date')['ctr'], use_container_width=True)
                st.caption("Click-Through Rate (%)")
            with col2:
                st.line_chart(trend_df.set_index('date')['cpc'], use_container_width=True)
                st.caption("Cost Per Click ($)")
    
    st.divider()
    
    # ========================================
    # SECTION 3: CAMPAIGN PERFORMANCE
    # ========================================
    st.subheader("🎯 Campaign Performance Analysis")
    
    if 'meta_campaign_insights' in meta_tables:
        campaign_query = f"""
        SELECT 
            campaign_name,
            campaign_id,
            SUM(impressions) as impressions,
            SUM(reach) as reach,
            SUM(clicks) as clicks,
            SUM(spend) as spend,
            CASE WHEN SUM(impressions) > 0 THEN SUM(clicks) * 100.0 / SUM(impressions) ELSE 0 END as ctr,
            CASE WHEN SUM(clicks) > 0 THEN SUM(spend) / SUM(clicks) ELSE 0 END as cpc,
            CASE WHEN SUM(impressions) > 0 THEN SUM(spend) * 1000.0 / SUM(impressions) ELSE 0 END as cpm,
            SUM(app_installs) as app_installs,
            CASE WHEN SUM(app_installs) > 0 THEN SUM(spend) / SUM(app_installs) ELSE 0 END as cpi,
            SUM(purchases) as purchases,
            SUM(revenue) as revenue
        FROM meta_campaign_insights_v
        WHERE date_day >= '{date_cutoff}' {account_filter.replace('ad_account_id', 'account_id')}
        GROUP BY campaign_name, campaign_id
        ORDER BY spend DESC
        """
        
        campaign_df = load_duckdb_data(duckdb_path, campaign_query)
        
        if campaign_df is not None and not campaign_df.empty:
            # Campaign efficiency quadrant
            col1, col2 = st.columns([2, 1])
            
            with col1:
                # Format for display
                display_df = campaign_df.copy()
                display_df['spend'] = display_df['spend'].apply(lambda x: f"${x:,.2f}")
                display_df['ctr'] = display_df['ctr'].apply(lambda x: f"{x:.2f}%")
                display_df['cpc'] = display_df['cpc'].apply(lambda x: f"${x:.2f}")
                display_df['cpm'] = display_df['cpm'].apply(lambda x: f"${x:.2f}")
                display_df['cpi'] = display_df['cpi'].apply(lambda x: f"${x:.2f}" if x > 0 else "-")
                display_df['impressions'] = display_df['impressions'].apply(lambda x: f"{int(x):,}")
                display_df['clicks'] = display_df['clicks'].apply(lambda x: f"{int(x):,}")
                display_df['app_installs'] = display_df['app_installs'].apply(lambda x: f"{int(x):,}" if x > 0 else "-")
                
                st.dataframe(
                    display_df[['campaign_name', 'spend', 'impressions', 'clicks', 'ctr', 'cpc', 'app_installs', 'cpi']],
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "campaign_name": "Campaign",
                        "spend": "Spend",
                        "impressions": "Impressions",
                        "clicks": "Clicks",
                        "ctr": "CTR",
                        "cpc": "CPC",
                        "app_installs": "Installs",
                        "cpi": "CPI"
                    }
                )
            
            with col2:
                st.markdown("**📊 Spend Distribution**")
                spend_data = campaign_df[['campaign_name', 'spend']].head(10)
                spend_data = spend_data[spend_data['spend'] > 0]
                if not spend_data.empty:
                    st.bar_chart(spend_data.set_index('campaign_name')['spend'])
        
        # Campaign time series
        st.markdown("**📈 Campaign Performance Over Time**")
        
        campaign_trend_query = f"""
        SELECT 
            date_day as date,
            campaign_name,
            SUM(spend) as spend,
            SUM(clicks) as clicks
        FROM meta_campaign_insights_v
        WHERE date_day >= '{date_cutoff}' {account_filter.replace('ad_account_id', 'account_id')}
        GROUP BY date_day, campaign_name
        ORDER BY date_day
        """
        
        campaign_trend_df = load_duckdb_data(duckdb_path, campaign_trend_query)
        
        if campaign_trend_df is not None and not campaign_trend_df.empty:
            # Pivot for time series
            pivot_df = campaign_trend_df.pivot_table(
                index='date', 
                columns='campaign_name', 
                values='spend', 
                aggfunc='sum'
            ).fillna(0)
            
            if not pivot_df.empty:
                st.line_chart(pivot_df, use_container_width=True)
    
    st.divider()
    
    # ========================================
    # SECTION 4: AD SET PERFORMANCE
    # ========================================
    st.subheader("🎨 Ad Set (Targeting) Analysis")
    
    if 'meta_adset_insights' in meta_tables:
        adset_query = f"""
        SELECT 
            ad_group_name as adset_name,
            campaign_name,
            SUM(impressions) as impressions,
            SUM(clicks) as clicks,
            SUM(spend) as spend,
            CASE WHEN SUM(impressions) > 0 THEN SUM(clicks) * 100.0 / SUM(impressions) ELSE 0 END as ctr,
            CASE WHEN SUM(clicks) > 0 THEN SUM(spend) / SUM(clicks) ELSE 0 END as cpc,
            SUM(app_installs) as app_installs,
            CASE WHEN SUM(app_installs) > 0 THEN SUM(spend) / SUM(app_installs) ELSE 0 END as cpi
        FROM meta_adset_insights_v
        WHERE date_day >= '{date_cutoff}' {account_filter.replace('ad_account_id', 'account_id')}
        GROUP BY ad_group_name, campaign_name
        ORDER BY spend DESC
        LIMIT 20
        """
        
        adset_df = load_duckdb_data(duckdb_path, adset_query)
        
        if adset_df is not None and not adset_df.empty:
            # Format for display
            display_df = adset_df.copy()
            display_df['spend'] = display_df['spend'].apply(lambda x: f"${x:,.2f}")
            display_df['ctr'] = display_df['ctr'].apply(lambda x: f"{x:.2f}%")
            display_df['cpc'] = display_df['cpc'].apply(lambda x: f"${x:.2f}")
            display_df['cpi'] = display_df['cpi'].apply(lambda x: f"${x:.2f}" if x > 0 else "-")
            display_df['impressions'] = display_df['impressions'].apply(lambda x: f"{int(x):,}")
            display_df['clicks'] = display_df['clicks'].apply(lambda x: f"{int(x):,}")
            display_df['app_installs'] = display_df['app_installs'].apply(lambda x: f"{int(x):,}" if x > 0 else "-")
            
            st.dataframe(
                display_df[['adset_name', 'campaign_name', 'spend', 'clicks', 'ctr', 'cpc', 'app_installs', 'cpi']],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "adset_name": "Ad Set",
                    "campaign_name": "Campaign",
                    "spend": "Spend",
                    "clicks": "Clicks",
                    "ctr": "CTR",
                    "cpc": "CPC",
                    "app_installs": "Installs",
                    "cpi": "CPI"
                }
            )
    
    st.divider()
    
    # ========================================
    # SECTION 5: GEOGRAPHIC ANALYSIS
    # ========================================
    st.subheader("🌍 Geographic Performance")
    
    if 'meta_geographic' in meta_tables:
        # Note: Geographic data is aggregated (not daily), so no date filter needed
        geo_where = f"WHERE 1=1 {account_filter.replace('ad_account_id', 'account_id')}" if account_filter else ""
        geo_query = f"""
        SELECT 
            country,
            SUM(impressions) as impressions,
            SUM(clicks) as clicks,
            SUM(spend) as spend,
            CASE WHEN SUM(impressions) > 0 THEN SUM(clicks) * 100.0 / SUM(impressions) ELSE 0 END as ctr,
            CASE WHEN SUM(clicks) > 0 THEN SUM(spend) / SUM(clicks) ELSE 0 END as cpc,
            SUM(app_installs) as app_installs,
            CASE WHEN SUM(app_installs) > 0 THEN SUM(spend) / SUM(app_installs) ELSE 0 END as cpi
        FROM meta_geographic_v
        {geo_where}
        GROUP BY country
        ORDER BY spend DESC
        """
        
        geo_df = load_duckdb_data(duckdb_path, geo_query)
        
        if geo_df is not None and not geo_df.empty:
            import plotly.express as px
            
            geo_col1, geo_col2 = st.columns(2)
            
            with geo_col1:
                # Choropleth world map
                fig_map = px.choropleth(
                    geo_df,
                    locations="country",
                    locationmode="country names",
                    color="spend",
                    hover_name="country",
                    hover_data={
                        "spend": "$.2f",
                        "clicks": ":,",
                        "app_installs": ":,",
                        "ctr": ":.2f"
                    },
                    color_continuous_scale="Purples",
                    title="Ad Spend by Country"
                )
                
                fig_map.update_layout(
                    geo=dict(
                        showframe=False,
                        showcoastlines=True,
                        projection_type='natural earth'
                    ),
                    margin=dict(l=0, r=0, t=40, b=0),
                    height=350,
                    coloraxis_colorbar=dict(title="Spend ($)")
                )
                
                st.plotly_chart(fig_map, use_container_width=True)
            
            with geo_col2:
                # Pie chart for top countries by spend
                top_countries = geo_df.head(8).copy()
                other_spend = geo_df.iloc[8:]['spend'].sum() if len(geo_df) > 8 else 0
                
                if other_spend > 0:
                    other_row = pd.DataFrame([{
                        'country': 'Others',
                        'spend': other_spend,
                        'clicks': 0,
                        'impressions': 0,
                        'app_installs': 0,
                        'ctr': 0,
                        'cpc': 0,
                        'cpi': 0
                    }])
                    top_countries = pd.concat([top_countries, other_row], ignore_index=True)
                
                fig_pie = px.pie(
                    top_countries,
                    values='spend',
                    names='country',
                    title='Spend Distribution by Country',
                    hole=0.4,
                    color_discrete_sequence=px.colors.qualitative.Pastel
                )
                
                fig_pie.update_layout(
                    margin=dict(l=0, r=0, t=40, b=0),
                    height=350,
                    showlegend=True,
                    legend=dict(orientation="h", yanchor="bottom", y=-0.3, xanchor="center", x=0.5)
                )
                
                fig_pie.update_traces(
                    textposition='inside',
                    textinfo='percent+label',
                    hovertemplate='<b>%{label}</b><br>Spend: $%{value:,.2f}<extra></extra>'
                )
                
                st.plotly_chart(fig_pie, use_container_width=True)
            
            # Data table with metrics
            st.caption("**Country Performance Metrics**")
            display_df = geo_df.copy()
            display_df['spend'] = display_df['spend'].apply(lambda x: f"${x:,.2f}")
            display_df['ctr'] = display_df['ctr'].apply(lambda x: f"{x:.2f}%")
            display_df['cpc'] = display_df['cpc'].apply(lambda x: f"${x:.2f}")
            display_df['cpi'] = display_df['cpi'].apply(lambda x: f"${x:.2f}" if x > 0 else "-")
            display_df['clicks'] = display_df['clicks'].apply(lambda x: f"{int(x):,}")
            display_df['app_installs'] = display_df['app_installs'].apply(lambda x: f"{int(x):,}" if x > 0 else "-")
            
            st.dataframe(
                display_df[['country', 'spend', 'clicks', 'ctr', 'cpc', 'app_installs', 'cpi']].head(15),
                use_container_width=True,
                hide_index=True,
                column_config={
                    "country": "Country",
                    "spend": "Spend",
                    "clicks": "Clicks",
                    "ctr": "CTR",
                    "cpc": "CPC",
                    "app_installs": "Installs",
                    "cpi": "CPI"
                }
            )
    else:
        st.info("No geographic data available. Run Meta Ads ETL to populate.")
    
    st.divider()
    
    # ========================================
    # SECTION 6: DEVICE & PLATFORM ANALYSIS
    # ========================================
    st.subheader("📱 Device & Platform Analysis")
    
    col1, col2 = st.columns(2)
    
    if 'meta_devices' in meta_tables:
        # Note: Device data is aggregated (not daily), so no date filter needed
        device_where = f"WHERE 1=1 {account_filter.replace('ad_account_id', 'account_id')}" if account_filter else ""
        device_query = f"""
        SELECT 
            device_platform,
            publisher_platform,
            SUM(impressions) as impressions,
            SUM(clicks) as clicks,
            SUM(spend) as spend,
            CASE WHEN SUM(impressions) > 0 THEN SUM(clicks) * 100.0 / SUM(impressions) ELSE 0 END as ctr,
            CASE WHEN SUM(clicks) > 0 THEN SUM(spend) / SUM(clicks) ELSE 0 END as cpc,
            SUM(app_installs) as app_installs
        FROM meta_devices_v
        {device_where}
        GROUP BY device_platform, publisher_platform
        ORDER BY spend DESC
        """
        
        device_df = load_duckdb_data(duckdb_path, device_query)
        
        if device_df is not None and not device_df.empty:
            with col1:
                st.markdown("**📲 Device Platform**")
                device_agg = device_df.groupby('device_platform')['spend'].sum().reset_index()
                st.bar_chart(device_agg.set_index('device_platform'))
            
            with col2:
                st.markdown("**📡 Publisher Platform**")
                pub_agg = device_df.groupby('publisher_platform')['spend'].sum().reset_index()
                st.bar_chart(pub_agg.set_index('publisher_platform'))
            
            # Detailed table
            st.markdown("**📊 Detailed Platform Metrics**")
            display_df = device_df.copy()
            display_df['spend'] = display_df['spend'].apply(lambda x: f"${x:,.2f}")
            display_df['ctr'] = display_df['ctr'].apply(lambda x: f"{x:.2f}%")
            display_df['cpc'] = display_df['cpc'].apply(lambda x: f"${x:.2f}")
            display_df['impressions'] = display_df['impressions'].apply(lambda x: f"{int(x):,}")
            display_df['clicks'] = display_df['clicks'].apply(lambda x: f"{int(x):,}")
            
            st.dataframe(
                display_df[['device_platform', 'publisher_platform', 'spend', 'impressions', 'clicks', 'ctr', 'cpc']],
                use_container_width=True,
                hide_index=True
            )
    
    st.divider()
    
    # ========================================
    # SECTION 7: DEMOGRAPHICS ANALYSIS
    # ========================================
    st.subheader("👥 Demographics Analysis")
    
    if 'meta_demographics' in meta_tables:
        # Note: Demographics data is aggregated (not daily), so no date filter needed
        demo_where = f"WHERE 1=1 {account_filter.replace('ad_account_id', 'account_id')}" if account_filter else ""
        demo_query = f"""
        SELECT 
            age,
            gender,
            SUM(impressions) as impressions,
            SUM(clicks) as clicks,
            SUM(spend) as spend,
            CASE WHEN SUM(impressions) > 0 THEN SUM(clicks) * 100.0 / SUM(impressions) ELSE 0 END as ctr,
            CASE WHEN SUM(clicks) > 0 THEN SUM(spend) / SUM(clicks) ELSE 0 END as cpc,
            SUM(app_installs) as app_installs
        FROM meta_demographics_v
        {demo_where}
        GROUP BY age, gender
        ORDER BY spend DESC
        """
        
        demo_df = load_duckdb_data(duckdb_path, demo_query)
        
        if demo_df is not None and not demo_df.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**👤 Spend by Age Group**")
                age_agg = demo_df.groupby('age')['spend'].sum().reset_index()
                # Sort by age properly
                age_order = ['13-17', '18-24', '25-34', '35-44', '45-54', '55-64', '65+']
                age_agg['age'] = pd.Categorical(age_agg['age'], categories=age_order, ordered=True)
                age_agg = age_agg.sort_values('age')
                st.bar_chart(age_agg.set_index('age'))
            
            with col2:
                st.markdown("**⚧️ Spend by Gender**")
                gender_agg = demo_df.groupby('gender')['spend'].sum().reset_index()
                st.bar_chart(gender_agg.set_index('gender'))
            
            # Demographics heatmap-style table
            st.markdown("**📊 Age x Gender Performance Matrix**")
            
            # Pivot for matrix view
            matrix_df = demo_df.pivot_table(
                index='age',
                columns='gender',
                values='spend',
                aggfunc='sum'
            ).fillna(0)
            
            if not matrix_df.empty:
                # Format as currency
                formatted_matrix = matrix_df.applymap(lambda x: f"${x:,.2f}")
                st.dataframe(formatted_matrix, use_container_width=True)
    
    st.divider()
    
    # ========================================
    # SECTION 8: RAW DATA EXPLORER
    # ========================================
    with st.expander("📋 Explore Raw Meta Ads Data"):
        table_choice = st.selectbox(
            "Select Table",
            options=meta_tables,
            key="meta_table_choice"
        )
        
        if table_choice:
            raw_df = load_duckdb_data(duckdb_path, f"SELECT * FROM {table_choice} ORDER BY date DESC LIMIT 1000")
            if raw_df is not None:
                st.dataframe(raw_df, use_container_width=True)
    
    # ========================================
    # SECTION 9: MBA INSIGHTS & RECOMMENDATIONS
    # ========================================
    st.divider()
    st.subheader("💡 Strategic Insights & Recommendations")
    
    if kpi_df is not None and not kpi_df.empty and kpi_df['spend'].iloc[0]:
        row = kpi_df.iloc[0]
        
        insights = []
        
        # CTR analysis
        ctr = row['ctr'] or 0
        if ctr < 0.5:
            insights.append("⚠️ **Low CTR Alert**: CTR is below 0.5%. Consider refreshing ad creatives or refining targeting.")
        elif ctr > 1.5:
            insights.append("✅ **Strong CTR**: CTR exceeds 1.5%, indicating good audience-creative fit.")
        
        # Frequency analysis
        frequency = row['frequency'] or 0
        if frequency > 3:
            insights.append("⚠️ **High Frequency Warning**: Frequency > 3 may cause ad fatigue. Consider expanding audience or refreshing creatives.")
        
        # CPI analysis (if app installs)
        cpi = row['cpi'] or 0
        installs = row['app_installs'] or 0
        if installs > 0:
            if cpi > 5:
                insights.append(f"⚠️ **CPI Optimization Needed**: Cost per install (${cpi:.2f}) is high. Review targeting and creatives.")
            elif cpi < 2:
                insights.append(f"✅ **Efficient CPI**: Cost per install (${cpi:.2f}) is efficient. Consider scaling budget.")
        
        # Budget efficiency
        spend = row['spend'] or 0
        clicks = row['clicks'] or 0
        if spend > 0 and clicks > 0:
            efficiency_ratio = clicks / spend
            if efficiency_ratio < 0.5:
                insights.append("📊 **Budget Efficiency**: Consider reallocating budget to higher-performing campaigns.")
        
        if insights:
            for insight in insights:
                st.markdown(insight)
        else:
            st.info("📊 Performance metrics are within normal ranges. Continue monitoring for trends.")


# ============================================
# Twitter/X Dashboard Page
# ============================================
def render_twitter_dashboard(twitter_config, duckdb_path: str):
    """
    Render the Twitter/X organic analytics dashboard.
    
    Displays:
    - Profile metrics (followers, following, tweets)
    - Tweet performance (impressions, engagements)
    - Daily metrics trends
    - Top performing tweets
    """
    
    st.header("🐦 Twitter/X - Page Analytics Dashboard")
    
    # Check if data exists
    has_data, total_rows, twitter_tables = check_twitter_data_exists(duckdb_path)
    
    if not has_data or total_rows == 0:
        st.info("""
        **No Twitter data available yet.**
        
        To populate data:
        1. Ensure Twitter API credentials are configured in `.env`
        2. Run the ETL: `python scripts/run_etl_twitter.py`
        
        **Note:** Twitter API requires a paid subscription ($100+/month) for read access.
        """)
        
        # Show ETL instructions
        with st.expander("📋 Setup Instructions"):
            st.markdown("""
            ### Twitter API Setup
            
            1. **Get API Access**: Sign up at [developer.twitter.com](https://developer.twitter.com)
            2. **Subscribe to Basic tier** ($100/month) for read access
            3. **Configure credentials** in `.env`:
               ```
               ENABLE_TWITTER=1
               TWITTER_BEARER_TOKEN=your_bearer_token
               TWITTER_CONSUMER_KEY=your_consumer_key
               TWITTER_CONSUMER_SECRET=your_consumer_secret
               TWITTER_ACCESS_TOKEN=your_access_token
               TWITTER_ACCESS_TOKEN_SECRET=your_access_token_secret
               TWITTER_USERNAME=YourUsername
               ```
            4. **Test connection**: `python scripts/test_twitter_connection.py`
            5. **Run ETL**: `python scripts/run_etl_twitter.py`
            """)
        return
    
    # Import date picker component
    from app.components.date_picker import render_date_range_picker
    
    # Date range filter using calendar picker
    start_date, end_date, _, _ = render_date_range_picker(
        key="twitter_dashboard",
        default_days=30,
        max_days=365,
        show_comparison=False
    )
    
    # Convert to string for SQL
    date_cutoff = start_date.strftime('%Y-%m-%d')
    
    # ========================================
    # SECTION 1: PROFILE OVERVIEW
    # ========================================
    st.subheader("👤 Profile Overview")
    
    if 'twitter_profile' in twitter_tables:
        profile_query = """
        SELECT *
        FROM twitter_profile
        ORDER BY snapshot_date DESC
        LIMIT 1
        """
        profile_df = load_duckdb_data(duckdb_path, profile_query)
        
        if profile_df is not None and not profile_df.empty:
            row = profile_df.iloc[0]
            
            col1, col2, col3, col4, col5 = st.columns(5)
            
            with col1:
                st.metric("👥 Followers", f"{int(row['followers_count']):,}")
            with col2:
                st.metric("➡️ Following", f"{int(row['following_count']):,}")
            with col3:
                st.metric("📝 Total Tweets", f"{int(row['tweet_count']):,}")
            with col4:
                st.metric("📋 Listed", f"{int(row['listed_count']):,}")
            with col5:
                verified = "✅ Yes" if row.get('verified', False) else "❌ No"
                st.metric("✓ Verified", verified)
            
            # Profile info
            with st.expander("📄 Profile Details"):
                st.markdown(f"**Username:** @{row['username']}")
                st.markdown(f"**Name:** {row['name']}")
                st.markdown(f"**Bio:** {row.get('description', 'N/A')}")
                st.markdown(f"**Location:** {row.get('location', 'N/A')}")
                st.markdown(f"**Account Created:** {row.get('created_at', 'N/A')[:10] if row.get('created_at') else 'N/A'}")
                st.markdown(f"**Last Updated:** {row['snapshot_date']}")
    
    st.divider()
    
    # ========================================
    # SECTION 2: ENGAGEMENT METRICS
    # ========================================
    st.subheader("📊 Engagement Metrics")
    
    if 'twitter_tweets' in twitter_tables:
        # Aggregate metrics
        metrics_query = f"""
        SELECT 
            COUNT(*) as total_tweets,
            SUM(impressions) as total_impressions,
            SUM(likes) as total_likes,
            SUM(retweets) as total_retweets,
            SUM(replies) as total_replies,
            SUM(quotes) as total_quotes,
            SUM(bookmarks) as total_bookmarks,
            SUM(likes + retweets + replies + quotes + bookmarks) as total_engagements,
            AVG(likes) as avg_likes,
            AVG(retweets) as avg_retweets
        FROM twitter_tweets
        WHERE created_date >= '{date_cutoff}'
        """
        
        metrics_df = load_duckdb_data(duckdb_path, metrics_query)
        
        if metrics_df is not None and not metrics_df.empty:
            row = metrics_df.iloc[0]
            
            col1, col2, col3, col4, col5, col6 = st.columns(6)
            
            with col1:
                st.metric("📝 Tweets", f"{int(row['total_tweets'] or 0):,}")
            with col2:
                st.metric("👁️ Impressions", f"{int(row['total_impressions'] or 0):,}")
            with col3:
                st.metric("❤️ Likes", f"{int(row['total_likes'] or 0):,}")
            with col4:
                st.metric("🔄 Retweets", f"{int(row['total_retweets'] or 0):,}")
            with col5:
                st.metric("💬 Replies", f"{int(row['total_replies'] or 0):,}")
            with col6:
                engagements = int(row['total_engagements'] or 0)
                impressions = int(row['total_impressions'] or 1)
                engagement_rate = (engagements / impressions * 100) if impressions > 0 else 0
                st.metric("📈 Eng. Rate", f"{engagement_rate:.2f}%")
    
    st.divider()
    
    # ========================================
    # SECTION 3: DAILY TRENDS
    # ========================================
    st.subheader("📈 Daily Performance Trends")
    
    if 'twitter_daily_metrics' in twitter_tables:
        trend_query = f"""
        SELECT 
            date,
            tweet_count,
            impressions,
            likes,
            retweets,
            replies,
            quotes,
            total_engagements,
            engagement_rate
        FROM twitter_daily_metrics
        WHERE date >= '{date_cutoff}'
        ORDER BY date
        """
        
        trend_df = load_duckdb_data(duckdb_path, trend_query)
        
        if trend_df is not None and not trend_df.empty:
            tab1, tab2, tab3, tab4 = st.tabs(["📊 Impressions", "❤️ Engagements", "📝 Tweets", "📈 Eng. Rate"])
            
            with tab1:
                st.bar_chart(trend_df.set_index('date')['impressions'], use_container_width=True)
                st.caption("Daily Impressions")
            
            with tab2:
                # Stacked engagement chart
                eng_df = trend_df.set_index('date')[['likes', 'retweets', 'replies', 'quotes']]
                st.bar_chart(eng_df, use_container_width=True)
                st.caption("Daily Engagements by Type")
            
            with tab3:
                st.bar_chart(trend_df.set_index('date')['tweet_count'], use_container_width=True)
                st.caption("Daily Tweet Count")
            
            with tab4:
                st.line_chart(trend_df.set_index('date')['engagement_rate'], use_container_width=True)
                st.caption("Daily Engagement Rate (%)")
        else:
            st.info("No daily metrics data available for the selected period.")
    
    st.divider()
    
    # ========================================
    # SECTION 4: TOP PERFORMING TWEETS
    # ========================================
    st.subheader("🏆 Top Performing Tweets")
    
    if 'twitter_tweets' in twitter_tables:
        col1, col2 = st.columns(2)
        
        with col1:
            sort_by = st.selectbox(
                "Sort by",
                options=["impressions", "likes", "retweets", "replies", "total_engagement"],
                format_func=lambda x: {
                    "impressions": "👁️ Impressions",
                    "likes": "❤️ Likes",
                    "retweets": "🔄 Retweets",
                    "replies": "💬 Replies",
                    "total_engagement": "📊 Total Engagement"
                }.get(x, x),
                key="twitter_sort"
            )
        
        with col2:
            tweet_type_filter = st.selectbox(
                "Tweet Type",
                options=["All", "original", "reply", "quote"],
                format_func=lambda x: {
                    "All": "All Tweets",
                    "original": "Original Tweets",
                    "reply": "Replies",
                    "quote": "Quote Tweets"
                }.get(x, x),
                key="twitter_type"
            )
        
        # Build query
        type_filter = f"AND tweet_type = '{tweet_type_filter}'" if tweet_type_filter != "All" else ""
        
        if sort_by == "total_engagement":
            order_by = "(likes + retweets + replies + quotes + bookmarks)"
        else:
            order_by = sort_by
        
        top_tweets_query = f"""
        SELECT 
            tweet_id,
            text,
            tweet_type,
            created_date,
            impressions,
            likes,
            retweets,
            replies,
            quotes,
            bookmarks,
            (likes + retweets + replies + quotes + bookmarks) as total_engagement
        FROM twitter_tweets
        WHERE created_date >= '{date_cutoff}' {type_filter}
        ORDER BY {order_by} DESC
        LIMIT 10
        """
        
        top_df = load_duckdb_data(duckdb_path, top_tweets_query)
        
        if top_df is not None and not top_df.empty:
            for _, tweet in top_df.iterrows():
                with st.container():
                    # Tweet header
                    col1, col2 = st.columns([4, 1])
                    with col1:
                        text = tweet['text']
                        if len(text) > 200:
                            text = text[:200] + "..."
                        st.markdown(f"**{tweet['created_date']}** • _{tweet['tweet_type']}_")
                        st.markdown(text)
                    
                    with col2:
                        st.caption(f"👁️ {int(tweet['impressions'] or 0):,}")
                    
                    # Metrics row
                    mcol1, mcol2, mcol3, mcol4, mcol5 = st.columns(5)
                    mcol1.caption(f"❤️ {int(tweet['likes']):,}")
                    mcol2.caption(f"🔄 {int(tweet['retweets']):,}")
                    mcol3.caption(f"💬 {int(tweet['replies']):,}")
                    mcol4.caption(f"📝 {int(tweet['quotes']):,}")
                    mcol5.caption(f"🔖 {int(tweet['bookmarks']):,}")
                    
                    st.divider()
        else:
            st.info("No tweets found for the selected filters.")
    
    st.divider()
    
    # ========================================
    # SECTION 5: TWEET TYPE BREAKDOWN
    # ========================================
    st.subheader("📊 Tweet Type Analysis")
    
    if 'twitter_tweets' in twitter_tables:
        type_query = f"""
        SELECT 
            tweet_type,
            COUNT(*) as count,
            SUM(impressions) as impressions,
            SUM(likes) as likes,
            SUM(retweets) as retweets,
            AVG(impressions) as avg_impressions,
            AVG(likes) as avg_likes
        FROM twitter_tweets
        WHERE created_date >= '{date_cutoff}'
        GROUP BY tweet_type
        ORDER BY count DESC
        """
        
        type_df = load_duckdb_data(duckdb_path, type_query)
        
        if type_df is not None and not type_df.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**📊 Tweet Count by Type**")
                st.bar_chart(type_df.set_index('tweet_type')['count'])
            
            with col2:
                st.markdown("**📈 Avg Engagement by Type**")
                type_df['avg_engagement'] = type_df['avg_likes'] + (type_df['impressions'] / type_df['count'].replace(0, 1) * 0.01)
                st.bar_chart(type_df.set_index('tweet_type')['avg_likes'])
    
    st.divider()
    
    # ========================================
    # SECTION 6: RAW DATA EXPLORER
    # ========================================
    with st.expander("🔍 Raw Data Explorer"):
        table_to_view = st.selectbox(
            "Select Table",
            options=twitter_tables,
            key="twitter_raw_table"
        )
        
        if table_to_view:
            raw_query = f"SELECT * FROM {table_to_view} ORDER BY 1 DESC LIMIT 100"
            raw_df = load_duckdb_data(duckdb_path, raw_query)
            
            if raw_df is not None:
                st.dataframe(raw_df, use_container_width=True, hide_index=True)
                st.caption(f"Showing up to 100 rows from {table_to_view}")


# ============================================
# Advanced Analytics Pages
# (Implemented in app/components/ modules)
# ============================================

# Import the render functions from their dedicated component files.
# Each module handles its own data loading, ML model training, and visualization.
from app.components.behavioral_analysis import render_behavioral_analysis  # noqa: E402
from app.components.forecasting import render_forecasting                  # noqa: E402
from app.components.clustering import render_clustering                    # noqa: E402


# ============================================
# ETL Control Panel
# ============================================
def render_etl_control_panel(duckdb_path: str):
    """
    Render the ETL Control Panel for manual data pulls.
    
    Provides buttons for:
    - Full Lifetime Pull (all historical data)
    - Daily Refresh (incremental update, last 3 days)
    - Individual source pulls
    - ETL status monitoring
    """
    
    st.header("🔧 ETL Control Panel")
    st.markdown("""
    Use this panel to manually trigger data extraction from all connected platforms.
    
    - **Lifetime Pull**: Extracts ALL historical data (replaces existing tables)
    - **Daily Refresh**: Extracts last 3 days (updates existing data without losing history)
    """)
    
    st.divider()
    
    # ========================================
    # SECTION 1: Quick Actions
    # ========================================
    st.subheader("⚡ Quick Actions")
    
    col1, col2 = st.columns(2)
    
    # Initialize session state for ETL status
    if 'etl_running' not in st.session_state:
        st.session_state.etl_running = False
    if 'etl_output' not in st.session_state:
        st.session_state.etl_output = ""
    if 'etl_status' not in st.session_state:
        st.session_state.etl_status = None
    
    with col1:
        st.markdown("### 📥 Full Lifetime Pull")
        st.caption("Extract ALL historical data from all platforms. This replaces existing data.")
        
        if st.button(
            "🚀 Run Lifetime ETL",
            disabled=st.session_state.etl_running,
            use_container_width=True,
            type="primary"
        ):
            st.session_state.etl_running = True
            st.session_state.etl_status = "running"
            
            with st.spinner("Running Lifetime ETL... This may take several minutes."):
                try:
                    result = subprocess.run(
                        [sys.executable, "scripts/run_etl_unified.py", 
                         "--source", "all", "--lifetime"],
                        capture_output=True,
                        text=True,
                        cwd=str(project_root),
                        timeout=1800  # 30 minute timeout
                    )
                    
                    st.session_state.etl_output = result.stdout + "\n" + result.stderr
                    
                    if result.returncode == 0:
                        st.session_state.etl_status = "success"
                        st.success("✅ Lifetime ETL completed successfully!")
                    else:
                        st.session_state.etl_status = "error"
                        st.error(f"❌ ETL failed with exit code {result.returncode}")
                        
                except subprocess.TimeoutExpired:
                    st.session_state.etl_status = "timeout"
                    st.error("⏱️ ETL timed out after 30 minutes")
                except Exception as e:
                    st.session_state.etl_status = "error"
                    st.error(f"❌ ETL failed: {e}")
                finally:
                    st.session_state.etl_running = False
                    # Clear cache to refresh data
                    st.cache_data.clear()
    
    with col2:
        st.markdown("### 🔄 Daily Refresh")
        st.caption("Extract last 3 days of data. Updates existing records without losing history.")
        
        if st.button(
            "🔄 Run Daily Refresh",
            disabled=st.session_state.etl_running,
            use_container_width=True,
            type="secondary"
        ):
            st.session_state.etl_running = True
            st.session_state.etl_status = "running"
            
            with st.spinner("Running Daily Refresh... This may take a few minutes."):
                try:
                    result = subprocess.run(
                        [sys.executable, "scripts/run_etl_unified.py", 
                         "--source", "all", "--lookback-days", "3"],
                        capture_output=True,
                        text=True,
                        cwd=str(project_root),
                        timeout=900  # 15 minute timeout
                    )
                    
                    st.session_state.etl_output = result.stdout + "\n" + result.stderr
                    
                    if result.returncode == 0:
                        st.session_state.etl_status = "success"
                        st.success("✅ Daily refresh completed successfully!")
                    else:
                        st.session_state.etl_status = "error"
                        st.error(f"❌ ETL failed with exit code {result.returncode}")
                        
                except subprocess.TimeoutExpired:
                    st.session_state.etl_status = "timeout"
                    st.error("⏱️ ETL timed out after 15 minutes")
                except Exception as e:
                    st.session_state.etl_status = "error"
                    st.error(f"❌ ETL failed: {e}")
                finally:
                    st.session_state.etl_running = False
                    # Clear cache to refresh data
                    st.cache_data.clear()
    
    st.divider()
    
    # ========================================
    # SECTION 2: Individual Source ETL
    # ========================================
    st.subheader("🎯 Individual Source ETL")
    st.caption("Run ETL for specific data sources")
    
    source_col1, source_col2, source_col3, source_col4, source_col5 = st.columns(5)
    
    sources = [
        ("ga4", "📊 GA4", source_col1),
        ("gsc", "🔍 GSC", source_col2),
        ("gads", "💰 Google Ads", source_col3),
        ("meta", "📘 Meta", source_col4),
        ("twitter", "🐦 Twitter", source_col5),
    ]
    
    # Mode selection
    mode = st.radio(
        "ETL Mode",
        options=["Daily (last 3 days)", "Lifetime (all data)"],
        horizontal=True,
        key="etl_mode_radio"
    )
    
    is_lifetime = "Lifetime" in mode
    
    for source_id, source_label, col in sources:
        with col:
            if st.button(
                source_label,
                disabled=st.session_state.etl_running,
                use_container_width=True,
                key=f"etl_{source_id}"
            ):
                st.session_state.etl_running = True
                
                with st.spinner(f"Running {source_label} ETL..."):
                    try:
                        cmd = [sys.executable, "scripts/run_etl_unified.py", 
                               "--source", source_id]
                        if is_lifetime:
                            cmd.append("--lifetime")
                        else:
                            cmd.extend(["--lookback-days", "3"])
                        
                        result = subprocess.run(
                            cmd,
                            capture_output=True,
                            text=True,
                            cwd=str(project_root),
                            timeout=600  # 10 minute timeout
                        )
                        
                        st.session_state.etl_output = result.stdout + "\n" + result.stderr
                        
                        if result.returncode == 0:
                            st.success(f"✅ {source_label} ETL completed!")
                        else:
                            st.error(f"❌ {source_label} ETL failed")
                            
                    except subprocess.TimeoutExpired:
                        st.error(f"⏱️ {source_label} ETL timed out")
                    except Exception as e:
                        st.error(f"❌ {source_label} ETL failed: {e}")
                    finally:
                        st.session_state.etl_running = False
                        st.cache_data.clear()
    
    st.divider()
    
    # ========================================
    # SECTION 3: ETL Output Log
    # ========================================
    with st.expander("📋 ETL Output Log", expanded=st.session_state.etl_status == "error"):
        if st.session_state.etl_output:
            st.code(st.session_state.etl_output, language="text")
        else:
            st.info("No ETL output yet. Run an ETL job to see output.")
    
    st.divider()
    
    # ========================================
    # SECTION 4: Data Status Summary
    # ========================================
    st.subheader("📊 Current Data Status")
    
    table_info = get_table_info(duckdb_path)
    
    if table_info:
        # Group by source
        source_groups = {
            'GA4': ('ga4_', '📊'),
            'GSC': ('gsc_', '🔍'),
            'Google Ads': ('gads_', '💰'),
            'Meta Ads': ('meta_', '📘'),
            'Twitter': ('twitter_', '🐦'),
        }
        
        data_rows = []
        for source_name, (prefix, icon) in source_groups.items():
            source_tables = {k: v for k, v in table_info.items() if k.startswith(prefix)}
            total_rows = sum(source_tables.values())
            table_count = len(source_tables)
            
            # Get date range if possible
            date_range = "N/A"
            if total_rows > 0:
                # Try to get date range from a table that has a date column
                for table in source_tables.keys():
                    try:
                        conn = duckdb.connect(duckdb_path, read_only=True)
                        result = conn.execute(f"SELECT MIN(date), MAX(date) FROM {table}").fetchone()
                        conn.close()
                        if result and result[0]:
                            date_range = f"{result[0]} to {result[1]}"
                            break
                    except:
                        continue
            
            data_rows.append({
                'Source': f"{icon} {source_name}",
                'Tables': table_count,
                'Total Rows': f"{total_rows:,}",
                'Date Range': date_range,
                'Status': '✅ Has Data' if total_rows > 0 else '⚠️ No Data'
            })
        
        status_df = pd.DataFrame(data_rows)
        st.dataframe(status_df, use_container_width=True, hide_index=True)
        
        # Detailed table breakdown
        with st.expander("📋 Detailed Table Breakdown"):
            for table_name, row_count in sorted(table_info.items()):
                st.text(f"  {table_name}: {row_count:,} rows")
    else:
        st.warning("No data tables found in the database. Run a Lifetime ETL to populate data.")
    
    st.divider()
    
    # ========================================
    # SECTION 5: CLI Reference
    # ========================================
    with st.expander("💻 Command Line Reference"):
        st.markdown("""
        You can also run ETL from the command line:
        
        **Lifetime Pull (all data):**
        ```bash
        python scripts/run_etl_unified.py --source all --lifetime
        ```
        
        **Daily Refresh (last 3 days):**
        ```bash
        python scripts/run_etl_unified.py --source all --lookback-days 3
        ```
        
        **Specific Source:**
        ```bash
        python scripts/run_etl_unified.py --source ga4 --lookback-days 30
        python scripts/run_etl_unified.py --source gads --lifetime
        python scripts/run_etl_unified.py --source meta --start-date 2024-01-01
        ```
        
        **Test Connections:**
        ```bash
        python scripts/test_connections_unified.py --all
        python scripts/test_connections_unified.py --source gads
        ```
        """)




# ============================================
# Main Application
# ============================================
def main():
    """Main application entry point."""
    
    # Load configurations
    ga4_config, ga4_error = load_ga4_configuration()
    gsc_config, gsc_error = load_gsc_configuration()
    gads_config, gads_error = load_gads_configuration()
    meta_config, meta_error = load_meta_configuration()
    
    # Determine DuckDB path
    if ga4_config:
        duckdb_path = str(ga4_config.duckdb_path)
    elif gsc_config:
        duckdb_path = str(gsc_config.duckdb_path)
    elif gads_config:
        duckdb_path = str(gads_config.duckdb_path)
    elif meta_config:
        duckdb_path = str(meta_config.duckdb_path)
    else:
        duckdb_path = str(project_root / "data" / "warehouse.duckdb")
    
    # Sidebar
    with st.sidebar:
        st.title("🎯 rs_analytics")
        st.caption("Analytics & Marketing Dashboard")
        
        st.divider()
        
        # ========================================
        # Grouped Navigation with Expanders
        # ========================================
        
        # Initialize page state if not exists
        if 'current_page' not in st.session_state:
            st.session_state.current_page = "📈 Executive Dashboard"
        
        # Helper function to create navigation button
        def nav_button(label: str, key: str):
            """Create a navigation button that updates the current page."""
            is_active = st.session_state.current_page == label
            button_type = "primary" if is_active else "secondary"
            if st.button(label, key=key, use_container_width=True, type=button_type):
                st.session_state.current_page = label
                st.rerun()
        
        # Executive Dashboard (always visible at top)
        nav_button("📈 Executive Dashboard", "nav_exec")
        
        st.markdown("")  # Spacing
        
        # Web & App Analytics Group
        with st.expander("🌐 **Web & App**", expanded=True):
            nav_button("📱 App Analytics", "nav_app_analytics")
            nav_button("📊 GA4 Analytics", "nav_ga4")
            nav_button("🛩️ AppsFlyer", "nav_appsflyer")
        
        # Marketing Platforms Group
        with st.expander("📣 **Marketing**", expanded=True):
            nav_button("💰 Google Ads (PPC)", "nav_gads")
            nav_button("🔍 Search Console (SEO)", "nav_gsc")
            nav_button("📘 Meta Ads", "nav_meta")
            nav_button("🐦 Twitter/X", "nav_twitter")
        
        # Lifecycle Mega-Pages (Phase 3)
        with st.expander("🔄 **Lifecycle**", expanded=True):
            nav_button("🚀 Acquire", "nav_acquire")
            nav_button("⚡ Activate", "nav_activate")
            nav_button("💰 Monetize", "nav_monetize")

        # Advanced Analytics Group
        with st.expander("🧠 **Advanced Analytics**", expanded=False):
            nav_button("🔄 Behavioral Analysis", "nav_behavioral")
            nav_button("📈 Forecasting", "nav_forecasting")
            nav_button("🎯 Clustering", "nav_clustering")
        
        # Miscellaneous Group
        with st.expander("⚙️ **Tools**", expanded=False):
            nav_button("🔧 ETL Control", "nav_etl")
            nav_button("📖 Glossary", "nav_glossary")
        
        # Get current page from session state
        page = st.session_state.current_page
        
        st.divider()
        
        # ========================================
        # Quick Data Status
        # ========================================
        st.subheader("📊 Data Status")
        
        table_info = get_table_info(duckdb_path)
        ga4_rows = sum(v for k, v in table_info.items() if k.startswith('ga4_'))
        gsc_rows = sum(v for k, v in table_info.items() if k.startswith('gsc_'))
        gads_rows = sum(v for k, v in table_info.items() if k.startswith('gads_'))
        meta_rows = sum(v for k, v in table_info.items() if k.startswith('meta_'))
        af_rows = sum(v for k, v in table_info.items() if k.startswith('af_'))
        twitter_rows = sum(v for k, v in table_info.items() if k.startswith('twitter_'))
        
        # Compact status display using columns
        status_col1, status_col2 = st.columns(2)
        
        with status_col1:
            if ga4_rows > 0:
                st.caption(f"✅ GA4: {ga4_rows:,}")
            else:
                st.caption("⚠️ GA4: —")
            
            if gsc_rows > 0:
                st.caption(f"✅ GSC: {gsc_rows:,}")
            else:
                st.caption("⚠️ GSC: —")
            
            if gads_rows > 0:
                st.caption(f"✅ GAds: {gads_rows:,}")
            else:
                st.caption("⚠️ GAds: —")
        
        with status_col2:
            if meta_rows > 0:
                st.caption(f"✅ Meta: {meta_rows:,}")
            else:
                st.caption("⚠️ Meta: —")
            
            if af_rows > 0:
                st.caption(f"✅ AF: {af_rows:,}")
            else:
                st.caption("⚠️ AF: —")
            
            if twitter_rows > 0:
                st.caption(f"✅ Twitter: {twitter_rows:,}")
            else:
                st.caption("⚠️ Twitter: —")
        
        st.divider()
        
        # Refresh button
        st.caption(f"Last refresh: {datetime.now().strftime('%H:%M:%S')}")
        if st.button("🔄 Refresh Data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
    
    # Main Content
    if page == "📈 Executive Dashboard":
        render_executive_dashboard(duckdb_path)
    
    elif page == "📱 App Analytics":
        render_app_analytics(duckdb_path)
    
    elif page == "📊 GA4 Analytics":
        if ga4_config:
            render_ga4_dashboard(ga4_config, duckdb_path)
        else:
            st.error("GA4 Configuration Error")
            with st.expander("Error Details"):
                st.code(ga4_error)
    
    elif page == "🔍 Search Console (SEO)":
        render_gsc_dashboard(gsc_config, duckdb_path)
    
    elif page == "💰 Google Ads (PPC)":
        render_gads_dashboard(gads_config, duckdb_path)
    
    elif page == "📘 Meta Ads":
        render_meta_dashboard(meta_config, duckdb_path)
    
    elif page == "🛩️ AppsFlyer":
        render_appsflyer_dashboard(duckdb_path)
    
    elif page == "🐦 Twitter/X":
        twitter_config, twitter_error = load_twitter_configuration()
        render_twitter_dashboard(twitter_config, duckdb_path)
    
    elif page == "🔧 ETL Control":
        render_etl_control_panel(duckdb_path)
    
    elif page == "📖 Glossary":
        from app.components.glossary import render_glossary
        render_glossary()
    
    # ========================================
    # Lifecycle Mega-Pages (Phase 3)
    # ========================================
    elif page == "🚀 Acquire":
        render_acquire_page(duckdb_path)

    elif page == "⚡ Activate":
        render_activate_page(duckdb_path)

    elif page == "💰 Monetize":
        render_monetize_page(duckdb_path)

    # ========================================
    # Advanced Analytics Pages
    # ========================================
    elif page == "🔄 Behavioral Analysis":
        render_behavioral_analysis(duckdb_path)
    
    elif page == "📈 Forecasting":
        render_forecasting(duckdb_path)
    
    elif page == "🎯 Clustering":
        render_clustering(duckdb_path)


if __name__ == "__main__":
    main()
