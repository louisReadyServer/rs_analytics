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
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple

import streamlit as st
import pandas as pd
import duckdb

# Bootstrap: add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from etl.utils import get_project_root
project_root = get_project_root()

from rs_analytics.utils.formatting import calculate_delta

# Import dashboard components
from app.components.executive_dashboard import render_executive_dashboard
from app.components.app_analytics import render_app_analytics
from app.components.appsflyer_dashboard import render_appsflyer_dashboard
from app.components.glossary import TERM_TOOLTIPS
from app.components.utils import (
    load_duckdb_data,
    get_table_info,
    check_gsc_data_exists,
    check_gads_data_exists,
    check_meta_data_exists,
    check_twitter_data_exists,
)
from app.components.gsc_dashboard import render_gsc_dashboard
from app.components.gads_dashboard import render_gads_dashboard
from app.components.meta_dashboard import render_meta_dashboard
from app.components.twitter_dashboard import render_twitter_dashboard

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


# NOTE: render_gsc_dashboard, render_gads_dashboard, render_meta_dashboard,
# and render_twitter_dashboard have been extracted to their own component
# files in app/components/. They are imported at the top of this file.


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
