"""
Google Search Console Dashboard Component

Displays SEO metrics: clicks, impressions, CTR, positions,
top queries, and top pages from GSC data.

Data source: gsc_* tables in DuckDB (populated by run_etl_gsc.py)
"""

import streamlit as st

from app.components.glossary import TERM_TOOLTIPS
from app.components.utils import load_duckdb_data, check_gsc_data_exists


def render_gsc_dashboard(gsc_config, duckdb_path: str):
    """Render the Google Search Console dashboard."""

    st.header("🔍 Google Search Console Dashboard")

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

    st.success(f"GSC data loaded: {total_rows:,} total rows across {len(gsc_tables)} tables")

    from app.components.date_picker import render_date_range_picker

    start_date, end_date, _, _ = render_date_range_picker(
        key="gsc_dashboard",
        default_days=30,
        max_days=365,
        show_comparison=False,
    )

    date_cutoff = start_date.strftime('%Y-%m-%d')

    st.divider()

    # ── Key SEO Metrics ──────────────────────────────────────
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
            totals_df = totals_df.fillna(0)
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric("Total Clicks", f"{int(totals_df['total_clicks'].iloc[0]):,}",
                         help=TERM_TOOLTIPS.get("Clicks"))
            with col2:
                st.metric("Total Impressions", f"{int(totals_df['total_impressions'].iloc[0]):,}",
                         help=TERM_TOOLTIPS.get("Impressions"))
            with col3:
                st.metric("Average CTR", f"{float(totals_df['avg_ctr'].iloc[0]):.2%}",
                         help=TERM_TOOLTIPS.get("CTR"))
            with col4:
                st.metric("Avg Position", f"{float(totals_df['avg_position'].iloc[0]):.1f}",
                         help=TERM_TOOLTIPS.get("Average Position"))

    st.divider()

    # ── Performance Over Time ────────────────────────────────
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

    # ── Top Queries and Pages ────────────────────────────────
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
                display_df['page'] = display_df['page'].apply(
                    lambda x: x.split('/')[-1] if x and len(x) > 40 else x
                )
                st.dataframe(display_df, use_container_width=True, hide_index=True)

    # ── Raw Data Explorer ────────────────────────────────────
    with st.expander("📋 Explore Raw GSC Data"):
        table_choice = st.selectbox("Select Table", options=gsc_tables, key="gsc_table_choice")
        if table_choice:
            raw_df = load_duckdb_data(duckdb_path, f"SELECT * FROM {table_choice} LIMIT 1000")
            if raw_df is not None:
                st.dataframe(raw_df, use_container_width=True)
