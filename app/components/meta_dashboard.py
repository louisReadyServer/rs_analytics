"""Meta (Facebook) Ads Dashboard - MBA-level marketing analytics with campaigns, geographic, device, and demographic insights."""

from datetime import timedelta
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from app.components.date_picker import render_date_range_picker
from app.components.glossary import TERM_TOOLTIPS
from app.components.utils import load_duckdb_data, check_meta_data_exists
from rs_analytics.utils.formatting import calculate_delta


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
        
        # Row 1: Core metrics
        col1, col2, col3, col4, col5, col6 = st.columns(6)
        
        with col1:
            spend = row['spend'] or 0
            prev_spend = prev_row['spend'] if prev_row is not None else None
            delta = calculate_delta(spend, prev_spend)
            st.metric(
                "💰 Total Spend",
                f"${spend:,.2f}",
                delta=f"{delta:+.1f}%" if delta else None,
                delta_color="inverse",
                help=TERM_TOOLTIPS.get("Spend"),
            )
        
        with col2:
            impressions = int(row['impressions'] or 0)
            delta = calculate_delta(impressions, prev_row['impressions'] if prev_row is not None else None)
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
            delta = calculate_delta(clicks, prev_row['clicks'] if prev_row is not None else None)
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
            delta = calculate_delta(installs, prev_row['app_installs'] if prev_row is not None else None)
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
