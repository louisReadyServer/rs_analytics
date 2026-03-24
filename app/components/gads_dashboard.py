"""Google Ads Dashboard Component - PPC analytics with campaigns, keywords, devices, geographic, and hourly data."""

from datetime import timedelta
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from app.components.date_picker import render_date_range_picker
from app.components.glossary import TERM_TOOLTIPS
from app.components.utils import load_duckdb_data, check_gads_data_exists
from rs_analytics.utils.formatting import calculate_delta


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
            # Helper: safely extract a numeric value, returning 0 for NaN/None
            def _safe_val(df, col):
                val = df[col].iloc[0]
                if pd.isna(val):
                    return 0
                return val

            # Extract current values
            impressions = int(_safe_val(summary_df, 'total_impressions'))
            clicks = int(_safe_val(summary_df, 'total_clicks'))
            cost = float(_safe_val(summary_df, 'total_cost'))
            ctr = float(_safe_val(summary_df, 'avg_ctr'))
            conversions = float(_safe_val(summary_df, 'total_conversions'))
            conv_value = float(_safe_val(summary_df, 'total_conversion_value'))
            avg_cpc = float(_safe_val(summary_df, 'avg_cpc'))
            avg_cpm = float(_safe_val(summary_df, 'avg_cpm'))
            avg_cpa = float(_safe_val(summary_df, 'avg_cpa'))
            roas = float(_safe_val(summary_df, 'roas'))
            
            # Calculate deltas if previous period data exists
            prev_impressions = int(_safe_val(prev_df, 'total_impressions')) if prev_df is not None and not prev_df.empty else 0
            prev_clicks = int(_safe_val(prev_df, 'total_clicks')) if prev_df is not None and not prev_df.empty else 0
            prev_cost = float(_safe_val(prev_df, 'total_cost')) if prev_df is not None and not prev_df.empty else 0
            prev_ctr = float(_safe_val(prev_df, 'avg_ctr')) if prev_df is not None and not prev_df.empty else 0
            prev_conversions = float(_safe_val(prev_df, 'total_conversions')) if prev_df is not None and not prev_df.empty else 0
            prev_conv_value = float(_safe_val(prev_df, 'total_conversion_value')) if prev_df is not None and not prev_df.empty else 0
            
            delta_impressions = calculate_delta(impressions, prev_impressions)
            delta_clicks = calculate_delta(clicks, prev_clicks)
            delta_cost = calculate_delta(cost, prev_cost)
            delta_ctr = calculate_delta(ctr, prev_ctr)
            delta_conversions = calculate_delta(conversions, prev_conversions)
            delta_conv_value = calculate_delta(conv_value, prev_conv_value)
            
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
