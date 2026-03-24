"""
Clustering - Keyword Intent Classification

Classifies search queries and paid keywords by user intent using a
hybrid rule-based + ML clustering approach.

Intent Categories:
- Transactional: buy, price, order, discount, cheap, deal, free trial
- Commercial Investigation: best, review, compare, vs, top, alternative
- Informational: how, what, why, when, guide, tutorial, tips, learn
- Navigational: brand terms, specific product names, login, dashboard
- Local: near me, location names, directions, hours, map

Data Sources:
- GSC gsc_queries: organic search queries with CTR, position, clicks
- Google Ads gads_keywords: paid keywords with cost, conversions, ROAS

Output:
- Intent distribution pie chart
- Intent-by-performance scatter
- Top queries per intent category
- Keyword clustering by performance similarity
"""

from typing import Optional, List, Dict, Tuple
import logging
import re

import streamlit as st
import pandas as pd
import numpy as np
import duckdb
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from app.components.glossary import TERM_TOOLTIPS

logger = logging.getLogger(__name__)


# ============================================
# Intent Classification Rules
# ============================================

# Regex patterns for rule-based intent detection
# Each pattern list is checked in order; first match wins
INTENT_PATTERNS: Dict[str, List[str]] = {
    'Transactional': [
        r'\b(buy|purchase|order|shop|cart|checkout|add to cart)\b',
        r'\b(price|pricing|cost|cheap|affordable|discount|coupon|deal|promo)\b',
        r'\b(free trial|subscribe|sign up|register|download|install)\b',
        r'\b(hire|book|reserve|appointment|schedule)\b',
        r'\b(for sale|in stock|delivery|shipping)\b',
    ],
    'Commercial Investigation': [
        r'\b(best|top|review|reviews|rating|ratings|comparison)\b',
        r'\b(vs|versus|compare|compared|alternative|alternatives)\b',
        r'\b(worth it|pros and cons|recommendation|should i)\b',
        r'\b(which|better|premium|professional|enterprise)\b',
    ],
    'Informational': [
        r'\b(how to|how do|how does|how can|how is)\b',
        r'\b(what is|what are|what does|what do)\b',
        r'\b(why|when|where|who)\b',
        r'\b(guide|tutorial|tips|learn|explain|meaning|definition)\b',
        r'\b(example|examples|template|sample|use case)\b',
    ],
    'Navigational': [
        r'\b(login|log in|sign in|signin|dashboard|account|portal)\b',
        r'\b(official|website|site|homepage|app|application)\b',
        r'\b(contact|support|help desk|customer service)\b',
    ],
    'Local': [
        r'\b(near me|nearby|closest|nearest)\b',
        r'\b(directions|location|address|hours|open now|map)\b',
        r'\b(in \w+ city|in \w+ town|local)\b',
    ],
}


def classify_intent_rules(query: str) -> str:
    """
    Classify a search query's intent using regex pattern matching.

    Checks patterns in priority order. Returns 'Informational' as default
    since most long-tail queries are informational in nature.
    """
    query_lower = query.lower().strip()

    # Check each intent category in priority order
    for intent, patterns in INTENT_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, query_lower):
                return intent

    # Default: classify by query structure heuristics
    word_count = len(query_lower.split())

    # Single-word queries are often navigational (brand searches)
    if word_count == 1:
        return 'Navigational'

    # Short queries with no clear signal default to informational
    return 'Informational'


# ============================================
# Data Loading
# ============================================

from app.components.utils import query_duckdb as _query


def _load_gsc_queries(duckdb_path: str) -> Optional[pd.DataFrame]:
    """
    Load aggregated organic search queries from Google Search Console.

    Combines across all dates to get lifetime performance per query.
    """
    sql = """
    SELECT
        query,
        SUM(clicks)       AS clicks,
        SUM(impressions)   AS impressions,
        AVG(ctr)           AS avg_ctr,
        AVG(position)      AS avg_position,
        COUNT(*)           AS days_seen
    FROM gsc_queries
    WHERE query IS NOT NULL AND query != ''
    GROUP BY query
    HAVING SUM(impressions) >= 3
    ORDER BY clicks DESC
    """
    return _query(duckdb_path, sql)


def _load_gads_keywords(duckdb_path: str) -> Optional[pd.DataFrame]:
    """
    Load aggregated paid keyword performance from Google Ads.
    """
    sql = """
    SELECT
        keyword_text                                    AS query,
        keyword_match_type                              AS match_type,
        SUM(impressions)                                AS impressions,
        SUM(clicks)                                     AS clicks,
        SUM(cost)                                       AS cost,
        SUM(conversions)                                AS conversions,
        SUM(conversions_value)                          AS revenue,
        CASE WHEN SUM(impressions) > 0
             THEN SUM(clicks) * 1.0 / SUM(impressions)
             ELSE 0 END                                 AS avg_ctr,
        CASE WHEN SUM(clicks) > 0
             THEN SUM(cost) / SUM(clicks)
             ELSE 0 END                                 AS avg_cpc,
        CASE WHEN SUM(conversions) > 0
             THEN SUM(cost) / SUM(conversions)
             ELSE NULL END                              AS cpa,
        CASE WHEN SUM(cost) > 0
             THEN SUM(conversions_value) / SUM(cost)
             ELSE 0 END                                 AS roas
    FROM gads_keywords
    WHERE keyword_text IS NOT NULL AND keyword_text != ''
    GROUP BY keyword_text, keyword_match_type
    HAVING SUM(impressions) >= 1
    ORDER BY cost DESC
    """
    return _query(duckdb_path, sql)


# ============================================
# ML Clustering (Performance-Based)
# ============================================

def _cluster_by_performance(
    df: pd.DataFrame,
    feature_cols: List[str],
    n_clusters: int = 5
) -> Tuple[pd.DataFrame, object]:
    """
    Run K-Means clustering on keyword performance features
    to discover groups of similarly-performing queries.

    Returns the dataframe with cluster labels and the model.
    """
    from sklearn.preprocessing import StandardScaler
    from sklearn.cluster import KMeans

    X = df[feature_cols].fillna(0).values

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    df = df.copy()
    df['perf_cluster'] = kmeans.fit_predict(X_scaled)

    return df, kmeans


def _name_performance_cluster(profile: pd.Series) -> str:
    """Give a performance cluster a descriptive name based on its traits."""
    names = []

    # Volume
    if profile.get('clicks', 0) > profile.get('clicks_median', 0):
        names.append("High Volume")
    else:
        names.append("Low Volume")

    # Efficiency (CTR)
    if profile.get('avg_ctr', 0) > 0.05:
        names.append("High CTR")
    elif profile.get('avg_ctr', 0) < 0.01:
        names.append("Low CTR")

    # Position (lower = better)
    if 'avg_position' in profile and profile['avg_position'] < 5:
        names.append("Top Ranked")
    elif 'avg_position' in profile and profile['avg_position'] > 20:
        names.append("Low Ranked")

    return " / ".join(names) if names else "Average"


# ============================================
# Dashboard Rendering
# ============================================

def render_clustering(duckdb_path: str):
    """
    Main render function for the Keyword Intent Classification page.

    Loads GSC and Google Ads keyword data, classifies intent using rules,
    then clusters by performance and displays interactive visualizations.
    """

    st.header("🎯 Keyword Intent Classification")
    st.markdown(
        "Classifies search queries and paid keywords by user intent, "
        "then clusters them by performance similarity."
    )

    # ------------------------------------------------------------------
    # Load data
    # ------------------------------------------------------------------
    gsc_df = _load_gsc_queries(duckdb_path)
    gads_df = _load_gads_keywords(duckdb_path)

    sources_loaded = []
    total_queries = 0

    if gsc_df is not None:
        sources_loaded.append(f"GSC Queries ({len(gsc_df):,})")
        total_queries += len(gsc_df)
    if gads_df is not None:
        sources_loaded.append(f"Google Ads Keywords ({len(gads_df):,})")
        total_queries += len(gads_df)

    if total_queries == 0:
        st.warning(
            "**No keyword data available.**\n\n"
            "Run the GSC and/or Google Ads ETL to populate data:\n"
            "```bash\n"
            "python scripts/run_etl_unified.py --source gsc --lifetime\n"
            "python scripts/run_etl_unified.py --source gads --lifetime\n"
            "```"
        )
        return

    st.success(f"Data loaded: {' · '.join(sources_loaded)} ({total_queries:,} total queries)")

    st.divider()

    # ==================================================================
    # Section 1 – GSC Intent Classification
    # ==================================================================
    if gsc_df is not None:
        st.subheader("🔍 Organic Query Intent (GSC)")
        st.caption(f"Classified {len(gsc_df):,} organic search queries by user intent.")

        # Classify each query
        gsc_df['intent'] = gsc_df['query'].apply(classify_intent_rules)

        # Word count for additional context
        gsc_df['word_count'] = gsc_df['query'].apply(lambda x: len(str(x).split()))

        # ------------------------------------------------------------------
        # Intent Distribution
        # ------------------------------------------------------------------
        intent_summary = (
            gsc_df
            .groupby('intent')
            .agg(
                query_count=('query', 'count'),
                total_clicks=('clicks', 'sum'),
                total_impressions=('impressions', 'sum'),
                avg_ctr=('avg_ctr', 'mean'),
                avg_position=('avg_position', 'mean'),
            )
            .reset_index()
            .sort_values('query_count', ascending=False)
        )

        # KPI row
        kpi_cols = st.columns(5)
        intent_colors = {
            'Transactional': '#e74c3c',
            'Commercial Investigation': '#f39c12',
            'Informational': '#3498db',
            'Navigational': '#2ecc71',
            'Local': '#9b59b6',
        }

        for idx, row in intent_summary.iterrows():
            col = kpi_cols[idx % 5]
            with col:
                pct = row['query_count'] / len(gsc_df) * 100
                st.metric(
                    row['intent'],
                    f"{row['query_count']:,}",
                    delta=f"{pct:.1f}%",
                    help=f"Avg Position: {row['avg_position']:.1f} | Avg CTR: {row['avg_ctr']:.2%}"
                )

        # Charts side by side
        chart_col1, chart_col2 = st.columns(2)

        with chart_col1:
            # Pie chart of intent distribution
            fig_pie = px.pie(
                intent_summary,
                values='query_count', names='intent',
                title='Query Distribution by Intent',
                hole=0.4,
                color='intent',
                color_discrete_map=intent_colors,
            )
            fig_pie.update_layout(height=350, margin=dict(t=50, b=10))
            st.plotly_chart(fig_pie, use_container_width=True)

        with chart_col2:
            # Clicks distribution by intent
            fig_clicks = px.bar(
                intent_summary,
                x='intent', y='total_clicks',
                color='intent',
                color_discrete_map=intent_colors,
                title='Total Clicks by Intent',
                text='total_clicks'
            )
            fig_clicks.update_layout(height=350, showlegend=False)
            fig_clicks.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
            st.plotly_chart(fig_clicks, use_container_width=True)

        # ------------------------------------------------------------------
        # Intent vs Performance Scatter
        # ------------------------------------------------------------------
        st.markdown("##### Intent vs Performance")

        fig_scatter = px.scatter(
            gsc_df.head(500),  # Top 500 by clicks for readability
            x='avg_position', y='avg_ctr',
            size='clicks',
            color='intent',
            color_discrete_map=intent_colors,
            hover_name='query',
            hover_data={
                'clicks': ':,.0f',
                'impressions': ':,.0f',
                'avg_ctr': ':.2%',
                'avg_position': ':.1f'
            },
            title='Query Intent Map: Position vs CTR (size = clicks)',
            labels={'avg_position': 'Avg Position (lower = better)', 'avg_ctr': 'Avg CTR'},
            size_max=30
        )
        # Invert x-axis so position 1 is on the right
        fig_scatter.update_xaxes(autorange='reversed')
        fig_scatter.update_layout(height=450, legend=dict(orientation='h', y=-0.15))
        st.plotly_chart(fig_scatter, use_container_width=True)

        st.divider()

        # ------------------------------------------------------------------
        # Top Queries per Intent
        # ------------------------------------------------------------------
        st.subheader("📋 Top Queries by Intent")

        intent_tabs = st.tabs(list(intent_colors.keys()))

        for tab, intent_name in zip(intent_tabs, intent_colors.keys()):
            with tab:
                intent_queries = gsc_df[gsc_df['intent'] == intent_name].nlargest(15, 'clicks')

                if intent_queries.empty:
                    st.info(f"No {intent_name} queries found.")
                    continue

                display_df = intent_queries[['query', 'clicks', 'impressions', 'avg_ctr', 'avg_position']].copy()
                display_df['avg_ctr'] = display_df['avg_ctr'].apply(lambda x: f"{x:.2%}")
                display_df['avg_position'] = display_df['avg_position'].apply(lambda x: f"{x:.1f}")
                display_df['clicks'] = display_df['clicks'].apply(lambda x: f"{int(x):,}")
                display_df['impressions'] = display_df['impressions'].apply(lambda x: f"{int(x):,}")

                display_df = display_df.rename(columns={
                    'query': 'Query',
                    'clicks': 'Clicks',
                    'impressions': 'Impr.',
                    'avg_ctr': 'CTR',
                    'avg_position': 'Pos.'
                })
                st.dataframe(display_df, use_container_width=True, hide_index=True)

    # ==================================================================
    # Section 2 – Google Ads Keyword Intent + Performance Clustering
    # ==================================================================
    if gads_df is not None:
        st.divider()
        st.subheader("💰 Paid Keyword Analysis (Google Ads)")
        st.caption(f"Classified {len(gads_df):,} paid keywords by intent and clustered by performance.")

        # Classify intent
        gads_df['intent'] = gads_df['query'].apply(classify_intent_rules)
        gads_df['word_count'] = gads_df['query'].apply(lambda x: len(str(x).split()))

        # Intent summary for paid keywords
        paid_intent = (
            gads_df
            .groupby('intent')
            .agg(
                keyword_count=('query', 'count'),
                total_cost=('cost', 'sum'),
                total_clicks=('clicks', 'sum'),
                total_conversions=('conversions', 'sum'),
                avg_cpc=('avg_cpc', 'mean'),
                avg_roas=('roas', 'mean'),
            )
            .reset_index()
            .sort_values('total_cost', ascending=False)
        )

        # KPI row
        paid_kpi = st.columns(5)
        for idx, row in paid_intent.iterrows():
            col = paid_kpi[idx % 5]
            with col:
                cpa = row['total_cost'] / row['total_conversions'] if row['total_conversions'] > 0 else 0
                st.metric(
                    row['intent'],
                    f"${row['total_cost']:,.0f}",
                    delta=f"{row['keyword_count']} keywords",
                    help=f"CPA: ${cpa:.2f} | ROAS: {row['avg_roas']:.2f}x"
                )

        # Intent spend breakdown
        chart_col1, chart_col2 = st.columns(2)

        with chart_col1:
            fig_spend = px.pie(
                paid_intent,
                values='total_cost', names='intent',
                title='Ad Spend by Intent',
                hole=0.4,
                color='intent',
                color_discrete_map=intent_colors,
            )
            fig_spend.update_layout(height=350, margin=dict(t=50, b=10))
            st.plotly_chart(fig_spend, use_container_width=True)

        with chart_col2:
            # Efficiency by intent
            paid_intent['cpa'] = paid_intent.apply(
                lambda r: r['total_cost'] / r['total_conversions'] if r['total_conversions'] > 0 else 0,
                axis=1
            )
            fig_eff = px.bar(
                paid_intent,
                x='intent', y='cpa',
                color='intent',
                color_discrete_map=intent_colors,
                title='Cost Per Acquisition by Intent',
                text='cpa'
            )
            fig_eff.update_layout(height=350, showlegend=False)
            fig_eff.update_traces(texttemplate='$%{text:.2f}', textposition='outside')
            fig_eff.update_yaxes(tickformat='$,.0f')
            st.plotly_chart(fig_eff, use_container_width=True)

        # ------------------------------------------------------------------
        # Performance-based Clustering
        # ------------------------------------------------------------------
        if len(gads_df) >= 10:
            st.divider()
            st.subheader("🔬 Performance-Based Keyword Clusters")
            st.caption("K-Means clustering groups keywords with similar cost, CTR, and conversion patterns.")

            perf_features = ['clicks', 'cost', 'avg_ctr', 'avg_cpc', 'conversions']
            available_features = [f for f in perf_features if f in gads_df.columns]

            if len(available_features) >= 3:
                n_perf_clusters = st.slider(
                    "Number of Performance Clusters",
                    min_value=2,
                    max_value=min(8, len(gads_df) - 1),
                    value=min(4, len(gads_df) - 1),
                    key="cl_perf_k",
                    help="Number of groups to segment campaigns by performance metrics (CTR, CPA, ROAS)"
                )

                gads_clustered, kmeans_model = _cluster_by_performance(
                    gads_df, available_features, n_clusters=n_perf_clusters
                )

                # Name clusters
                clicks_median = gads_clustered['clicks'].median()
                cluster_names = {}
                for c in range(n_perf_clusters):
                    mask = gads_clustered['perf_cluster'] == c
                    profile = gads_clustered.loc[mask, available_features].mean()
                    profile['clicks_median'] = clicks_median
                    cluster_names[c] = _name_performance_cluster(profile)

                gads_clustered['cluster_name'] = gads_clustered['perf_cluster'].map(cluster_names)

                # Cluster summary table
                cluster_summary_rows = []
                for c in range(n_perf_clusters):
                    mask = gads_clustered['perf_cluster'] == c
                    subset = gads_clustered.loc[mask]
                    cluster_summary_rows.append({
                        'Cluster': cluster_names[c],
                        'Keywords': len(subset),
                        'Avg Cost': f"${subset['cost'].mean():,.2f}",
                        'Avg Clicks': f"{subset['clicks'].mean():,.0f}",
                        'Avg CTR': f"{subset['avg_ctr'].mean():.2%}",
                        'Avg CPC': f"${subset['avg_cpc'].mean():.2f}",
                        'Total Conv.': f"{subset['conversions'].sum():,.1f}",
                    })

                st.dataframe(
                    pd.DataFrame(cluster_summary_rows),
                    use_container_width=True,
                    hide_index=True
                )

                # Scatter plot of clusters
                fig_clusters = px.scatter(
                    gads_clustered,
                    x='cost', y='clicks',
                    color='cluster_name',
                    hover_name='query',
                    hover_data={
                        'cost': ':$,.2f',
                        'clicks': ':,.0f',
                        'conversions': ':,.1f',
                        'avg_ctr': ':.2%'
                    },
                    title='Keyword Performance Clusters',
                    labels={'cost': 'Cost ($)', 'clicks': 'Clicks'},
                    color_discrete_sequence=px.colors.qualitative.Set2
                )
                fig_clusters.update_layout(height=400, legend=dict(orientation='h', y=-0.15))
                st.plotly_chart(fig_clusters, use_container_width=True)

    # ==================================================================
    # Methodology
    # ==================================================================
    st.divider()
    with st.expander("🔬 Methodology & Interpretation Guide"):
        st.markdown("""
        **Intent Classification: Hybrid Rule-Based Approach**

        Queries are classified using regex pattern matching against curated
        keyword lists for each intent category. The priority order is:

        1. **Transactional** — buy, price, order, discount, free trial
        2. **Commercial Investigation** — best, review, compare, vs, alternative
        3. **Informational** — how to, what is, guide, tutorial, tips
        4. **Navigational** — login, dashboard, official site, contact
        5. **Local** — near me, directions, hours, location

        Queries with no pattern match default to:
        - *Navigational* if single word (likely brand)
        - *Informational* otherwise (most long-tail queries)

        ---

        **Performance Clustering: K-Means**

        Paid keywords are also grouped by performance similarity using K-Means
        on standardized features (clicks, cost, CTR, CPC, conversions).
        This reveals keyword groups that behave similarly regardless of intent.

        **How to Use These Insights:**
        - **Transactional queries** → Prioritize for paid ads and conversion optimization
        - **Informational queries** → Create content / blog strategy around these
        - **High Volume / Low CTR clusters** → Improve ad copy or landing pages
        - **High CTR / Top Ranked** → Protect these positions, they're your winners
        - **Commercial Investigation** → Create comparison and review content
        """)
