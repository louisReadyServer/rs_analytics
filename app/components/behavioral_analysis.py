"""
Behavioral Analysis - Customer Segmentation & Persona Inference

Uses K-Means clustering on GA4, Meta, and AppsFlyer data to discover
distinct user segments and infer behavioral personas.

Feature Sources:
- GA4 traffic_overview: acquisition channel, engagement
- GA4 geographic_data: location patterns
- GA4 technology_data: device / OS / browser preferences
- Meta demographics: age, gender response rates
- AppsFlyer af_daily_geo: app engagement, loyalty, revenue

Output:
- Cluster scatter plot (PCA-reduced)
- Persona cards with top traits
- Segment comparison table
- Feature importance radar charts
"""

from typing import Optional, List, Dict, Tuple
import logging

import streamlit as st
import pandas as pd
import numpy as np
import duckdb
import plotly.express as px
import plotly.graph_objects as go

from app.components.glossary import TERM_TOOLTIPS

logger = logging.getLogger(__name__)


# ============================================
# Data Loading
# ============================================

def _query(duckdb_path: str, sql: str) -> Optional[pd.DataFrame]:
    """Execute a read-only DuckDB query and return a DataFrame."""
    try:
        conn = duckdb.connect(duckdb_path, read_only=True)
        df = conn.execute(sql).fetchdf()
        conn.close()
        return df if not df.empty else None
    except Exception as e:
        logger.warning(f"Query failed: {e}")
        return None


def _load_traffic_features(duckdb_path: str) -> Optional[pd.DataFrame]:
    """
    Load acquisition and engagement features from GA4 traffic overview.

    Groups by source/medium to build a profile per acquisition channel.
    """
    sql = """
    SELECT
        sessionSource                             AS source,
        sessionMedium                             AS medium,
        COUNT(*)                                  AS days_active,
        SUM(CAST(sessions      AS DOUBLE))        AS total_sessions,
        SUM(CAST(totalUsers    AS DOUBLE))        AS total_users,
        SUM(CAST(newUsers      AS DOUBLE))        AS new_users,
        AVG(CAST(bounceRate    AS DOUBLE))        AS avg_bounce_rate,
        SUM(CAST(screenPageViews AS DOUBLE))      AS total_pageviews,
        -- derived
        CASE WHEN SUM(CAST(totalUsers AS DOUBLE)) > 0
             THEN SUM(CAST(newUsers AS DOUBLE)) / SUM(CAST(totalUsers AS DOUBLE))
             ELSE 0 END                           AS new_user_ratio,
        CASE WHEN SUM(CAST(sessions AS DOUBLE)) > 0
             THEN SUM(CAST(screenPageViews AS DOUBLE)) / SUM(CAST(sessions AS DOUBLE))
             ELSE 0 END                           AS pages_per_session
    FROM ga4_traffic_overview
    WHERE sessionSource IS NOT NULL
    GROUP BY sessionSource, sessionMedium
    HAVING SUM(CAST(sessions AS DOUBLE)) >= 5
    ORDER BY total_sessions DESC
    """
    return _query(duckdb_path, sql)


def _load_geo_features(duckdb_path: str) -> Optional[pd.DataFrame]:
    """
    Load geographic engagement features from GA4.

    Groups by country for regional behavior profiles.
    """
    sql = """
    SELECT
        country,
        COUNT(*)                              AS days_active,
        SUM(CAST(sessions   AS DOUBLE))       AS total_sessions,
        SUM(CAST(totalUsers AS DOUBLE))       AS total_users,
        SUM(CAST(newUsers   AS DOUBLE))       AS new_users,
        CASE WHEN SUM(CAST(totalUsers AS DOUBLE)) > 0
             THEN SUM(CAST(newUsers AS DOUBLE)) / SUM(CAST(totalUsers AS DOUBLE))
             ELSE 0 END                       AS new_user_ratio
    FROM ga4_geographic_data
    WHERE country IS NOT NULL
      AND country NOT IN ('(not set)', '')
    GROUP BY country
    HAVING SUM(CAST(sessions AS DOUBLE)) >= 3
    ORDER BY total_sessions DESC
    """
    return _query(duckdb_path, sql)


def _load_tech_features(duckdb_path: str) -> Optional[pd.DataFrame]:
    """
    Load technology preferences from GA4.

    Groups by device/OS/browser for tech preference profiling.
    """
    sql = """
    SELECT
        deviceCategory,
        operatingSystem,
        browser,
        SUM(CAST(sessions        AS DOUBLE)) AS total_sessions,
        SUM(CAST(totalUsers      AS DOUBLE)) AS total_users,
        SUM(CAST(screenPageViews AS DOUBLE)) AS total_pageviews,
        CASE WHEN SUM(CAST(sessions AS DOUBLE)) > 0
             THEN SUM(CAST(screenPageViews AS DOUBLE)) / SUM(CAST(sessions AS DOUBLE))
             ELSE 0 END                      AS pages_per_session
    FROM ga4_technology_data
    WHERE deviceCategory IS NOT NULL
    GROUP BY deviceCategory, operatingSystem, browser
    HAVING SUM(CAST(sessions AS DOUBLE)) >= 3
    """
    return _query(duckdb_path, sql)


def _load_appsflyer_features(duckdb_path: str) -> Optional[pd.DataFrame]:
    """
    Load app engagement features from AppsFlyer by country/source.
    """
    sql = """
    SELECT
        country,
        media_source,
        platform,
        SUM(installs)                AS total_installs,
        SUM(sessions)                AS total_sessions,
        SUM(loyal_users)             AS total_loyal_users,
        AVG(loyal_users_per_install) AS avg_loyalty_rate,
        SUM(total_revenue)           AS total_revenue,
        AVG(arpu)                    AS avg_arpu,
        AVG(conversion_rate)         AS avg_conversion_rate,
        SUM(clicks)                  AS total_clicks,
        SUM(impressions)             AS total_impressions
    FROM af_daily_geo
    WHERE media_source IS NOT NULL
    GROUP BY country, media_source, platform
    HAVING SUM(installs) >= 1
    """
    return _query(duckdb_path, sql)


# ============================================
# Feature Engineering & Clustering
# ============================================

def _build_channel_features(traffic_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, List[str]]:
    """
    Build a numeric feature matrix from traffic channel data.

    Returns:
        (feature_matrix, labels_df, feature_names)
    """
    # Select numeric columns for clustering
    feature_cols = [
        'total_sessions', 'total_users', 'new_users',
        'avg_bounce_rate', 'total_pageviews',
        'new_user_ratio', 'pages_per_session', 'days_active'
    ]

    # Keep label columns for display
    labels_df = traffic_df[['source', 'medium']].copy()
    labels_df['label'] = labels_df['source'] + ' / ' + labels_df['medium']

    # Build feature matrix and fill missing values with 0
    features = traffic_df[feature_cols].fillna(0).copy()

    return features, labels_df, feature_cols


def _run_clustering(
    features: pd.DataFrame,
    n_clusters: int = 4,
    random_state: int = 42
) -> Tuple[np.ndarray, np.ndarray, object, object]:
    """
    Run K-Means clustering with StandardScaler preprocessing.

    Returns:
        (cluster_labels, pca_2d_coords, scaler, kmeans_model)
    """
    from sklearn.preprocessing import StandardScaler
    from sklearn.cluster import KMeans
    from sklearn.decomposition import PCA

    # Scale features so no single metric dominates
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(features.values)

    # Cluster
    kmeans = KMeans(n_clusters=n_clusters, random_state=random_state, n_init=10)
    labels = kmeans.fit_predict(X_scaled)

    # Reduce to 2D for scatter plot
    pca = PCA(n_components=2, random_state=random_state)
    coords = pca.fit_transform(X_scaled)

    return labels, coords, scaler, kmeans


def _find_optimal_k(features: pd.DataFrame, max_k: int = 8) -> int:
    """
    Use the Elbow method with silhouette score to pick the best k.

    Tests k from 2 to max_k and returns the k with the highest
    silhouette score.
    """
    from sklearn.preprocessing import StandardScaler
    from sklearn.cluster import KMeans
    from sklearn.metrics import silhouette_score

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(features.values)

    best_k = 3  # sensible default
    best_score = -1

    for k in range(2, min(max_k + 1, len(features))):
        km = KMeans(n_clusters=k, random_state=42, n_init=10)
        labels = km.fit_predict(X_scaled)
        score = silhouette_score(X_scaled, labels)
        if score > best_score:
            best_score = score
            best_k = k

    return best_k


# ============================================
# Persona Definitions & Glossary
# ============================================

# Engagement-level persona definitions
ENGAGEMENT_PERSONAS = {
    "Deep Engagers": {
        "description": "Users who view 3+ pages per session on average",
        "behavior": "These visitors explore your content thoroughly, often reading multiple articles or viewing multiple products before leaving.",
        "action": "High-value audience — prioritize these acquisition channels and create more content to keep them engaged.",
        "threshold": "Pages/Session > 3"
    },
    "Quick Visitors": {
        "description": "Users with bounce rates above 70%",
        "behavior": "These visitors land on a page and leave without further interaction. They may not find what they're looking for, or the landing page doesn't match their intent.",
        "action": "Improve landing page relevance, page load speed, and calls-to-action for these channels.",
        "threshold": "Bounce Rate > 70%"
    },
    "Moderate Browsers": {
        "description": "Users with typical browsing patterns (1-3 pages, moderate bounce)",
        "behavior": "Average engagement — these visitors look around but don't dive deep. They represent your 'typical' user behavior.",
        "action": "Test content improvements and conversion optimization to move them toward Deep Engagers.",
        "threshold": "1-3 Pages/Session, Bounce Rate 30-70%"
    }
}

# Audience-type persona definitions
AUDIENCE_PERSONAS = {
    "New Audience": {
        "description": "Channels where 70%+ of users are first-time visitors",
        "behavior": "These channels are primarily driving discovery and top-of-funnel awareness. Users are unfamiliar with your brand.",
        "action": "Focus on first impressions, onboarding flows, and converting new users to repeat visitors.",
        "threshold": "New User Ratio > 70%"
    },
    "Returning Core": {
        "description": "Channels where 70%+ of users are returning visitors",
        "behavior": "These channels bring back your loyal audience. Users already know your brand and are coming back intentionally.",
        "action": "Nurture these users with loyalty programs, personalized content, and upsell opportunities.",
        "threshold": "New User Ratio < 30%"
    },
    "Mixed Audience": {
        "description": "Channels with a balanced mix of new and returning users",
        "behavior": "Healthy acquisition channel that both attracts new users and retains existing ones.",
        "action": "Maintain current strategy; test segmented messaging for new vs. returning visitors.",
        "threshold": "New User Ratio 30-70%"
    }
}

# Feature definitions for the metrics used in analysis
FEATURE_DEFINITIONS = {
    "total_sessions": {
        "name": "Total Sessions",
        "definition": "The total number of user sessions initiated from this channel during the analysis period.",
        "interpretation": "Higher = more traffic volume from this source."
    },
    "total_users": {
        "name": "Total Users", 
        "definition": "The count of unique users who visited from this channel.",
        "interpretation": "Represents audience reach — how many distinct people this channel brings."
    },
    "new_users": {
        "name": "New Users",
        "definition": "Users visiting your site/app for the first time from this channel.",
        "interpretation": "Measures acquisition effectiveness — how well the channel attracts new audiences."
    },
    "avg_bounce_rate": {
        "name": "Bounce Rate",
        "definition": "Percentage of sessions where users left after viewing only one page (no further interaction).",
        "interpretation": "Lower is generally better. High bounce may indicate poor landing page relevance or UX issues."
    },
    "total_pageviews": {
        "name": "Total Pageviews",
        "definition": "Sum of all pages viewed by users from this channel.",
        "interpretation": "Indicates overall content consumption volume."
    },
    "new_user_ratio": {
        "name": "New User %",
        "definition": "Percentage of users from this channel who are first-time visitors (New Users / Total Users).",
        "interpretation": "High = acquisition-focused channel; Low = retention/loyalty channel."
    },
    "pages_per_session": {
        "name": "Pages / Session",
        "definition": "Average number of pages viewed per session from this channel.",
        "interpretation": "Higher = deeper engagement. Users are exploring more content."
    },
    "days_active": {
        "name": "Days Active",
        "definition": "Number of distinct days this channel generated traffic during the analysis period.",
        "interpretation": "Higher = more consistent traffic source; Lower = sporadic or campaign-driven."
    }
}


def _name_persona(cluster_profile: pd.Series) -> Tuple[str, str, str]:
    """
    Generate a descriptive persona name from cluster centroid values.

    Uses relative feature strengths to assign a human-readable label.
    
    Returns:
        Tuple of (persona_name, engagement_type, audience_type)
    """
    # Engagement level classification
    if cluster_profile.get('pages_per_session', 0) > 3:
        engagement_type = "Deep Engagers"
    elif cluster_profile.get('avg_bounce_rate', 0) > 0.7:
        engagement_type = "Quick Visitors"
    else:
        engagement_type = "Moderate Browsers"

    # Audience type classification
    if cluster_profile.get('new_user_ratio', 0) > 0.7:
        audience_type = "New Audience"
    elif cluster_profile.get('new_user_ratio', 0) < 0.3:
        audience_type = "Returning Core"
    else:
        audience_type = "Mixed Audience"

    persona_name = f"{engagement_type} / {audience_type}"
    return persona_name, engagement_type, audience_type


# ============================================
# Dashboard Rendering
# ============================================

def _render_glossary_section():
    """
    Render an expandable glossary section explaining all metrics and persona types.
    """
    with st.expander("📖 **Glossary: Understanding Personas & Metrics**", expanded=False):
        st.markdown("### Engagement Personas")
        st.markdown("*These describe how deeply users interact with your content:*")
        
        for persona_name, info in ENGAGEMENT_PERSONAS.items():
            st.markdown(f"**{persona_name}**")
            st.markdown(f"- *Definition:* {info['description']}")
            st.markdown(f"- *Behavior:* {info['behavior']}")
            st.markdown(f"- *Recommended Action:* {info['action']}")
            st.markdown(f"- *Threshold:* `{info['threshold']}`")
            st.markdown("")
        
        st.divider()
        
        st.markdown("### Audience Personas")
        st.markdown("*These describe the composition of new vs. returning users:*")
        
        for persona_name, info in AUDIENCE_PERSONAS.items():
            st.markdown(f"**{persona_name}**")
            st.markdown(f"- *Definition:* {info['description']}")
            st.markdown(f"- *Behavior:* {info['behavior']}")
            st.markdown(f"- *Recommended Action:* {info['action']}")
            st.markdown(f"- *Threshold:* `{info['threshold']}`")
            st.markdown("")
        
        st.divider()
        
        st.markdown("### Metric Definitions")
        st.markdown("*These are the features used in the clustering analysis:*")
        
        for metric_key, info in FEATURE_DEFINITIONS.items():
            st.markdown(f"**{info['name']}**")
            st.markdown(f"- *Definition:* {info['definition']}")
            st.markdown(f"- *Interpretation:* {info['interpretation']}")
            st.markdown("")


def render_behavioral_analysis(duckdb_path: str):
    """
    Main render function for the Behavioral Analysis page.

    Loads multi-source data, builds features, runs K-Means clustering,
    and displays interactive segment visualizations with persona cards.
    """

    st.header("🔄 Behavioral Analysis")
    st.markdown("Discover distinct user segments and infer behavioral personas from your analytics data.")
    
    # Page-level explainer
    st.info(
        "**What is this page?** This analysis uses machine learning (K-Means clustering) to automatically "
        "group your traffic acquisition channels into distinct behavioral segments. Each segment represents "
        "a 'persona' — a pattern of user behavior that can inform your marketing and product strategy."
    )

    # ------------------------------------------------------------------
    # Load data
    # ------------------------------------------------------------------
    traffic_df = _load_traffic_features(duckdb_path)
    geo_df = _load_geo_features(duckdb_path)
    tech_df = _load_tech_features(duckdb_path)
    af_df = _load_appsflyer_features(duckdb_path)

    # Check that we have at least traffic data (core requirement)
    if traffic_df is None or len(traffic_df) < 4:
        st.warning(
            "**Insufficient data for behavioral analysis.**\n\n"
            "This module needs at least 4 distinct traffic source/medium "
            "combinations with 5+ sessions each. Run the GA4 ETL to populate data:\n"
            "```bash\npython scripts/run_etl_unified.py --source ga4 --lifetime\n```"
        )
        return

    # Show data sources loaded with explicit platform identification
    sources_loaded = []
    source_details = []
    
    if traffic_df is not None:
        sources_loaded.append(f"GA4 Traffic ({len(traffic_df)} channels)")
        source_details.append("🌐 **Web Analytics (GA4)**: Traffic acquisition channels by source/medium")
    if geo_df is not None:
        sources_loaded.append(f"GA4 Geo ({len(geo_df)} countries)")
        source_details.append("🌍 **Geographic Data (GA4)**: User sessions and behavior by country")
    if tech_df is not None:
        sources_loaded.append(f"GA4 Tech ({len(tech_df)} combos)")
        source_details.append("💻 **Technology Data (GA4)**: Device, OS, and browser preferences")
    if af_df is not None:
        sources_loaded.append(f"AppsFlyer ({len(af_df)} segments)")
        source_details.append("📱 **Mobile App Analytics (AppsFlyer)**: App installs, loyalty, and revenue by geo/source")

    st.success(f"Data loaded: {' · '.join(sources_loaded)}")
    
    # Expandable section showing exactly what data sources are being used
    with st.expander("ℹ️ **Data Sources Being Analyzed**", expanded=False):
        st.markdown("This analysis combines data from the following platforms:")
        for detail in source_details:
            st.markdown(f"- {detail}")
        st.markdown("")
        st.markdown(
            "*Note: The primary segmentation is based on **GA4 Web Traffic** data, specifically "
            "how different acquisition channels (e.g., google/organic, facebook/cpc, direct/none) "
            "drive different user behaviors on your website.*"
        )
    
    # Render the glossary section
    _render_glossary_section()

    st.divider()

    # ==================================================================
    # Section 1 – Channel-Based Segmentation (main clustering)
    # ==================================================================
    st.subheader("📊 Channel Segmentation")
    st.caption("Clusters acquisition channels by engagement behavior to discover distinct audience segments.")
    
    st.markdown(
        """
        <div style="background-color: #1e3a5f; color: #e8f4fd; padding: 12px; border-radius: 8px; margin-bottom: 16px; border-left: 4px solid #4dabf7;">
        <strong style="color: #ffffff;">📌 Analysis Context:</strong> This segmentation analyzes your <strong style="color: #ffffff;">website traffic</strong> 
        from <strong style="color: #ffffff;">Google Analytics 4 (GA4)</strong>. Each data point represents a unique acquisition channel 
        (source/medium combination like "google/organic" or "facebook/cpc"). The algorithm groups channels 
        that drive similar user behavior patterns.
        </div>
        """,
        unsafe_allow_html=True
    )

    # Build features from traffic data
    features, labels_df, feature_cols = _build_channel_features(traffic_df)

    # Let user choose number of clusters or auto-detect
    cluster_col1, cluster_col2 = st.columns([1, 3])

    with cluster_col1:
        auto_k = st.checkbox("Auto-detect clusters", value=True, key="ba_auto_k")
        if auto_k:
            n_clusters = _find_optimal_k(features)
            st.metric("Optimal K", n_clusters)
        else:
            n_clusters = st.slider(
                "Number of Segments",
                min_value=2,
                max_value=min(8, len(features) - 1),
                value=4,
                key="ba_n_clusters"
            )

    # Run clustering
    cluster_labels, pca_coords, scaler, kmeans = _run_clustering(features, n_clusters)

    # Combine results
    results_df = labels_df.copy()
    results_df['cluster'] = cluster_labels
    results_df['pca_x'] = pca_coords[:, 0]
    results_df['pca_y'] = pca_coords[:, 1]

    # Merge back numeric features for profiling
    for col in feature_cols:
        results_df[col] = features[col].values

    # Name each cluster by its centroid profile
    # Store both name and component types for detailed explanations
    cluster_names = {}
    cluster_engagement_types = {}
    cluster_audience_types = {}
    
    for c in range(n_clusters):
        mask = results_df['cluster'] == c
        profile = results_df.loc[mask, feature_cols].mean()
        name, engagement_type, audience_type = _name_persona(profile)
        cluster_names[c] = name
        cluster_engagement_types[c] = engagement_type
        cluster_audience_types[c] = audience_type

    results_df['segment'] = results_df['cluster'].map(cluster_names)

    # ------------------------------------------------------------------
    # Scatter plot (PCA-reduced)
    # ------------------------------------------------------------------
    with cluster_col2:
        fig_scatter = px.scatter(
            results_df,
            x='pca_x', y='pca_y',
            color='segment',
            hover_name='label',
            hover_data={
                'total_sessions': ':,.0f',
                'pages_per_session': ':.1f',
                'avg_bounce_rate': ':.2%',
                'new_user_ratio': ':.1%',
                'pca_x': False, 'pca_y': False, 'cluster': False
            },
            title='Audience Segments (PCA Projection)',
            color_discrete_sequence=px.colors.qualitative.Set2,
            labels={'pca_x': 'Component 1', 'pca_y': 'Component 2'}
        )
        fig_scatter.update_traces(marker=dict(size=10, line=dict(width=1, color='white')))
        fig_scatter.update_layout(height=400, legend=dict(orientation='h', y=-0.15))
        st.plotly_chart(fig_scatter, use_container_width=True)

    st.divider()

    # ------------------------------------------------------------------
    # Persona Cards
    # ------------------------------------------------------------------
    st.subheader("👤 Persona Profiles")
    st.caption("Each card summarizes the dominant traits of a discovered segment. Hover over persona names for definitions.")

    persona_cols = st.columns(min(n_clusters, 4))

    for idx, (cluster_id, name) in enumerate(cluster_names.items()):
        col = persona_cols[idx % len(persona_cols)]
        mask = results_df['cluster'] == cluster_id
        segment = results_df.loc[mask]
        profile = segment[feature_cols].mean()
        
        # Get the component types for this cluster
        engagement_type = cluster_engagement_types[cluster_id]
        audience_type = cluster_audience_types[cluster_id]

        with col:
            st.markdown(f"#### {name}")
            st.caption(f"Segment {cluster_id + 1} · {len(segment)} GA4 web channels")
            
            # Add explanation of what this persona means
            engagement_info = ENGAGEMENT_PERSONAS.get(engagement_type, {})
            audience_info = AUDIENCE_PERSONAS.get(audience_type, {})
            
            with st.expander("💡 What does this mean?", expanded=False):
                st.markdown(f"**{engagement_type}:** {engagement_info.get('description', 'N/A')}")
                st.markdown(f"*{engagement_info.get('behavior', '')}*")
                st.markdown("")
                st.markdown(f"**{audience_type}:** {audience_info.get('description', 'N/A')}")
                st.markdown(f"*{audience_info.get('behavior', '')}*")
                st.markdown("")
                st.markdown("**📋 Recommended Actions:**")
                st.markdown(f"- {engagement_info.get('action', 'N/A')}")
                st.markdown(f"- {audience_info.get('action', 'N/A')}")

            # Key metrics with inline explanations via help parameter
            st.metric(
                "Avg Sessions", 
                f"{profile['total_sessions']:,.0f}",
                help="Average total sessions per channel in this segment"
            )
            st.metric(
                "Pages / Session", 
                f"{profile['pages_per_session']:.1f}",
                help="Average pages viewed per session — higher means deeper engagement"
            )
            st.metric(
                "Bounce Rate", 
                f"{profile['avg_bounce_rate']:.0%}",
                help="% of sessions with only one pageview — lower is generally better"
            )
            st.metric(
                "New User %", 
                f"{profile['new_user_ratio']:.0%}",
                help="% of users who are first-time visitors from this channel"
            )

            # Top channels in this segment
            top = segment.nlargest(3, 'total_sessions')['label'].tolist()
            st.markdown("**Top Channels (GA4 Web):**")
            for ch in top:
                st.markdown(f"- {ch}")

    st.divider()

    # ------------------------------------------------------------------
    # Segment Comparison Table
    # ------------------------------------------------------------------
    st.subheader("📋 Segment Comparison")
    st.markdown(
        "*Comparing all discovered segments across key metrics. "
        "Data source: **GA4 Web Traffic** (acquisition channels).*"
    )

    comparison_rows = []
    for cluster_id, name in cluster_names.items():
        mask = results_df['cluster'] == cluster_id
        segment = results_df.loc[mask]
        profile = segment[feature_cols].mean()
        comparison_rows.append({
            'Segment': name,
            'Channels': len(segment),
            'Avg Sessions': f"{profile['total_sessions']:,.0f}",
            'Pages/Session': f"{profile['pages_per_session']:.1f}",
            'Bounce Rate': f"{profile['avg_bounce_rate']:.0%}",
            'New User %': f"{profile['new_user_ratio']:.0%}",
            'Total Pageviews': f"{profile['total_pageviews']:,.0f}",
        })

    comparison_df = pd.DataFrame(comparison_rows)
    st.dataframe(comparison_df, use_container_width=True, hide_index=True)
    
    # Add column definitions
    with st.expander("📖 Column Definitions", expanded=False):
        st.markdown("""
        | Column | Definition |
        |--------|------------|
        | **Segment** | Auto-generated persona name based on behavior patterns |
        | **Channels** | Number of source/medium combinations in this cluster |
        | **Avg Sessions** | Average session count per channel in this segment |
        | **Pages/Session** | Average depth of engagement (pages viewed per visit) |
        | **Bounce Rate** | % of single-page sessions (leave without interaction) |
        | **New User %** | Proportion of first-time visitors in this segment |
        | **Total Pageviews** | Average total page views across channels |
        """)

    st.divider()

    # ------------------------------------------------------------------
    # Radar chart – feature profile per segment
    # ------------------------------------------------------------------
    st.subheader("🕸️ Segment Feature Profiles")
    st.caption("Radar chart showing how each segment scores across behavioral dimensions.")
    
    st.markdown(
        """
        <div style="background-color: #4a3f00; color: #fff9e6; padding: 10px; border-radius: 6px; margin-bottom: 12px; border-left: 4px solid #ffc107;">
        <strong style="color: #ffffff;">📊 How to Read This Chart:</strong> Each axis represents a normalized metric (0-1 scale). 
        Segments that extend further on an axis have higher values for that metric. Compare shapes to see 
        which segments excel in different areas. Data source: <strong style="color: #ffffff;">GA4 Web Traffic</strong>.
        </div>
        """,
        unsafe_allow_html=True
    )

    # Normalize feature means to 0-1 range for radar comparison
    from sklearn.preprocessing import MinMaxScaler

    radar_features = ['total_sessions', 'pages_per_session', 'avg_bounce_rate',
                      'new_user_ratio', 'total_pageviews', 'days_active']
    radar_labels = ['Sessions', 'Pages/Session', 'Bounce Rate',
                    'New User %', 'Pageviews', 'Days Active']

    radar_data = []
    for cluster_id, name in cluster_names.items():
        mask = results_df['cluster'] == cluster_id
        means = results_df.loc[mask, radar_features].mean().values
        radar_data.append(means)

    radar_array = np.array(radar_data)
    # Normalize each feature column to 0-1
    mins = radar_array.min(axis=0)
    maxs = radar_array.max(axis=0)
    ranges = maxs - mins
    ranges[ranges == 0] = 1  # avoid division by zero
    radar_norm = (radar_array - mins) / ranges

    fig_radar = go.Figure()
    colors = px.colors.qualitative.Set2

    for idx, (cluster_id, name) in enumerate(cluster_names.items()):
        values = radar_norm[idx].tolist()
        values.append(values[0])  # close the polygon
        fig_radar.add_trace(go.Scatterpolar(
            r=values,
            theta=radar_labels + [radar_labels[0]],
            fill='toself',
            name=name,
            line_color=colors[idx % len(colors)],
            opacity=0.6
        ))

    fig_radar.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 1])),
        height=450,
        legend=dict(orientation='h', y=-0.1)
    )
    st.plotly_chart(fig_radar, use_container_width=True)

    # ==================================================================
    # Section 2 – Geographic Behavior Segments (if data available)
    # ==================================================================
    if geo_df is not None and len(geo_df) >= 4:
        st.divider()
        st.subheader("🌍 Geographic Behavior Segments")
        st.caption("Clusters countries by engagement patterns to find regional personas.")
        
        st.markdown(
            """
            <div style="background-color: #0d3b66; color: #e0f0ff; padding: 10px; border-radius: 6px; margin-bottom: 12px; border-left: 4px solid #2196F3;">
            <strong style="color: #ffffff;">📍 Data Source:</strong> This section analyzes <strong style="color: #ffffff;">GA4 Geographic Data</strong> — 
            user sessions and behavior patterns grouped by country. Each row represents a cluster of countries 
            that show similar engagement patterns on your <strong style="color: #ffffff;">website</strong>.
            </div>
            """,
            unsafe_allow_html=True
        )

        geo_features = ['total_sessions', 'total_users', 'new_users',
                        'new_user_ratio', 'days_active']
        geo_X = geo_df[geo_features].fillna(0)

        n_geo_k = min(_find_optimal_k(geo_X, max_k=6), len(geo_df) - 1)
        geo_labels, geo_coords, _, _ = _run_clustering(geo_X, n_clusters=n_geo_k)

        geo_results = geo_df.copy()
        geo_results['cluster'] = geo_labels

        # Summary table
        geo_summary_rows = []
        for c in range(n_geo_k):
            mask = geo_results['cluster'] == c
            subset = geo_results.loc[mask]
            geo_summary_rows.append({
                'Segment': f"Region Group {c + 1}",
                'Countries': len(subset),
                'Top Countries': ', '.join(subset.nlargest(3, 'total_sessions')['country'].tolist()),
                'Avg Sessions': f"{subset['total_sessions'].mean():,.0f}",
                'Avg Users': f"{subset['total_users'].mean():,.0f}",
                'Avg New %': f"{subset['new_user_ratio'].mean():.0%}",
            })

        st.dataframe(pd.DataFrame(geo_summary_rows), use_container_width=True, hide_index=True)

    # ==================================================================
    # Section 3 – Technology Preference Breakdown
    # ==================================================================
    if tech_df is not None and not tech_df.empty:
        st.divider()
        st.subheader("💻 Technology Preferences")
        st.caption("Device, OS, and browser distribution across your user base.")
        
        st.markdown(
            """
            <div style="background-color: #1b4332; color: #d8f3dc; padding: 10px; border-radius: 6px; margin-bottom: 12px; border-left: 4px solid #4CAF50;">
            <strong style="color: #ffffff;">📱 Data Source:</strong> This section analyzes <strong style="color: #ffffff;">GA4 Technology Data</strong> — 
            how your <strong style="color: #ffffff;">website</strong> users are distributed across different devices, operating systems, 
            and browsers. Use this to prioritize development and testing efforts.
            </div>
            """,
            unsafe_allow_html=True
        )

        tech_col1, tech_col2 = st.columns(2)

        with tech_col1:
            # Device split
            device_agg = tech_df.groupby('deviceCategory')['total_sessions'].sum().reset_index()
            fig_device = px.pie(
                device_agg, values='total_sessions', names='deviceCategory',
                title='Sessions by Device', hole=0.4,
                color_discrete_sequence=px.colors.qualitative.Pastel
            )
            fig_device.update_layout(height=300, margin=dict(t=40, b=10))
            st.plotly_chart(fig_device, use_container_width=True)

        with tech_col2:
            # OS split
            os_agg = tech_df.groupby('operatingSystem')['total_sessions'].sum().nlargest(6).reset_index()
            fig_os = px.pie(
                os_agg, values='total_sessions', names='operatingSystem',
                title='Sessions by OS (Top 6)', hole=0.4,
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            fig_os.update_layout(height=300, margin=dict(t=40, b=10))
            st.plotly_chart(fig_os, use_container_width=True)

    # ==================================================================
    # Section 4 – Mobile App Insights (AppsFlyer)
    # ==================================================================
    if af_df is not None and not af_df.empty:
        st.divider()
        st.subheader("📱 Mobile App User Segments")
        st.caption("App installation and engagement patterns from AppsFlyer.")
        
        st.markdown(
            """
            <div style="background-color: #5c1a33; color: #fce4ec; padding: 10px; border-radius: 6px; margin-bottom: 12px; border-left: 4px solid #e91e63;">
            <strong style="color: #ffffff;">📲 Data Source:</strong> This section analyzes <strong style="color: #ffffff;">AppsFlyer Mobile App Data</strong> — 
            app installs, user loyalty, revenue, and engagement grouped by country and media source. 
            This is <strong style="color: #ffffff;">mobile app</strong> behavior, separate from website traffic above.
            </div>
            """,
            unsafe_allow_html=True
        )
        
        af_col1, af_col2 = st.columns(2)
        
        with af_col1:
            # Installs by media source
            source_agg = af_df.groupby('media_source').agg({
                'total_installs': 'sum',
                'total_revenue': 'sum',
                'avg_loyalty_rate': 'mean'
            }).reset_index().nlargest(8, 'total_installs')
            
            fig_installs = px.bar(
                source_agg,
                x='media_source',
                y='total_installs',
                title='App Installs by Media Source',
                color='avg_loyalty_rate',
                color_continuous_scale='Greens',
                labels={'total_installs': 'Installs', 'media_source': 'Media Source', 'avg_loyalty_rate': 'Loyalty Rate'}
            )
            fig_installs.update_layout(height=350, showlegend=False)
            st.plotly_chart(fig_installs, use_container_width=True)
        
        with af_col2:
            # Platform distribution
            platform_agg = af_df.groupby('platform')['total_installs'].sum().reset_index()
            fig_platform = px.pie(
                platform_agg, values='total_installs', names='platform',
                title='Installs by Platform (iOS vs Android)', hole=0.4,
                color_discrete_sequence=['#5AC8FA', '#A4C639']
            )
            fig_platform.update_layout(height=350, margin=dict(t=40, b=10))
            st.plotly_chart(fig_platform, use_container_width=True)
        
        # AppsFlyer metrics definitions
        with st.expander("📖 AppsFlyer Metric Definitions", expanded=False):
            st.markdown("""
            | Metric | Definition |
            |--------|------------|
            | **Installs** | Number of app installations attributed to this source |
            | **Loyal Users** | Users who opened the app 3+ times (configurable in AppsFlyer) |
            | **Loyalty Rate** | Loyal Users / Installs — indicates retention quality |
            | **ARPU** | Average Revenue Per User — revenue generated per install |
            | **Conversion Rate** | % of clicks that resulted in installs |
            """)
        
        # Summary table of top sources
        st.markdown("**Top Performing Media Sources (Mobile App):**")
        af_summary = af_df.groupby('media_source').agg({
            'total_installs': 'sum',
            'total_loyal_users': 'sum',
            'total_revenue': 'sum',
            'avg_loyalty_rate': 'mean',
            'avg_arpu': 'mean'
        }).reset_index()
        af_summary['loyalty_rate_fmt'] = af_summary['avg_loyalty_rate'].apply(lambda x: f"{x:.1%}" if pd.notna(x) else "N/A")
        af_summary['arpu_fmt'] = af_summary['avg_arpu'].apply(lambda x: f"${x:.2f}" if pd.notna(x) else "N/A")
        af_summary = af_summary.nlargest(10, 'total_installs')
        
        display_df = af_summary[['media_source', 'total_installs', 'total_loyal_users', 'loyalty_rate_fmt', 'total_revenue', 'arpu_fmt']].copy()
        display_df.columns = ['Media Source', 'Installs', 'Loyal Users', 'Loyalty Rate', 'Revenue', 'ARPU']
        st.dataframe(display_df, use_container_width=True, hide_index=True)

    # ==================================================================
    # Methodology
    # ==================================================================
    st.divider()
    with st.expander("🔬 Methodology & Interpretation Guide"):
        st.markdown("""
        ## Data Sources Used
        
        This analysis integrates data from multiple platforms to provide a holistic view:
        
        | Platform | Data Type | Analysis Focus |
        |----------|-----------|----------------|
        | **GA4 (Web)** | Traffic Overview | Channel segmentation & engagement personas |
        | **GA4 (Web)** | Geographic Data | Regional behavior patterns |
        | **GA4 (Web)** | Technology Data | Device, OS, browser preferences |
        | **AppsFlyer (Mobile)** | Attribution Data | App install sources & loyalty metrics |
        
        ---
        
        ## Algorithm: K-Means Clustering
        
        **What it does:** Groups similar data points together by minimizing the distance 
        between points and their cluster centers (centroids).
        
        **Feature Engineering:**
        - Features are aggregated per traffic source/medium to build channel-level profiles
        - All features are standardized (zero mean, unit variance) before clustering
        - This ensures no single metric dominates due to scale differences
        - PCA (Principal Component Analysis) reduces dimensions for visualization
        
        **Cluster Count Selection:**
        - Auto-detect uses **Silhouette Score** optimization (testing k=2 to 8)
        - Silhouette Score measures how similar a point is to its own cluster vs. other clusters
        - Higher silhouette = better-separated, more cohesive clusters
        
        ---
        
        ## Persona Naming Logic
        
        Personas are automatically assigned based on two dimensions:
        
        **1. Engagement Level** (how deeply users interact):
        - **Deep Engagers**: Pages/Session > 3
        - **Quick Visitors**: Bounce Rate > 70%
        - **Moderate Browsers**: Everything else (1-3 pages, 30-70% bounce)
        
        **2. Audience Composition** (new vs. returning):
        - **New Audience**: New User Ratio > 70%
        - **Returning Core**: New User Ratio < 30%
        - **Mixed Audience**: Everything else
        
        ---
        
        ## How to Use These Insights
        
        | Persona | Recommended Action |
        |---------|-------------------|
        | **Quick Visitors** | Audit landing pages, improve relevance, speed up load times |
        | **Deep Engagers** | Increase budget on these channels, study what makes them work |
        | **New Audience** | Build retention flows, email capture, onboarding sequences |
        | **Returning Core** | Upsell opportunities, loyalty programs, referral incentives |
        | **Moderate Browsers** | A/B test improvements to convert to Deep Engagers |
        """)
