"""
Forecasting - Churn / Engagement Prediction

Uses classification models (Random Forest, Logistic Regression) to predict
which acquisition cohorts and channels are at risk of declining engagement.

Because the warehouse stores aggregated daily data (not user-level rows),
we treat each (source, medium, date) or (country, media_source, date) row
as a *cohort observation* and predict whether the next period's engagement
will decline, stay flat, or grow.

Feature Sources:
- GA4 traffic_overview: sessions, bounce rate, pageviews by channel/date
- AppsFlyer af_daily_geo: installs, sessions, loyal users, revenue by geo/source/date

Output:
- Cohort risk scores
- Feature importance chart
- Trend decomposition
- Engagement forecast
"""

from typing import Optional, List, Tuple
import logging

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


def _load_daily_traffic(duckdb_path: str) -> Optional[pd.DataFrame]:
    """
    Load daily engagement data from GA4 traffic overview.

    Aggregates to channel level per day so we can detect engagement trends.
    """
    sql = """
    SELECT
        date                                       AS date_raw,
        sessionSource                              AS source,
        sessionMedium                              AS medium,
        CAST(sessions        AS DOUBLE)            AS sessions,
        CAST(totalUsers      AS DOUBLE)            AS total_users,
        CAST(newUsers        AS DOUBLE)            AS new_users,
        CAST(bounceRate      AS DOUBLE)            AS bounce_rate,
        CAST(screenPageViews AS DOUBLE)            AS pageviews
    FROM ga4_traffic_overview
    WHERE sessionSource IS NOT NULL
    ORDER BY date
    """
    df = _query(duckdb_path, sql)
    if df is None:
        return None

    # Parse GA4 date format (YYYYMMDD)
    df['date'] = pd.to_datetime(df['date_raw'], format='%Y%m%d', errors='coerce')
    df = df.dropna(subset=['date'])
    return df


def _load_daily_appsflyer(duckdb_path: str) -> Optional[pd.DataFrame]:
    """
    Load daily app engagement data from AppsFlyer.
    """
    sql = """
    SELECT
        date,
        country,
        media_source,
        platform,
        installs,
        sessions,
        loyal_users,
        loyal_users_per_install,
        total_revenue,
        conversion_rate,
        clicks
    FROM af_daily_geo
    WHERE media_source IS NOT NULL
    ORDER BY date
    """
    df = _query(duckdb_path, sql)
    if df is None:
        return None
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.dropna(subset=['date'])
    return df


# ============================================
# Feature Engineering
# ============================================

def _build_cohort_features(traffic_df: pd.DataFrame, window: int = 7) -> pd.DataFrame:
    """
    Build rolling-window features for each channel and create a binary
    target: did engagement *decline* in the next window compared to this one?

    Features per window:
    - sessions_sum, pageviews_sum, users_sum
    - avg_bounce_rate
    - sessions_trend (% change from previous window)
    - pageviews_per_session
    - new_user_ratio

    Target:
    - engagement_declined: 1 if next-window sessions < this-window sessions
    """
    # Aggregate by channel per day
    channel_daily = (
        traffic_df
        .groupby(['source', 'medium', 'date'])
        .agg(
            sessions=('sessions', 'sum'),
            pageviews=('pageviews', 'sum'),
            users=('total_users', 'sum'),
            new_users=('new_users', 'sum'),
            bounce_rate=('bounce_rate', 'mean'),
        )
        .reset_index()
        .sort_values(['source', 'medium', 'date'])
    )

    # Create channel identifier
    channel_daily['channel'] = channel_daily['source'] + ' / ' + channel_daily['medium']

    # Rolling window aggregation per channel
    rows = []
    for channel, grp in channel_daily.groupby('channel'):
        grp = grp.sort_values('date').reset_index(drop=True)

        # Need at least 2 windows worth of data
        if len(grp) < window * 2:
            continue

        for start in range(0, len(grp) - window * 2 + 1, max(window // 2, 1)):
            current_window = grp.iloc[start:start + window]
            next_window = grp.iloc[start + window:start + window * 2]

            if len(current_window) < window or len(next_window) < window:
                continue

            cur_sessions = current_window['sessions'].sum()
            nxt_sessions = next_window['sessions'].sum()
            cur_pageviews = current_window['pageviews'].sum()
            cur_users = current_window['users'].sum()
            cur_new_users = current_window['new_users'].sum()

            rows.append({
                'channel': channel,
                'window_start': current_window['date'].iloc[0],
                'window_end': current_window['date'].iloc[-1],
                # Features
                'sessions_sum': cur_sessions,
                'pageviews_sum': cur_pageviews,
                'users_sum': cur_users,
                'avg_bounce_rate': current_window['bounce_rate'].mean(),
                'pageviews_per_session': cur_pageviews / cur_sessions if cur_sessions > 0 else 0,
                'new_user_ratio': cur_new_users / cur_users if cur_users > 0 else 0,
                # Target: did engagement decline?
                'next_sessions': nxt_sessions,
                'engagement_declined': 1 if nxt_sessions < cur_sessions else 0,
                'session_change_pct': ((nxt_sessions - cur_sessions) / cur_sessions * 100)
                                       if cur_sessions > 0 else 0,
            })

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows)


# ============================================
# Model Training & Prediction
# ============================================

def _train_churn_model(
    cohort_df: pd.DataFrame,
    feature_cols: List[str],
    target_col: str = 'engagement_declined'
) -> Tuple[object, object, pd.DataFrame, float]:
    """
    Train a Random Forest classifier to predict engagement decline.

    Returns:
        (model, scaler, feature_importance_df, accuracy_score)
    """
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.preprocessing import StandardScaler
    from sklearn.model_selection import cross_val_score

    X = cohort_df[feature_cols].fillna(0).values
    y = cohort_df[target_col].values

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # Train model
    model = RandomForestClassifier(
        n_estimators=100,
        max_depth=5,
        random_state=42,
        class_weight='balanced'  # Handle imbalanced classes
    )
    model.fit(X_scaled, y)

    # Cross-validated accuracy
    cv_scores = cross_val_score(model, X_scaled, y, cv=min(5, len(cohort_df) // 3), scoring='accuracy')
    accuracy = cv_scores.mean()

    # Feature importance
    importance_df = pd.DataFrame({
        'Feature': feature_cols,
        'Importance': model.feature_importances_
    }).sort_values('Importance', ascending=False)

    return model, scaler, importance_df, accuracy


# ============================================
# Dashboard Rendering
# ============================================

def render_forecasting(duckdb_path: str):
    """
    Main render function for the Churn/Engagement Prediction page.

    Loads time-series channel data, engineers rolling-window features,
    trains a classifier, and displays risk scores with feature importance.
    """

    st.header("📈 Churn & Engagement Prediction")
    st.markdown(
        "Predicts which acquisition channels are at risk of declining engagement "
        "using machine learning on rolling-window cohort data."
    )

    # ------------------------------------------------------------------
    # Load data
    # ------------------------------------------------------------------
    traffic_df = _load_daily_traffic(duckdb_path)
    af_df = _load_daily_appsflyer(duckdb_path)

    sources_loaded = []
    if traffic_df is not None:
        date_range = f"{traffic_df['date'].min().strftime('%Y-%m-%d')} → {traffic_df['date'].max().strftime('%Y-%m-%d')}"
        sources_loaded.append(f"GA4 Traffic ({len(traffic_df):,} rows, {date_range})")
    if af_df is not None:
        sources_loaded.append(f"AppsFlyer ({len(af_df):,} rows)")

    if traffic_df is None or len(traffic_df) < 30:
        st.warning(
            "**Insufficient data for churn prediction.**\n\n"
            "This module needs at least 30 days of GA4 traffic data. "
            "Run the GA4 ETL to populate:\n"
            "```bash\npython scripts/run_etl_unified.py --source ga4 --lifetime\n```"
        )
        return

    st.success(f"Data loaded: {' · '.join(sources_loaded)}")

    # ------------------------------------------------------------------
    # Configuration
    # ------------------------------------------------------------------
    st.divider()

    config_col1, config_col2 = st.columns(2)
    with config_col1:
        window_size = st.selectbox(
            "Rolling Window Size (days)",
            options=[3, 5, 7, 14],
            index=2,
            key="fc_window",
            help="Number of days per observation window. Smaller = more sensitive, larger = more stable."
        )
    with config_col2:
        st.info(f"Each channel's engagement is measured in **{window_size}-day windows**, "
                "then compared to the next window to detect decline.")

    # ------------------------------------------------------------------
    # Build features and train model
    # ------------------------------------------------------------------
    cohort_df = _build_cohort_features(traffic_df, window=window_size)

    if cohort_df.empty or len(cohort_df) < 10:
        st.warning(
            f"Not enough rolling-window observations with a {window_size}-day window. "
            "Try a smaller window or load more historical data."
        )
        return

    feature_cols = [
        'sessions_sum', 'pageviews_sum', 'users_sum',
        'avg_bounce_rate', 'pageviews_per_session', 'new_user_ratio'
    ]

    model, scaler, importance_df, accuracy = _train_churn_model(cohort_df, feature_cols)

    st.divider()

    # ==================================================================
    # Section 1 – Model Performance
    # ==================================================================
    st.subheader("🤖 Model Performance")

    perf_col1, perf_col2, perf_col3 = st.columns(3)

    with perf_col1:
        st.metric("Cross-Val Accuracy", f"{accuracy:.1%}",
                  help="5-fold cross-validated accuracy")
    with perf_col2:
        decline_rate = cohort_df['engagement_declined'].mean()
        st.metric("Decline Rate", f"{decline_rate:.1%}",
                  help="% of windows where engagement dropped")
    with perf_col3:
        st.metric("Observations", f"{len(cohort_df):,}",
                  help=f"Total {window_size}-day cohort windows analyzed")

    st.divider()

    # ==================================================================
    # Section 2 – Feature Importance
    # ==================================================================
    st.subheader("📊 What Drives Engagement Decline?")
    st.caption("Feature importance from the Random Forest model — higher = more predictive of decline.")

    imp_col1, imp_col2 = st.columns([2, 1])

    with imp_col1:
        # Horizontal bar chart
        fig_imp = px.bar(
            importance_df,
            x='Importance', y='Feature',
            orientation='h',
            color='Importance',
            color_continuous_scale='RdYlGn_r',
            title='Feature Importance for Engagement Decline'
        )
        fig_imp.update_layout(height=350, showlegend=False, yaxis=dict(autorange='reversed'))
        st.plotly_chart(fig_imp, use_container_width=True)

    with imp_col2:
        st.markdown("**Interpretation:**")
        top_feature = importance_df.iloc[0]['Feature']
        feature_descriptions = {
            'sessions_sum': "Total session volume is the strongest signal — channels with unstable session counts are most at risk.",
            'pageviews_sum': "Pageview volume changes predict engagement shifts.",
            'users_sum': "User count fluctuations indicate audience instability.",
            'avg_bounce_rate': "High bounce rate is a leading indicator of declining engagement.",
            'pageviews_per_session': "Content depth (pages/session) changes correlate with engagement decline.",
            'new_user_ratio': "Channels overly dependent on new users may lack retention."
        }
        st.info(feature_descriptions.get(top_feature, f"{top_feature} is the most important predictor."))

    st.divider()

    # ==================================================================
    # Section 3 – Channel Risk Scores
    # ==================================================================
    st.subheader("⚠️ Channel Risk Assessment")
    st.caption("Each channel's most recent window is scored for engagement decline probability.")

    # Get the latest window per channel
    latest_cohorts = (
        cohort_df
        .sort_values('window_end')
        .groupby('channel')
        .last()
        .reset_index()
    )

    # Predict decline probability
    X_latest = latest_cohorts[feature_cols].fillna(0).values
    X_latest_scaled = scaler.transform(X_latest)
    decline_probs = model.predict_proba(X_latest_scaled)

    # Probability of class 1 (declined)
    latest_cohorts['risk_score'] = decline_probs[:, 1] if decline_probs.shape[1] > 1 else decline_probs[:, 0]

    # Risk level labels
    def risk_level(score):
        if score >= 0.7:
            return "🔴 High"
        elif score >= 0.4:
            return "🟡 Medium"
        else:
            return "🟢 Low"

    latest_cohorts['risk_level'] = latest_cohorts['risk_score'].apply(risk_level)

    # Sort by risk
    latest_cohorts = latest_cohorts.sort_values('risk_score', ascending=False)

    # Summary metrics
    risk_m1, risk_m2, risk_m3 = st.columns(3)
    high_risk = (latest_cohorts['risk_score'] >= 0.7).sum()
    med_risk = ((latest_cohorts['risk_score'] >= 0.4) & (latest_cohorts['risk_score'] < 0.7)).sum()
    low_risk = (latest_cohorts['risk_score'] < 0.4).sum()

    with risk_m1:
        st.metric("🔴 High Risk", high_risk)
    with risk_m2:
        st.metric("🟡 Medium Risk", med_risk)
    with risk_m3:
        st.metric("🟢 Low Risk", low_risk)

    # Risk score bar chart
    fig_risk = px.bar(
        latest_cohorts.head(15),
        x='channel', y='risk_score',
        color='risk_score',
        color_continuous_scale='RdYlGn_r',
        title='Engagement Decline Risk by Channel (Top 15)',
        labels={'risk_score': 'Risk Score', 'channel': 'Channel'}
    )
    fig_risk.update_layout(height=400, xaxis_tickangle=-45)
    fig_risk.add_hline(y=0.7, line_dash="dash", line_color="red", annotation_text="High Risk")
    fig_risk.add_hline(y=0.4, line_dash="dash", line_color="orange", annotation_text="Medium Risk")
    st.plotly_chart(fig_risk, use_container_width=True)

    # Detailed risk table
    display_cols = ['channel', 'risk_level', 'risk_score', 'sessions_sum',
                    'pageviews_per_session', 'avg_bounce_rate', 'new_user_ratio',
                    'session_change_pct']
    display_df = latest_cohorts[display_cols].copy()
    display_df['risk_score'] = display_df['risk_score'].apply(lambda x: f"{x:.0%}")
    display_df['sessions_sum'] = display_df['sessions_sum'].apply(lambda x: f"{x:,.0f}")
    display_df['pageviews_per_session'] = display_df['pageviews_per_session'].apply(lambda x: f"{x:.1f}")
    display_df['avg_bounce_rate'] = display_df['avg_bounce_rate'].apply(lambda x: f"{x:.0%}")
    display_df['new_user_ratio'] = display_df['new_user_ratio'].apply(lambda x: f"{x:.0%}")
    display_df['session_change_pct'] = display_df['session_change_pct'].apply(lambda x: f"{x:+.1f}%")

    display_df = display_df.rename(columns={
        'channel': 'Channel',
        'risk_level': 'Risk',
        'risk_score': 'Score',
        'sessions_sum': 'Sessions',
        'pageviews_per_session': 'Pages/Sess',
        'avg_bounce_rate': 'Bounce',
        'new_user_ratio': 'New %',
        'session_change_pct': 'Δ Sessions'
    })
    st.dataframe(display_df, use_container_width=True, hide_index=True)

    st.divider()

    # ==================================================================
    # Section 4 – Engagement Trend Over Time
    # ==================================================================
    st.subheader("📈 Engagement Trend")
    st.caption("Overall daily sessions trend with rolling average to spot macro-level shifts.")

    daily_agg = (
        traffic_df
        .groupby('date')
        .agg(total_sessions=('sessions', 'sum'), total_users=('total_users', 'sum'))
        .reset_index()
        .sort_values('date')
    )

    # Rolling averages
    daily_agg['sessions_7d_avg'] = daily_agg['total_sessions'].rolling(7, min_periods=1).mean()
    daily_agg['sessions_14d_avg'] = daily_agg['total_sessions'].rolling(14, min_periods=1).mean()

    fig_trend = make_subplots(specs=[[{"secondary_y": False}]])

    fig_trend.add_trace(go.Bar(
        x=daily_agg['date'], y=daily_agg['total_sessions'],
        name='Daily Sessions', marker_color='#d5e8f0', opacity=0.6
    ))
    fig_trend.add_trace(go.Scatter(
        x=daily_agg['date'], y=daily_agg['sessions_7d_avg'],
        name='7-Day Avg', line=dict(color='#2196F3', width=2)
    ))
    fig_trend.add_trace(go.Scatter(
        x=daily_agg['date'], y=daily_agg['sessions_14d_avg'],
        name='14-Day Avg', line=dict(color='#FF5722', width=2, dash='dot')
    ))

    fig_trend.update_layout(
        title='Daily Sessions with Rolling Averages',
        height=400,
        hovermode='x unified',
        legend=dict(orientation='h', y=-0.15)
    )
    st.plotly_chart(fig_trend, use_container_width=True)

    # ==================================================================
    # Methodology
    # ==================================================================
    st.divider()
    with st.expander("🔬 Methodology & Interpretation Guide"):
        st.markdown(f"""
        **Approach:** Rolling-window cohort analysis with Random Forest classification

        **How it works:**
        1. Traffic data is grouped by channel (source/medium) per day
        2. Each channel's data is split into {window_size}-day windows
        3. For each window, engagement features are computed (sessions, bounce rate, etc.)
        4. The target is whether the *next* window has fewer sessions (engagement declined)
        5. A Random Forest classifier learns which feature patterns predict decline

        **Risk Score Interpretation:**
        - **🔴 High Risk (≥70%):** Strong probability of engagement decline — investigate immediately
        - **🟡 Medium Risk (40-70%):** Some decline signals — monitor closely
        - **🟢 Low Risk (<40%):** Engagement appears stable

        **Limitations:**
        - Works on aggregated channel data, not individual users
        - Accuracy depends on data volume and history length
        - External factors (seasonality, campaigns) may affect predictions

        **Actions:**
        - High-risk channels: Review landing pages, ad copy, targeting
        - Monitor bounce rate spikes as early warning
        - Channels with high new-user ratio may need retention strategies
        """)
