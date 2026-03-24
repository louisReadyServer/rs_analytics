"""
Source adapter mappings for channel normalization.

This is the ONE place where we map platform-specific column names and values
to canonical names used by the metric layer and canonical views.

Why this exists:
- Google Ads uses 'cost' (already converted) and 'cost_micros' (raw)
- Meta uses 'spend'
- GA4 dates are YYYYMMDD strings, others use YYYY-MM-DD or DATE
- Each platform calls conversions differently
- Channel names vary: 'google_ads' vs 'Google Ads' vs 'google ads'

When adding a new data source, add its mapping here and update
data/views/v_exec_daily.sql accordingly.

TODO markers below indicate column mappings that may need adjustment
based on your actual DuckDB table schemas. Check with:
    SELECT column_name FROM information_schema.columns WHERE table_name = '...'
"""

from typing import Dict, List, Optional


# ============================================
# Canonical Channel Names
# ============================================

# These are the standard channel identifiers used across the dashboard.
# Every view and metric should use these exact strings.
CHANNEL_MAP: Dict[str, str] = {
    # Paid channels (have spend data)
    "google_ads": "Google Ads",
    "meta_ads": "Meta Ads",
    "twitter_ads": "Twitter/X",

    # Organic channels (no spend)
    "organic_search": "Organic Search",
    "direct": "Direct",
    "referral": "Referral",

    # App channels
    "appsflyer_organic": "App Organic",
    "appsflyer_paid": "App Paid",

    # Internal
    "app_revenue": "App Revenue",
}


def normalize_channel(raw_channel: str) -> str:
    """
    Convert a raw channel/platform string to the canonical display name.

    Falls back to title-casing the input if no mapping exists.

    Args:
        raw_channel: Platform identifier from a query (e.g., 'google_ads')

    Returns:
        Human-readable channel name (e.g., 'Google Ads')

    Examples:
        >>> normalize_channel("google_ads")   # "Google Ads"
        >>> normalize_channel("meta_ads")     # "Meta Ads"
        >>> normalize_channel("some_new")     # "Some New"
    """
    if not raw_channel:
        return "Unknown"
    key = raw_channel.strip().lower().replace(" ", "_")
    return CHANNEL_MAP.get(key, raw_channel.replace("_", " ").title())


# ============================================
# Source-Specific Column Adapters
# ============================================

# These dictionaries map each source's native column names to the canonical
# names expected by v_exec_daily and the MetricEngine.
#
# Canonical schema for v_exec_daily:
#   date_day      DATE         The calendar date
#   channel       VARCHAR      Canonical channel key (e.g., 'google_ads')
#   channel_type  VARCHAR      'paid' | 'organic' | 'app'
#   spend         DOUBLE       Ad spend in currency (0 for organic)
#   clicks        BIGINT       Clicks
#   impressions   BIGINT       Impressions
#   installs      BIGINT       App installs (0 if N/A)
#   signups       BIGINT       Sign-ups (0 if N/A)
#   conversions   DOUBLE       Platform-reported conversions
#   revenue       DOUBLE       Conversion value / purchase value
#   attribution   VARCHAR      'platform_native' | 'internal_observed'

GOOGLE_ADS_COLUMNS = {
    # Source table: gads_campaigns (or gads_daily_summary)
    # Date column is VARCHAR 'YYYY-MM-DD', cast to DATE
    "date": "CAST(date AS DATE)",
    "channel": "'google_ads'",
    "channel_type": "'paid'",
    "spend": "cost",                             # Already converted from micros
    "clicks": "clicks",
    "impressions": "impressions",
    "installs": "0",                             # Google Ads doesn't track installs directly
    "signups": "0",
    "conversions": "COALESCE(conversions, 0)",
    "revenue": "COALESCE(conversions_value, 0)",
    "attribution": "'platform_native'",
}

META_ADS_COLUMNS = {
    # Source table: meta_campaign_insights
    # Date column is already DATE type
    "date": "date",
    "channel": "'meta_ads'",
    "channel_type": "'paid'",
    "spend": "spend",
    "clicks": "clicks",
    "impressions": "impressions",
    "installs": "COALESCE(app_installs, 0)",
    "signups": "0",
    "conversions": "COALESCE(app_installs, 0)",  # Meta's primary conversion for this app
    "revenue": "COALESCE(purchase_value, 0)",
    "attribution": "'platform_native'",
}

GSC_COLUMNS = {
    # Source table: gsc_daily_totals
    # Date column is VARCHAR 'YYYY-MM-DD'
    "date": "CAST(date AS DATE)",
    "channel": "'organic_search'",
    "channel_type": "'organic'",
    "spend": "0",
    "clicks": "clicks",
    "impressions": "impressions",
    "installs": "0",
    "signups": "0",
    "conversions": "0",   # GSC has no conversion tracking
    "revenue": "0",
    "attribution": "'platform_native'",
}

GA4_COLUMNS = {
    # Source table: ga4_sessions
    # Date column is VARCHAR 'YYYYMMDD' — needs special conversion
    "date": "STRPTIME(date, '%Y%m%d')::DATE",
    "channel": "'ga4_web'",
    "channel_type": "'organic'",
    "spend": "0",
    "clicks": "0",   # GA4 doesn't have "clicks" — sessions is the equivalent
    "impressions": "0",
    "installs": "0",
    "signups": "0",
    "conversions": "0",
    "revenue": "0",
    "attribution": "'platform_native'",
    # GA4-specific extra columns (not in canonical schema, but used by GA4 tab)
    "sessions": "CAST(sessions AS BIGINT)",
    "users": "CAST(totalUsers AS BIGINT)",
    "new_users": "CAST(newUsers AS BIGINT)",
    "bounce_rate": "CAST(bounceRate AS DOUBLE)",
}

APPSFLYER_COLUMNS = {
    # Source table: af_daily_geo
    # Date column is already DATE type
    "date": "date",
    "channel": "CASE WHEN media_source IN ('organic', 'Organic', '') THEN 'appsflyer_organic' ELSE 'appsflyer_paid' END",
    "channel_type": "CASE WHEN media_source IN ('organic', 'Organic', '') THEN 'organic' ELSE 'paid' END",
    "spend": "COALESCE(total_cost, 0)",
    "clicks": "COALESCE(clicks, 0)",
    "impressions": "COALESCE(impressions, 0)",
    "installs": "COALESCE(installs, 0)",
    "signups": "COALESCE(user_sign_up_unique_users, 0)",
    "conversions": "COALESCE(installs, 0)",
    "revenue": "COALESCE(total_revenue, 0)",
    "attribution": "'platform_native'",
}

APP_REVENUE_COLUMNS = {
    # Source table: mart.platform_daily_overview
    # This captures internal app revenue (top-ups) that no ad platform tracks
    "date": "activity_date",
    "channel": "'app_revenue'",
    "channel_type": "'app'",
    "spend": "0",
    "clicks": "0",
    "impressions": "0",
    "installs": "0",
    "signups": "new_signups",
    "conversions": "payer_count",
    "revenue": "topups_sum_sgd",
    "attribution": "'internal_observed'",
}


# ============================================
# Data Source Freshness Expectations
# ============================================

# Used by the Data Quality checker (Phase 2) to determine
# whether each source is fresh or stale.
# Freshness thresholds tuned for a local app where ETL is run manually
# (not a production pipeline with hourly syncs). The expected_delay_hours
# represents how old the data can be before a WARNING is raised. FAIL is
# triggered at 1.5× that window. "critical" means a FAIL here → overall
# status becomes FAIL (vs WARN for non-critical).
SOURCE_FRESHNESS = {
    "gads_campaigns": {
        "label": "Google Ads",
        "date_column": "date",
        "date_format": "YYYY-MM-DD",
        "expected_delay_hours": 72,     # 3 days — manual ETL cadence
        "critical": False,
    },
    "meta_campaign_insights": {
        "label": "Meta Ads",
        "date_column": "date",
        "date_format": None,  # Already DATE type
        "expected_delay_hours": 72,     # 3 days — manual ETL cadence
        "critical": False,
    },
    "ga4_sessions": {
        "label": "GA4",
        "date_column": "date",
        "date_format": "YYYYMMDD",
        "expected_delay_hours": 72,     # GA4 has 24-48h processing delay
        "critical": False,
    },
    "gsc_daily_totals": {
        "label": "GSC",
        "date_column": "date",
        "date_format": "YYYY-MM-DD",
        "expected_delay_hours": 120,    # GSC has 2-3 day inherent delay
        "critical": False,
    },
    "af_daily_geo": {
        "label": "AppsFlyer",
        "date_column": "date",
        "date_format": None,  # Already DATE type
        "expected_delay_hours": 120,    # 5 days — manual ETL cadence
        "critical": False,
    },
    "mart.platform_daily_overview": {
        "label": "User Logs",
        "date_column": "activity_date",
        "date_format": None,  # Already DATE type
        "expected_delay_hours": 168,    # Weekly batch, so 7 days is OK
        "critical": False,
    },
}
