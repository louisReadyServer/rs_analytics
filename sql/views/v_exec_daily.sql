-- ============================================
-- v_exec_daily: Canonical cross-channel daily view
-- ============================================
--
-- PURPOSE:
--   Single source of truth for executive-level daily metrics.
--   Normalizes all data sources (Google Ads, Meta Ads, GSC, AppsFlyer,
--   User Logs) into one consistent schema.
--
-- USED BY:
--   - MetricEngine (rs_analytics/metrics/engine.py)
--   - Executive Dashboard KPI tiles, trend charts, channel table
--   - Change Detection engine
--
-- CANONICAL COLUMNS:
--   date_day       DATE      Calendar date
--   channel        VARCHAR   Canonical channel key (google_ads, meta_ads, etc.)
--   channel_type   VARCHAR   'paid' | 'organic' | 'app'
--   spend          DOUBLE    Ad spend (0 for non-paid channels)
--   clicks         BIGINT    Clicks / link clicks
--   impressions    BIGINT    Impressions
--   installs       BIGINT    App installs (0 if N/A)
--   signups        BIGINT    Sign-ups / registrations (0 if N/A)
--   conversions    DOUBLE    Platform-reported conversions
--   revenue        DOUBLE    Conversion value / purchase revenue
--   attribution    VARCHAR   'platform_native' | 'internal_observed'
--
-- NOTES:
--   - Google Ads 'cost' column is already converted from micros.
--   - GA4 is NOT included here because it doesn't map to the
--     spend/clicks/conversions schema. GA4 data lives in fact_web_daily.
--   - AppsFlyer splits organic vs paid via the media_source column.
--   - User Logs revenue (mart.platform_daily_overview) uses 'internal_observed'
--     attribution since it's from server-side payment logs, not an ad platform.
--
-- TODO: If your af_daily_geo has different event column names, adjust the
--       COALESCE expressions in the appsflyer CTE below.
-- ============================================

CREATE OR REPLACE VIEW v_exec_daily AS

-- ── Google Ads ──────────────────────────────────────────────
WITH gads AS (
    SELECT
        CAST(date AS DATE)                        AS date_day,
        'google_ads'                              AS channel,
        'paid'                                    AS channel_type,
        SUM(cost)                                 AS spend,
        SUM(clicks)                               AS clicks,
        SUM(impressions)                          AS impressions,
        0                                         AS installs,
        0                                         AS signups,
        SUM(COALESCE(conversions, 0))             AS conversions,
        SUM(COALESCE(conversions_value, 0))       AS revenue,
        'platform_native'                         AS attribution
    FROM gads_campaigns
    GROUP BY CAST(date AS DATE)
),

-- ── Meta Ads ────────────────────────────────────────────────
meta AS (
    SELECT
        date                                      AS date_day,
        'meta_ads'                                AS channel,
        'paid'                                    AS channel_type,
        SUM(spend)                                AS spend,
        SUM(clicks)                               AS clicks,
        SUM(impressions)                          AS impressions,
        SUM(COALESCE(app_installs, 0))            AS installs,
        0                                         AS signups,
        SUM(COALESCE(app_installs, 0))            AS conversions,
        SUM(COALESCE(purchase_value, 0))          AS revenue,
        'platform_native'                         AS attribution
    FROM meta_campaign_insights
    GROUP BY date
),

-- ── Organic Search (GSC) ────────────────────────────────────
gsc AS (
    SELECT
        CAST(date AS DATE)                        AS date_day,
        'organic_search'                          AS channel,
        'organic'                                 AS channel_type,
        0                                         AS spend,
        SUM(clicks)                               AS clicks,
        SUM(impressions)                          AS impressions,
        0                                         AS installs,
        0                                         AS signups,
        0                                         AS conversions,
        0                                         AS revenue,
        'platform_native'                         AS attribution
    FROM gsc_daily_totals
    GROUP BY CAST(date AS DATE)
),

-- ── AppsFlyer (mobile installs, aggregated across geo) ──────
appsflyer AS (
    SELECT
        date                                      AS date_day,
        CASE
            WHEN media_source IN ('organic', 'Organic', '')
            THEN 'appsflyer_organic'
            ELSE 'appsflyer_paid'
        END                                       AS channel,
        CASE
            WHEN media_source IN ('organic', 'Organic', '')
            THEN 'organic'
            ELSE 'paid'
        END                                       AS channel_type,
        SUM(COALESCE(total_cost, 0))              AS spend,
        SUM(COALESCE(clicks, 0))                  AS clicks,
        SUM(COALESCE(impressions, 0))             AS impressions,
        SUM(COALESCE(installs, 0))                AS installs,
        SUM(COALESCE(user_sign_up_unique_users, 0)) AS signups,
        SUM(COALESCE(installs, 0))                AS conversions,
        SUM(COALESCE(total_revenue, 0))           AS revenue,
        'platform_native'                         AS attribution
    FROM af_daily_geo
    GROUP BY
        date,
        CASE WHEN media_source IN ('organic', 'Organic', '') THEN 'appsflyer_organic' ELSE 'appsflyer_paid' END,
        CASE WHEN media_source IN ('organic', 'Organic', '') THEN 'organic' ELSE 'paid' END
),

-- ── App Revenue (internal user logs) ────────────────────────
app_rev AS (
    SELECT
        activity_date                             AS date_day,
        'app_revenue'                             AS channel,
        'app'                                     AS channel_type,
        0                                         AS spend,
        0                                         AS clicks,
        0                                         AS impressions,
        0                                         AS installs,
        COALESCE(new_signups, 0)                  AS signups,
        COALESCE(payer_count, 0)                  AS conversions,
        COALESCE(topups_sum_sgd, 0)               AS revenue,
        'internal_observed'                       AS attribution
    FROM mart.platform_daily_overview
)

-- ── UNION all sources ───────────────────────────────────────
SELECT * FROM gads
UNION ALL
SELECT * FROM meta
UNION ALL
SELECT * FROM gsc
UNION ALL
SELECT * FROM appsflyer
UNION ALL
SELECT * FROM app_rev;
