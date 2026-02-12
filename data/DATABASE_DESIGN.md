# rs_analytics Database Design

**Database:** DuckDB (Local Analytics Warehouse)  
**Location:** `data/warehouse.duckdb`  
**Total Tables:** 50 (6 GA4 + 10 GSC + 9 Google Ads + 10 Meta Ads + 2 AppsFlyer + 4 ref + 5 core + 4 mart User Logs)  
**Total Rows:** ~134,000+ (analytics) + ~70,000+ (user logs)  
**Last Updated:** 2026-02-12

---

## Table of Contents

1. [Overview](#overview)
2. [GA4 Tables](#ga4-tables-google-analytics-4)
3. [GSC Tables](#gsc-tables-google-search-console)
4. [Google Ads Tables](#google-ads-tables)
5. [Meta Ads Tables](#meta-ads-tables)
6. [User Logs Warehouse (core + mart)](#user-logs-warehouse-core--mart)
7. [Data Dictionary](#data-dictionary)
8. [Query Examples](#query-examples)
9. [ETL Process](#etl-process)

---

## Overview

### Summary by Data Source

| Source | Tables | Rows | Primary Use |
|--------|--------|------|-------------|
| GA4 (Google Analytics 4) | 6 | 26,668 | Website traffic, user behavior, events |
| GSC (Google Search Console) | 10 | 96,873 | Organic search, SEO keywords, rankings |
| Google Ads | 9 | 9,010 | Google paid advertising, campaigns, keywords |
| Meta Ads | 10 | 537 | Facebook/Instagram advertising, demographics |
| AppsFlyer | 2 | 1,046 | Mobile app installs, in-app events, geo |
| User Logs (core + mart) | 13 | ~70,000+ | App users, activity, points ledger, daily/weekly KPIs |
| **Total** | **50** | **~204,000+** | |

---

## User Logs Warehouse (core + mart)

The User Logs warehouse uses two schemas in the same DuckDB database:

- **core**: Source-of-truth event and dimension tables (append-safe, full lineage). Used for auditing and for rebuilding marts.
- **mart**: Pre-aggregated reporting tables for dashboards (daily/weekly overview, per-user daily metrics). Refreshed from core each batch.

Data is loaded from CSV files in `data/user_logs/` with the same file names on each run. Load is idempotent per file: re-running replaces that file’s contribution. Promo rows whose type starts with `dev_testing` are excluded from the database and from all calculations.

### Schema purpose

| Schema | Purpose |
|--------|---------|
| `core` | Detailed, auditable event-level data; immutable ledger; computed user state snapshot |
| `mart` | Daily and weekly aggregates for app overview and per-user metrics |
| `ref` | Reference data (e.g. points packages) |

### ref.dim_points_package

**Purpose:** Points package definitions (SGD amount and points at 144 points per SGD).

| Column | Type | Description |
|--------|------|-------------|
| `package_code` | VARCHAR (PK) | e.g. SGD50, SGD100, SGD200, SGD500 |
| `cash_amount_sgd` | DECIMAL(12,2) | Cash amount in SGD |
| `points_amount` | BIGINT | Points granted |
| `points_per_sgd` | DECIMAL(12,2) | Always 144 |

---

### core.dim_user

**Purpose:** One row per user: registration and mobile verification status.

| Column | Type | Description |
|--------|------|-------------|
| `user_id` | VARCHAR (PK) | Immutable universal user UUID |
| `registration_ts` | TIMESTAMP | Registration timestamp from customer.csv |
| `registration_ip` | VARCHAR | Registration IP (raw) |
| `registration_country_code` | VARCHAR | Derived from IP (e.g. SG); placeholder if not enriched |
| `registration_country_name` | VARCHAR | Derived country name |
| `mobile_verified` | BOOLEAN | TRUE if user appears in redeem-mobile-verification.csv |
| `mobile_verified_at` | TIMESTAMP | First mobile verification reward timestamp |
| `created_at` | TIMESTAMP | Row insertion time |

---

### core.fact_user_activity

**Purpose:** Raw behavioral event log (duplicates allowed). One row per activity event.

| Column | Type | Description |
|--------|------|-------------|
| `activity_event_id` | BIGINT (PK) | Surrogate key |
| `user_id` | VARCHAR | User who performed the action |
| `event_ts` | TIMESTAMP | Event timestamp |
| `activity_type` | VARCHAR | e.g. LAUNCH_SERVER, TERMINATE_SERVER |
| `activity_title` | VARCHAR | Human-readable title |
| `source_file_name` | VARCHAR | e.g. activity.csv |
| `source_row_number` | BIGINT | Row index in source file (lineage) |
| `loaded_at` | TIMESTAMP | Ingestion time |

---

### core.fact_points_ledger

**Purpose:** Immutable accounting ledger of all point movements (credits and debits).

| Column | Type | Description |
|--------|------|-------------|
| `ledger_entry_id` | BIGINT (PK) | Surrogate key |
| `user_id` | VARCHAR | User account |
| `ledger_ts` | TIMESTAMP | Transaction timestamp |
| `entry_type` | VARCHAR | PURCHASE_TOP_UP, BALANCE_CREDIT, USAGE_REDEEM, MOBILE_VERIFICATION_BONUS, PROMO_CODE_BONUS, PROMO_RELOAD_BONUS |
| `points_delta` | BIGINT | Signed: positive = credit, negative = deduction (e.g. REDEEM) |
| `points_source` | VARCHAR | paid (payment.csv only), free_claim, consumption, balance_credit (point.csv TOP_UP; source unknown) |
| `related_cash_amount_sgd` | DECIMAL(12,2) | Cash amount when applicable (e.g. top-up) |
| `related_cash_currency` | VARCHAR | e.g. SGD |
| `promo_type` | VARCHAR | Promo/campaign name when applicable |
| `source_file_name` | VARCHAR | Source CSV |
| `source_row_number` | BIGINT | Source row (lineage) |
| `notes` | VARCHAR | Optional validation/parsing notes |
| `loaded_at` | TIMESTAMP | Ingestion time |

---

### core.fact_payment_topup

**Purpose:** Monetary top-up transactions (payments that credit points).

| Column | Type | Description |
|--------|------|-------------|
| `payment_id` | BIGINT (PK) | Surrogate key |
| `user_id` | VARCHAR | Payer |
| `payment_ts` | TIMESTAMP | Payment time |
| `purpose` | VARCHAR | e.g. PURCHASE_CREDIT |
| `cash_amount_sgd` | DECIMAL(12,2) | Amount in SGD |
| `points_credited` | BIGINT | Points granted |
| `points_per_sgd` | DECIMAL(12,2) | Expected ratio (144) |
| `package_code` | VARCHAR | e.g. SGD50 |
| `source_file_name` | VARCHAR | Source CSV |
| `source_row_number` | BIGINT | Source row |
| `loaded_at` | TIMESTAMP | Ingestion time |

---

### core.user_account_state

**Purpose:** Computed per-user snapshot: balance, spend, and VPS live count. Refreshed each batch from core facts.

| Column | Type | Description |
|--------|------|-------------|
| `user_id` | VARCHAR (PK) | User |
| `as_of_ts` | TIMESTAMP | Snapshot computation time |
| `current_points_balance` | BIGINT | Sum of all points_delta in ledger |
| `total_points_earned_paid` | BIGINT | Sum of positive deltas where points_source = paid (payment.csv only; actual monetary purchases) |
| `total_points_earned_free` | BIGINT | Sum of positive deltas where points_source = free_claim |
| `total_points_spent` | BIGINT | Sum of absolute negative deltas (consumption) |
| `total_launch_count` | BIGINT | Count of LAUNCH_SERVER in activity |
| `total_terminate_count` | BIGINT | Count of TERMINATE_SERVER in activity |
| `current_vps_live` | BIGINT | total_launch_count - total_terminate_count |
| `last_activity_ts` | TIMESTAMP | Latest activity event |
| `last_ledger_ts` | TIMESTAMP | Latest ledger event |
| `updated_at` | TIMESTAMP | Last refresh time |

---

### mart.user_daily_activity

**Purpose:** Per-user daily activity counts.

| Column | Type | Description |
|--------|------|-------------|
| `activity_date` | DATE | Day (with user_id = PK component) |
| `user_id` | VARCHAR | User |
| `total_events` | BIGINT | Total activity events that day |
| `launch_count` | BIGINT | LAUNCH_SERVER count |
| `terminate_count` | BIGINT | TERMINATE_SERVER count |
| `reboot_count` | BIGINT | REBOOT_SERVER count |
| `suspend_count` | BIGINT | SUSPEND_SERVER count |
| `other_activity_count` | BIGINT | Other activity types |

---

### mart.user_daily_points

**Purpose:** Per-user daily points movement and top-up summary.

| Column | Type | Description |
|--------|------|-------------|
| `activity_date` | DATE | Day |
| `user_id` | VARCHAR | User |
| `points_earned_paid` | BIGINT | Points from paid top-up that day (payment.csv only) |
| `points_earned_free` | BIGINT | Points from free/promo that day |
| `points_spent` | BIGINT | Points consumed (REDEEM) that day |
| `net_points_delta` | BIGINT | Net change that day |
| `end_of_day_balance` | BIGINT | Running balance at end of day |
| `topup_count` | BIGINT | Number of top-ups that day |
| `topup_sum_sgd` | DECIMAL(12,2) | Sum of cash top-up that day |

---

### mart.platform_daily_overview

**Purpose:** One row per day: app-level KPIs for dashboards.

| Column | Type | Description |
|--------|------|-------------|
| `activity_date` | DATE (PK) | Day |
| `new_signups` | BIGINT | New registrations that day |
| `mobile_verified_new` | BIGINT | New mobile verifications that day |
| `active_users` | BIGINT | Distinct users with activity that day |
| `new_vps_created` | BIGINT | LAUNCH_SERVER count that day |
| `vps_terminated` | BIGINT | TERMINATE_SERVER count that day |
| `net_vps_change` | BIGINT | new_vps_created - vps_terminated |
| `topups_count` | BIGINT | Number of top-up transactions |
| `topups_sum_sgd` | DECIMAL(12,2) | Sum of top-up revenue (SGD) |
| `payer_count` | BIGINT | Distinct users who topped up that day |
| `points_earned_paid` | BIGINT | Points from paid top-up (payment.csv only) |
| `points_earned_free` | BIGINT | Points from free/promo |
| `points_spent` | BIGINT | Points consumed |
| `net_points_delta` | BIGINT | Net platform points change |

---

### mart.platform_weekly_overview

**Purpose:** Same metrics as platform_daily_overview aggregated by week (week_start_date).

| Column | Type | Description |
|--------|------|-------------|
| `week_start_date` | DATE (PK) | Start of week |
| (same KPI columns as platform_daily_overview) | | Sums over the week |

---

### Relationships (User Logs)

- `core.dim_user.user_id` is the parent dimension.
- `core.fact_user_activity`, `core.fact_points_ledger`, and `core.fact_payment_topup` reference `user_id`.
- `core.user_account_state` is 1:1 with `core.dim_user` and is derived from activity + ledger.
- Mart tables are derived from core (no direct CSV load).

### Computation definitions

- **current_points_balance:** `SUM(points_delta)` over `core.fact_points_ledger` per user.
- **total_points_spent:** Sum of `-points_delta` where `points_delta < 0` per user.
- **current_vps_live:** `SUM(CASE WHEN activity_type = 'LAUNCH_SERVER' THEN 1 WHEN activity_type = 'TERMINATE_SERVER' THEN -1 ELSE 0 END)` per user from `core.fact_user_activity`.

### User Logs ETL flow and idempotency

1. **Create schemas/tables:** `CREATE SCHEMA IF NOT EXISTS core/mart/ref` and `CREATE TABLE IF NOT EXISTS` for all tables.
2. **Load core from CSVs:** For each file, delete existing rows where `source_file_name = '<filename>'`, then insert all rows from the CSV (normalized headers, same file names required). Order: customer → mobile_verified update → activity → payment → point → redeem-mobile-verification → redeem-promocode (excluding dev_testing%) → redeem-reload.
3. **Refresh core.user_account_state:** Recompute from `fact_points_ledger` and `fact_user_activity` (full replace).
4. **Refresh marts:** Truncate and repopulate `mart.user_daily_activity`, `mart.user_daily_points`, `mart.platform_daily_overview`, `mart.platform_weekly_overview` from core.

Re-running the ETL with the same CSV set replaces that file’s data and recomputes state and marts; no duplicate ledger rows for the same source file.

### User Logs query examples

**Daily platform overview (last 7 days):**
```sql
SELECT * FROM mart.platform_daily_overview
WHERE activity_date >= current_date - 7
ORDER BY activity_date DESC;
```

**Per-user balance and source breakdown:**
```sql
SELECT user_id, current_points_balance, total_points_earned_paid, total_points_earned_free, total_points_spent, current_vps_live
FROM core.user_account_state
ORDER BY current_points_balance DESC
LIMIT 20;
```

**Users with most live VPS and top-ups:**
```sql
SELECT u.user_id, u.registration_ts, s.current_vps_live, s.total_points_earned_paid, s.current_points_balance
FROM core.dim_user u
JOIN core.user_account_state s ON u.user_id = s.user_id
WHERE s.current_vps_live > 0 OR s.total_points_earned_paid > 0
ORDER BY s.current_vps_live DESC, s.total_points_earned_paid DESC
LIMIT 20;
```

---

## GA4 Tables (Google Analytics 4)

| Table Name | Rows | Purpose |
|------------|------|---------|
| `ga4_sessions` | 278 | Basic daily session metrics |
| `ga4_traffic_overview` | 4,153 | Traffic by source/medium/campaign |
| `ga4_page_performance` | 6,138 | Page-level content performance |
| `ga4_geographic_data` | 10,000 | Geographic breakdown (country/region/city) |
| `ga4_technology_data` | 3,741 | Device, OS, browser breakdown |
| `ga4_event_data` | 2,358 | Custom event tracking |

---

### ga4_sessions

**Purpose:** Basic daily session totals for quick monitoring.

| Column | Type | Description |
|--------|------|-------------|
| `date` | VARCHAR | Date (YYYYMMDD format) |
| `sessions` | VARCHAR | Total sessions |
| `totalUsers` | VARCHAR | Total users |
| `newUsers` | VARCHAR | New users |
| `bounceRate` | VARCHAR | Bounce rate (0-1) |

---

### ga4_traffic_overview

**Purpose:** Traffic analysis by source, medium, and campaign.

| Column | Type | Description |
|--------|------|-------------|
| `date` | VARCHAR | Date (YYYYMMDD format) |
| `sessionSource` | VARCHAR | Traffic source (google, facebook, direct, etc.) |
| `sessionMedium` | VARCHAR | Traffic medium (organic, cpc, referral, etc.) |
| `sessionCampaignName` | VARCHAR | Campaign name |
| `sessions` | VARCHAR | Session count |
| `totalUsers` | VARCHAR | Total users |
| `newUsers` | VARCHAR | New users |
| `bounceRate` | VARCHAR | Bounce rate |
| `screenPageViews` | VARCHAR | Page views |

---

### ga4_page_performance

**Purpose:** Page-level content analysis.

| Column | Type | Description |
|--------|------|-------------|
| `date` | VARCHAR | Date (YYYYMMDD format) |
| `pagePath` | VARCHAR | URL path (/page/subpage) |
| `pageTitle` | VARCHAR | Page title |
| `screenPageViews` | VARCHAR | Page views |
| `sessions` | VARCHAR | Sessions |
| `bounceRate` | VARCHAR | Bounce rate |
| `averageSessionDuration` | VARCHAR | Avg session duration (seconds) |

---

### ga4_geographic_data

**Purpose:** Geographic breakdown by country, region, and city.

| Column | Type | Description |
|--------|------|-------------|
| `date` | VARCHAR | Date (YYYYMMDD format) |
| `country` | VARCHAR | Country name |
| `region` | VARCHAR | State/province/region |
| `city` | VARCHAR | City name |
| `sessions` | VARCHAR | Sessions |
| `totalUsers` | VARCHAR | Total users |
| `newUsers` | VARCHAR | New users |

---

### ga4_technology_data

**Purpose:** Device, browser, and OS analysis.

| Column | Type | Description |
|--------|------|-------------|
| `date` | VARCHAR | Date (YYYYMMDD format) |
| `deviceCategory` | VARCHAR | Device type (desktop, mobile, tablet) |
| `operatingSystem` | VARCHAR | OS name (Windows, iOS, Android, etc.) |
| `browser` | VARCHAR | Browser name (Chrome, Safari, etc.) |
| `sessions` | VARCHAR | Sessions |
| `totalUsers` | VARCHAR | Total users |
| `screenPageViews` | VARCHAR | Page views |

---

### ga4_event_data

**Purpose:** Custom event tracking and user interactions.

| Column | Type | Description |
|--------|------|-------------|
| `date` | VARCHAR | Date (YYYYMMDD format) |
| `eventName` | VARCHAR | Event name (page_view, click, scroll, etc.) |
| `eventCount` | VARCHAR | Number of event occurrences |
| `totalUsers` | VARCHAR | Users who triggered the event |

---

## GSC Tables (Google Search Console)

| Table Name | Rows | Purpose |
|------------|------|---------|
| `gsc_queries` | 45,218 | Search keywords over time |
| `gsc_pages` | 8,992 | Page-level SEO performance |
| `gsc_countries` | 15,513 | Geographic search performance |
| `gsc_devices` | 671 | Device-specific SEO metrics |
| `gsc_query_page` | 5,680 | Query to page mapping |
| `gsc_query_country` | 13,353 | Query by country |
| `gsc_query_device` | 3,419 | Query by device |
| `gsc_page_country` | 3,397 | Page by country |
| `gsc_page_device` | 372 | Page by device |
| `gsc_daily_totals` | 258 | Daily aggregate totals |

---

### gsc_queries

**Purpose:** Search query/keyword performance over time.

| Column | Type | Description |
|--------|------|-------------|
| `_dataset` | VARCHAR | Dataset identifier |
| `date` | VARCHAR | Date (YYYY-MM-DD) |
| `query` | VARCHAR | Search query/keyword |
| `clicks` | BIGINT | Total clicks |
| `impressions` | BIGINT | Total impressions |
| `ctr` | DOUBLE | Click-through rate (0-1) |
| `position` | DOUBLE | Average search position |

---

### gsc_pages

**Purpose:** Page-level organic search performance.

| Column | Type | Description |
|--------|------|-------------|
| `_dataset` | VARCHAR | Dataset identifier |
| `date` | VARCHAR | Date (YYYY-MM-DD) |
| `page` | VARCHAR | Page URL |
| `clicks` | BIGINT | Total clicks |
| `impressions` | BIGINT | Total impressions |
| `ctr` | DOUBLE | Click-through rate |
| `position` | DOUBLE | Average position |

---

### gsc_countries

**Purpose:** Search performance by country.

| Column | Type | Description |
|--------|------|-------------|
| `_dataset` | VARCHAR | Dataset identifier |
| `date` | VARCHAR | Date (YYYY-MM-DD) |
| `country` | VARCHAR | Country code (3-letter: usa, sgp, etc.) |
| `clicks` | BIGINT | Total clicks |
| `impressions` | BIGINT | Total impressions |
| `ctr` | DOUBLE | Click-through rate |
| `position` | DOUBLE | Average position |

---

### gsc_devices

**Purpose:** Search performance by device type.

| Column | Type | Description |
|--------|------|-------------|
| `_dataset` | VARCHAR | Dataset identifier |
| `date` | VARCHAR | Date (YYYY-MM-DD) |
| `device` | VARCHAR | Device type (DESKTOP, MOBILE, TABLET) |
| `clicks` | BIGINT | Total clicks |
| `impressions` | BIGINT | Total impressions |
| `ctr` | DOUBLE | Click-through rate |
| `position` | DOUBLE | Average position |

---

### gsc_query_page

**Purpose:** Query to page mapping (which queries drive which pages).

| Column | Type | Description |
|--------|------|-------------|
| `_dataset` | VARCHAR | Dataset identifier |
| `query` | VARCHAR | Search query |
| `page` | VARCHAR | Page URL |
| `clicks` | BIGINT | Total clicks |
| `impressions` | BIGINT | Total impressions |
| `ctr` | DOUBLE | Click-through rate |
| `position` | DOUBLE | Average position |

---

### gsc_query_country

**Purpose:** Query performance by country.

| Column | Type | Description |
|--------|------|-------------|
| `_dataset` | VARCHAR | Dataset identifier |
| `query` | VARCHAR | Search query |
| `country` | VARCHAR | Country code |
| `clicks` | BIGINT | Total clicks |
| `impressions` | BIGINT | Total impressions |
| `ctr` | DOUBLE | Click-through rate |
| `position` | DOUBLE | Average position |

---

### gsc_query_device

**Purpose:** Query performance by device.

| Column | Type | Description |
|--------|------|-------------|
| `_dataset` | VARCHAR | Dataset identifier |
| `query` | VARCHAR | Search query |
| `device` | VARCHAR | Device type |
| `clicks` | BIGINT | Total clicks |
| `impressions` | BIGINT | Total impressions |
| `ctr` | DOUBLE | Click-through rate |
| `position` | DOUBLE | Average position |  

---

### gsc_page_country

**Purpose:** Page performance by country.

| Column | Type | Description |
|--------|------|-------------|
| `_dataset` | VARCHAR | Dataset identifier |
| `page` | VARCHAR | Page URL |
| `country` | VARCHAR | Country code |
| `clicks` | BIGINT | Total clicks |
| `impressions` | BIGINT | Total impressions |
| `ctr` | DOUBLE | Click-through rate |
| `position` | DOUBLE | Average position |

---

### gsc_page_device

**Purpose:** Page performance by device.

| Column | Type | Description |
|--------|------|-------------|
| `_dataset` | VARCHAR | Dataset identifier |
| `page` | VARCHAR | Page URL |
| `device` | VARCHAR | Device type |
| `clicks` | BIGINT | Total clicks |
| `impressions` | BIGINT | Total impressions |
| `ctr` | DOUBLE | Click-through rate |
| `position` | DOUBLE | Average position |

---

### gsc_daily_totals

**Purpose:** Daily aggregate totals across all queries/pages.

| Column | Type | Description |
|--------|------|-------------|
| `_dataset` | VARCHAR | Dataset identifier |
| `date` | VARCHAR | Date (YYYY-MM-DD) |
| `clicks` | BIGINT | Total daily clicks |
| `impressions` | BIGINT | Total daily impressions |
| `ctr` | DOUBLE | Daily click-through rate |
| `position` | DOUBLE | Daily average position |

---

## Google Ads Tables

| Table Name | Rows | Purpose |
|------------|------|---------|
| `gads_daily_summary` | 273 | Daily account-level totals |
| `gads_campaigns` | 273 | Campaign performance |
| `gads_ad_groups` | 857 | Ad group performance |
| `gads_keywords` | 930 | Keyword performance |
| `gads_ads` | 857 | Individual ad metrics |
| `gads_devices` | 655 | Performance by device |
| `gads_geographic` | 273 | Geographic performance |
| `gads_hourly` | 4,543 | Hour-by-hour performance |
| `gads_conversions` | 349 | Conversion action data |

---

### gads_campaigns

**Purpose:** Campaign-level performance metrics.

| Column | Type | Description |
|--------|------|-------------|
| `campaign_id` | BIGINT | Campaign ID (PK) |
| `campaign_name` | VARCHAR | Campaign name |
| `campaign_status` | VARCHAR | Status (ENABLED, PAUSED, REMOVED) |
| `campaign_type` | VARCHAR | Type (SEARCH, DISPLAY, VIDEO, etc.) |
| `date` | VARCHAR | Date (YYYY-MM-DD) |
| `impressions` | BIGINT | Ad impressions |
| `clicks` | BIGINT | Ad clicks |
| `cost_micros` | BIGINT | Cost in micros (÷1,000,000 for actual) |
| `cost` | DOUBLE | Cost in currency units |
| `ctr` | DOUBLE | Click-through rate |
| `average_cpc_micros` | DOUBLE | Avg CPC in micros |
| `average_cpc` | DOUBLE | Avg CPC in currency |
| `average_cpm_micros` | DOUBLE | Avg CPM in micros |
| `average_cpm` | BIGINT | Avg CPM |
| `conversions` | DOUBLE | Conversions |
| `conversions_value` | DOUBLE | Conversion value |
| `all_conversions` | DOUBLE | All conversions (inc. cross-device) |
| `all_conversions_value` | DOUBLE | All conversions value |
| `conversion_rate` | DOUBLE | Conversion rate |
| `view_through_conversions` | BIGINT | View-through conversions |
| `interactions` | BIGINT | Total interactions |
| `interaction_rate` | DOUBLE | Interaction rate |
| `engagement_rate` | DOUBLE | Engagement rate |
| `video_views` | BIGINT | Video views |
| `video_quartile_p25_rate` | DOUBLE | Video 25% watched rate |
| `video_quartile_p50_rate` | DOUBLE | Video 50% watched rate |
| `video_quartile_p75_rate` | DOUBLE | Video 75% watched rate |
| `video_quartile_p100_rate` | DOUBLE | Video 100% watched rate |
| `search_impression_share` | DOUBLE | Search impression share |
| `search_rank_lost_impression_share` | DOUBLE | Lost IS (rank) |
| `search_budget_lost_impression_share` | DOUBLE | Lost IS (budget) |
| `average_position` | INTEGER | Average position (deprecated) |
| `top_impression_percentage` | DOUBLE | Top impression % |
| `absolute_top_impression_percentage` | DOUBLE | Absolute top impression % |

---

### gads_ad_groups

**Purpose:** Ad group-level performance metrics.

| Column | Type | Description |
|--------|------|-------------|
| `campaign_id` | BIGINT | Parent campaign ID |
| `campaign_name` | VARCHAR | Campaign name |
| `campaign_status` | INTEGER | Campaign status code |
| `campaign_type` | INTEGER | Campaign type code |
| `ad_group_id` | BIGINT | Ad group ID (PK) |
| `ad_group_name` | VARCHAR | Ad group name |
| `ad_group_status` | VARCHAR | Status |
| `ad_group_type` | VARCHAR | Type |
| `date` | VARCHAR | Date |
| `impressions` | BIGINT | Impressions |
| `clicks` | BIGINT | Clicks |
| `cost_micros` | BIGINT | Cost (micros) |
| `cost` | DOUBLE | Cost |
| `ctr` | DOUBLE | CTR |
| `average_cpc_micros` | DOUBLE | Avg CPC (micros) |
| `average_cpc` | DOUBLE | Avg CPC |
| `average_cpm_micros` | DOUBLE | Avg CPM (micros) |
| `average_cpm` | BIGINT | Avg CPM |
| `conversions` | DOUBLE | Conversions |
| `conversions_value` | DOUBLE | Conversion value |
| `all_conversions` | DOUBLE | All conversions |
| `all_conversions_value` | DOUBLE | All conversions value |
| `conversion_rate` | DOUBLE | Conversion rate |
| `view_through_conversions` | BIGINT | View-through conversions |
| `interactions` | BIGINT | Interactions |
| `interaction_rate` | DOUBLE | Interaction rate |
| `engagement_rate` | DOUBLE | Engagement rate |
| `video_views` | BIGINT | Video views |
| `video_quartile_p25_rate` | DOUBLE | Video 25% rate |
| `video_quartile_p50_rate` | DOUBLE | Video 50% rate |
| `video_quartile_p75_rate` | DOUBLE | Video 75% rate |
| `video_quartile_p100_rate` | DOUBLE | Video 100% rate |
| `search_impression_share` | DOUBLE | Search IS |
| `search_rank_lost_impression_share` | DOUBLE | Lost IS (rank) |
| `search_budget_lost_impression_share` | DOUBLE | Lost IS (budget) |
| `average_position` | INTEGER | Avg position |
| `top_impression_percentage` | DOUBLE | Top impression % |
| `absolute_top_impression_percentage` | DOUBLE | Absolute top % |

---

### gads_keywords

**Purpose:** Keyword-level performance metrics.

| Column | Type | Description |
|--------|------|-------------|
| `campaign_id` | BIGINT | Campaign ID |
| `campaign_name` | VARCHAR | Campaign name |
| `campaign_status` | INTEGER | Campaign status |
| `campaign_type` | INTEGER | Campaign type |
| `ad_group_id` | BIGINT | Ad group ID |
| `ad_group_name` | VARCHAR | Ad group name |
| `ad_group_status` | INTEGER | Ad group status |
| `ad_group_type` | INTEGER | Ad group type |
| `keyword_id` | BIGINT | Keyword criterion ID (PK) |
| `keyword_text` | VARCHAR | Keyword text |
| `keyword_match_type` | VARCHAR | Match type (BROAD, PHRASE, EXACT) |
| `keyword_status` | VARCHAR | Keyword status |
| `date` | VARCHAR | Date |
| `impressions` | BIGINT | Impressions |
| `clicks` | BIGINT | Clicks |
| `cost_micros` | BIGINT | Cost (micros) |
| `cost` | DOUBLE | Cost |
| `ctr` | DOUBLE | CTR |
| `average_cpc_micros` | DOUBLE | Avg CPC (micros) |
| `average_cpc` | DOUBLE | Avg CPC |
| `average_cpm_micros` | DOUBLE | Avg CPM (micros) |
| `average_cpm` | BIGINT | Avg CPM |
| `conversions` | DOUBLE | Conversions |
| `conversions_value` | DOUBLE | Conversion value |
| `all_conversions` | DOUBLE | All conversions |
| `all_conversions_value` | DOUBLE | All conversions value |
| `conversion_rate` | DOUBLE | Conversion rate |
| `view_through_conversions` | BIGINT | View-through conversions |
| `interactions` | BIGINT | Interactions |
| `interaction_rate` | DOUBLE | Interaction rate |
| `engagement_rate` | DOUBLE | Engagement rate |
| `video_views` | BIGINT | Video views |
| `video_quartile_p25_rate` | DOUBLE | Video 25% rate |
| `video_quartile_p50_rate` | DOUBLE | Video 50% rate |
| `video_quartile_p75_rate` | DOUBLE | Video 75% rate |
| `video_quartile_p100_rate` | DOUBLE | Video 100% rate |
| `search_impression_share` | DOUBLE | Search IS |
| `search_rank_lost_impression_share` | DOUBLE | Lost IS (rank) |
| `search_budget_lost_impression_share` | DOUBLE | Lost IS (budget) |
| `average_position` | INTEGER | Avg position |
| `top_impression_percentage` | DOUBLE | Top impression % |
| `absolute_top_impression_percentage` | DOUBLE | Absolute top % |

---

### gads_ads

**Purpose:** Individual ad performance metrics.

| Column | Type | Description |
|--------|------|-------------|
| `campaign_id` | BIGINT | Campaign ID |
| `campaign_name` | VARCHAR | Campaign name |
| `campaign_status` | INTEGER | Campaign status |
| `campaign_type` | INTEGER | Campaign type |
| `ad_group_id` | BIGINT | Ad group ID |
| `ad_group_name` | VARCHAR | Ad group name |
| `ad_group_status` | INTEGER | Ad group status |
| `ad_group_type` | INTEGER | Ad group type |
| `ad_id` | BIGINT | Ad ID (PK) |
| `ad_status` | VARCHAR | Ad status |
| `ad_type` | VARCHAR | Ad type |
| `date` | VARCHAR | Date |
| `impressions` | BIGINT | Impressions |
| `clicks` | BIGINT | Clicks |
| `cost_micros` | BIGINT | Cost (micros) |
| `cost` | DOUBLE | Cost |
| `ctr` | DOUBLE | CTR |
| *(+ all standard metrics)* | | |

---

### gads_devices

**Purpose:** Performance breakdown by device type.

| Column | Type | Description |
|--------|------|-------------|
| `campaign_id` | BIGINT | Campaign ID |
| `campaign_name` | VARCHAR | Campaign name |
| `campaign_status` | INTEGER | Campaign status |
| `campaign_type` | INTEGER | Campaign type |
| `date` | VARCHAR | Date |
| `device` | VARCHAR | Device (DESKTOP, MOBILE, TABLET) |
| `impressions` | BIGINT | Impressions |
| `clicks` | BIGINT | Clicks |
| `cost_micros` | BIGINT | Cost (micros) |
| `cost` | DOUBLE | Cost |
| *(+ all standard metrics)* | | |

---

### gads_geographic

**Purpose:** Performance breakdown by geographic location.

| Column | Type | Description |
|--------|------|-------------|
| `campaign_id` | BIGINT | Campaign ID |
| `campaign_name` | VARCHAR | Campaign name |
| `campaign_status` | INTEGER | Campaign status |
| `campaign_type` | INTEGER | Campaign type |
| `date` | VARCHAR | Date |
| `country_criterion_id` | BIGINT | Country criterion ID |
| `location_type` | VARCHAR | Location type |
| `impressions` | BIGINT | Impressions |
| `clicks` | BIGINT | Clicks |
| `cost_micros` | BIGINT | Cost (micros) |
| `cost` | DOUBLE | Cost |
| *(+ all standard metrics)* | | |

---

### gads_hourly

**Purpose:** Hour-by-hour performance breakdown.

| Column | Type | Description |
|--------|------|-------------|
| `campaign_id` | BIGINT | Campaign ID |
| `campaign_name` | VARCHAR | Campaign name |
| `campaign_status` | INTEGER | Campaign status |
| `campaign_type` | INTEGER | Campaign type |
| `date` | VARCHAR | Date |
| `hour` | DOUBLE | Hour of day (0-23) |
| `impressions` | BIGINT | Impressions |
| `clicks` | BIGINT | Clicks |
| `cost_micros` | BIGINT | Cost (micros) |
| `cost` | DOUBLE | Cost |
| *(+ all standard metrics)* | | |

---

### gads_conversions

**Purpose:** Conversion action performance.

| Column | Type | Description |
|--------|------|-------------|
| `campaign_id` | BIGINT | Campaign ID |
| `campaign_name` | VARCHAR | Campaign name |
| `campaign_status` | INTEGER | Campaign status |
| `campaign_type` | INTEGER | Campaign type |
| `date` | VARCHAR | Date |
| `conversion_action` | VARCHAR | Conversion action resource |
| `conversion_action_name` | VARCHAR | Conversion action name |
| `impressions` | BIGINT | Impressions |
| `clicks` | BIGINT | Clicks |
| `conversions` | DOUBLE | Conversions |
| `conversions_value` | DOUBLE | Conversion value |
| *(+ all standard metrics)* | | |

---

### gads_daily_summary

**Purpose:** Daily account-level totals.

| Column | Type | Description |
|--------|------|-------------|
| `campaign_id` | BIGINT | Campaign ID |
| `campaign_name` | VARCHAR | Campaign name |
| `campaign_status` | INTEGER | Campaign status |
| `campaign_type` | INTEGER | Campaign type |
| `date` | VARCHAR | Date |
| `impressions` | BIGINT | Total impressions |
| `clicks` | BIGINT | Total clicks |
| `cost_micros` | BIGINT | Total cost (micros) |
| `cost` | DOUBLE | Total cost |
| *(+ all standard metrics)* | | |

---

## Meta Ads Tables

| Table Name | Rows | Purpose |
|------------|------|---------|
| `meta_daily_account` | 41 | Daily account-level metrics |
| `meta_campaigns` | 15 | Campaign metadata |
| `meta_campaign_insights` | 140 | Daily campaign performance |
| `meta_adsets` | 15 | Ad set (targeting) metadata |
| `meta_adset_insights` | 140 | Daily ad set performance |
| `meta_ads` | 159 | Individual ad metadata |
| `meta_ad_insights` | 0 | Daily ad-level performance |
| `meta_geographic` | 3 | Country-level breakdown |
| `meta_devices` | 6 | Device/platform breakdown |
| `meta_demographics` | 18 | Age/gender breakdown |

---

### meta_daily_account

**Purpose:** Daily account-level performance totals.

| Column | Type | Description |
|--------|------|-------------|
| `date` | DATE | Date |
| `ad_account_id` | VARCHAR | Ad account ID (act_XXXX) |
| `impressions` | BIGINT | Total impressions |
| `reach` | BIGINT | Unique reach |
| `clicks` | BIGINT | Total clicks |
| `unique_clicks` | BIGINT | Unique clicks |
| `spend` | DOUBLE | Total spend |
| `ctr` | DOUBLE | Click-through rate |
| `cpc` | DOUBLE | Cost per click |
| `cpm` | DOUBLE | Cost per mille |
| `frequency` | DOUBLE | Average frequency |
| `cost_per_unique_click` | DOUBLE | Cost per unique click |
| `link_clicks` | BIGINT | Link clicks |
| `page_engagement` | BIGINT | Page engagement |
| `post_engagement` | BIGINT | Post engagement |
| `app_installs` | BIGINT | App installs |
| `purchases` | BIGINT | Purchases |
| `leads` | BIGINT | Leads |
| `purchase_value` | DOUBLE | Purchase value |
| `video_p25` | BIGINT | Video 25% watched |
| `video_p50` | BIGINT | Video 50% watched |
| `video_p75` | BIGINT | Video 75% watched |
| `video_p100` | BIGINT | Video 100% watched |
| `extracted_at` | TIMESTAMP | ETL timestamp |

---

### meta_campaigns

**Purpose:** Campaign metadata and configuration.

| Column | Type | Description |
|--------|------|-------------|
| `campaign_id` | VARCHAR | Campaign ID (PK) |
| `ad_account_id` | VARCHAR | Ad account ID |
| `campaign_name` | VARCHAR | Campaign name |
| `status` | VARCHAR | Status (ACTIVE, PAUSED) |
| `effective_status` | VARCHAR | Effective status |
| `objective` | VARCHAR | Campaign objective |
| `buying_type` | VARCHAR | Buying type |
| `daily_budget` | DOUBLE | Daily budget |
| `lifetime_budget` | DOUBLE | Lifetime budget |
| `budget_remaining` | DOUBLE | Remaining budget |
| `created_time` | TIMESTAMP | Creation time |
| `start_time` | TIMESTAMP | Start time |
| `stop_time` | TIMESTAMP | Stop time |
| `extracted_at` | TIMESTAMP | ETL timestamp |

---

### meta_campaign_insights

**Purpose:** Daily campaign-level performance metrics.

| Column | Type | Description |
|--------|------|-------------|
| `date` | DATE | Date |
| `ad_account_id` | VARCHAR | Ad account ID |
| `campaign_id` | VARCHAR | Campaign ID |
| `campaign_name` | VARCHAR | Campaign name |
| `impressions` | BIGINT | Impressions |
| `reach` | BIGINT | Reach |
| `clicks` | BIGINT | Clicks |
| `unique_clicks` | BIGINT | Unique clicks |
| `spend` | DOUBLE | Spend |
| `ctr` | DOUBLE | CTR |
| `cpc` | DOUBLE | CPC |
| `cpm` | DOUBLE | CPM |
| `frequency` | DOUBLE | Frequency |
| `link_clicks` | BIGINT | Link clicks |
| `app_installs` | BIGINT | App installs |
| `purchases` | BIGINT | Purchases |
| `leads` | BIGINT | Leads |
| `purchase_value` | DOUBLE | Purchase value |
| `extracted_at` | TIMESTAMP | ETL timestamp |

---

### meta_adsets

**Purpose:** Ad set metadata with targeting information.

| Column | Type | Description |
|--------|------|-------------|
| `adset_id` | VARCHAR | Ad set ID (PK) |
| `ad_account_id` | VARCHAR | Ad account ID |
| `campaign_id` | VARCHAR | Parent campaign ID |
| `adset_name` | VARCHAR | Ad set name |
| `status` | VARCHAR | Status |
| `effective_status` | VARCHAR | Effective status |
| `optimization_goal` | VARCHAR | Optimization goal |
| `billing_event` | VARCHAR | Billing event |
| `bid_strategy` | VARCHAR | Bid strategy |
| `daily_budget` | DOUBLE | Daily budget |
| `lifetime_budget` | DOUBLE | Lifetime budget |
| `budget_remaining` | DOUBLE | Remaining budget |
| `target_countries` | VARCHAR | Target countries (comma-separated) |
| `created_time` | TIMESTAMP | Creation time |
| `start_time` | TIMESTAMP | Start time |
| `end_time` | TIMESTAMP | End time |
| `extracted_at` | TIMESTAMP | ETL timestamp |

---

### meta_adset_insights

**Purpose:** Daily ad set-level performance metrics.

| Column | Type | Description |
|--------|------|-------------|
| `date` | DATE | Date |
| `ad_account_id` | VARCHAR | Ad account ID |
| `campaign_id` | VARCHAR | Campaign ID |
| `campaign_name` | VARCHAR | Campaign name |
| `adset_id` | VARCHAR | Ad set ID |
| `adset_name` | VARCHAR | Ad set name |
| `impressions` | BIGINT | Impressions |
| `reach` | BIGINT | Reach |
| `clicks` | BIGINT | Clicks |
| `unique_clicks` | BIGINT | Unique clicks |
| `spend` | DOUBLE | Spend |
| `ctr` | DOUBLE | CTR |
| `cpc` | DOUBLE | CPC |
| `cpm` | DOUBLE | CPM |
| `frequency` | DOUBLE | Frequency |
| `link_clicks` | BIGINT | Link clicks |
| `app_installs` | BIGINT | App installs |
| `purchases` | BIGINT | Purchases |
| `leads` | BIGINT | Leads |
| `purchase_value` | DOUBLE | Purchase value |
| `extracted_at` | TIMESTAMP | ETL timestamp |

---

### meta_ads

**Purpose:** Individual ad metadata.

| Column | Type | Description |
|--------|------|-------------|
| `ad_id` | VARCHAR | Ad ID (PK) |
| `ad_account_id` | VARCHAR | Ad account ID |
| `campaign_id` | VARCHAR | Campaign ID |
| `adset_id` | VARCHAR | Ad set ID |
| `ad_name` | VARCHAR | Ad name |
| `status` | VARCHAR | Status |
| `effective_status` | VARCHAR | Effective status |
| `creative_id` | VARCHAR | Creative ID |
| `created_time` | TIMESTAMP | Creation time |
| `extracted_at` | TIMESTAMP | ETL timestamp |

---

### meta_ad_insights

**Purpose:** Daily ad-level performance metrics.

| Column | Type | Description |
|--------|------|-------------|
| `date` | DATE | Date |
| `ad_account_id` | VARCHAR | Ad account ID |
| `campaign_id` | VARCHAR | Campaign ID |
| `campaign_name` | VARCHAR | Campaign name |
| `adset_id` | VARCHAR | Ad set ID |
| `adset_name` | VARCHAR | Ad set name |
| `ad_id` | VARCHAR | Ad ID |
| `ad_name` | VARCHAR | Ad name |
| `impressions` | BIGINT | Impressions |
| `reach` | BIGINT | Reach |
| `clicks` | BIGINT | Clicks |
| `spend` | DOUBLE | Spend |
| `ctr` | DOUBLE | CTR |
| `cpc` | DOUBLE | CPC |
| `cpm` | DOUBLE | CPM |
| `link_clicks` | BIGINT | Link clicks |
| `app_installs` | BIGINT | App installs |
| `purchases` | BIGINT | Purchases |
| `purchase_value` | DOUBLE | Purchase value |
| `extracted_at` | TIMESTAMP | ETL timestamp |

---

### meta_geographic

**Purpose:** Performance breakdown by country.

| Column | Type | Description |
|--------|------|-------------|
| `date_start` | DATE | Period start |
| `date_stop` | DATE | Period end |
| `ad_account_id` | VARCHAR | Ad account ID |
| `country` | VARCHAR | Country code |
| `impressions` | BIGINT | Impressions |
| `reach` | BIGINT | Reach |
| `clicks` | BIGINT | Clicks |
| `spend` | DOUBLE | Spend |
| `ctr` | DOUBLE | CTR |
| `cpc` | DOUBLE | CPC |
| `cpm` | DOUBLE | CPM |
| `app_installs` | BIGINT | App installs |
| `purchases` | BIGINT | Purchases |
| `purchase_value` | DOUBLE | Purchase value |
| `extracted_at` | TIMESTAMP | ETL timestamp |

---

### meta_devices

**Purpose:** Performance breakdown by device and publisher platform.

| Column | Type | Description |
|--------|------|-------------|
| `date_start` | DATE | Period start |
| `date_stop` | DATE | Period end |
| `ad_account_id` | VARCHAR | Ad account ID |
| `device_platform` | VARCHAR | Device (mobile, desktop) |
| `publisher_platform` | VARCHAR | Platform (facebook, instagram, etc.) |
| `impressions` | BIGINT | Impressions |
| `reach` | BIGINT | Reach |
| `clicks` | BIGINT | Clicks |
| `spend` | DOUBLE | Spend |
| `ctr` | DOUBLE | CTR |
| `cpc` | DOUBLE | CPC |
| `cpm` | DOUBLE | CPM |
| `app_installs` | BIGINT | App installs |
| `purchases` | BIGINT | Purchases |
| `extracted_at` | TIMESTAMP | ETL timestamp |

---

### meta_demographics

**Purpose:** Performance breakdown by age and gender.

| Column | Type | Description |
|--------|------|-------------|
| `date_start` | DATE | Period start |
| `date_stop` | DATE | Period end |
| `ad_account_id` | VARCHAR | Ad account ID |
| `age` | VARCHAR | Age bracket (18-24, 25-34, etc.) |
| `gender` | VARCHAR | Gender (male, female, unknown) |
| `impressions` | BIGINT | Impressions |
| `reach` | BIGINT | Reach |
| `clicks` | BIGINT | Clicks |
| `spend` | DOUBLE | Spend |
| `ctr` | DOUBLE | CTR |
| `cpc` | DOUBLE | CPC |
| `cpm` | DOUBLE | CPM |
| `app_installs` | BIGINT | App installs |
| `purchases` | BIGINT | Purchases |
| `extracted_at` | TIMESTAMP | ETL timestamp |

---

## AppsFlyer Tables

| Table Name | Rows | Purpose |
|------------|------|---------|
| `af_daily_sources` | 0 | Daily performance by media source & campaign |
| `af_daily_geo` | 1,046 | Daily performance by country, media source & campaign |

---

### af_daily_sources

**Purpose:** Daily aggregate performance by media source and campaign, per platform (iOS/Android).

| Column | Type | Description |
|--------|------|-------------|
| `date` | DATE | Date (PK) |
| `platform` | VARCHAR | 'ios' or 'android' (PK) |
| `app_id` | VARCHAR | AppsFlyer app identifier (PK) |
| `agency` | VARCHAR | Agency/PMD |
| `media_source` | VARCHAR | Media source, e.g., 'Organic', 'Facebook Ads' (PK) |
| `campaign` | VARCHAR | Campaign name (PK) |
| `impressions` | DOUBLE | Ad impressions |
| `clicks` | DOUBLE | Clicks |
| `ctr` | DOUBLE | Click-through rate |
| `installs` | DOUBLE | App installs |
| `conversion_rate` | DOUBLE | Install conversion rate |
| `sessions` | DOUBLE | App sessions |
| `loyal_users` | DOUBLE | Loyal users (users who return) |
| `loyal_users_per_install` | DOUBLE | Ratio of loyal users to installs |
| `total_revenue` | DOUBLE | Total revenue (SGD) |
| `total_cost` | DOUBLE | Total ad spend (SGD) |
| `roi` | DOUBLE | Return on investment |
| `arpu` | DOUBLE | Average revenue per user |
| `avg_ecpi` | DOUBLE | Average effective cost per install |
| `extracted_at` | TIMESTAMP | ETL timestamp |
| *(dynamic event columns)* | DOUBLE | In-app events (e.g., `user_sign_up_unique_users`) |

---

### af_daily_geo

**Purpose:** Daily aggregate performance by country, media source, and campaign — includes in-app event breakdowns.

| Column | Type | Description |
|--------|------|-------------|
| `date` | DATE | Date (PK) |
| `country` | VARCHAR | ISO country code (PK) |
| `platform` | VARCHAR | 'ios' or 'android' (PK) |
| `app_id` | VARCHAR | AppsFlyer app identifier (PK) |
| `agency` | VARCHAR | Agency/PMD |
| `media_source` | VARCHAR | Media source (PK) |
| `campaign` | VARCHAR | Campaign name (PK) |
| `impressions` | DOUBLE | Ad impressions |
| `clicks` | DOUBLE | Clicks |
| `ctr` | DOUBLE | Click-through rate |
| `installs` | DOUBLE | App installs |
| `conversion_rate` | DOUBLE | Install conversion rate |
| `sessions` | DOUBLE | App sessions |
| `loyal_users` | DOUBLE | Loyal users |
| `loyal_users_per_install` | DOUBLE | Loyal users / installs ratio |
| `total_revenue` | DOUBLE | Revenue (SGD) |
| `total_cost` | DOUBLE | Ad spend (SGD) |
| `roi` | DOUBLE | Return on investment |
| `arpu` | DOUBLE | Average revenue per user |
| `avg_ecpi` | DOUBLE | Average effective cost per install |
| `extracted_at` | TIMESTAMP | ETL timestamp |
| `app_install_unique_users` | DOUBLE | Unique users: app install event |
| `app_install_event_counter` | DOUBLE | Count: app install events |
| `user_sign_up_unique_users` | DOUBLE | Unique users: sign-up |
| `user_sign_up_event_counter` | DOUBLE | Count: sign-up events |
| `screen_view_unique_users` | DOUBLE | Unique users: screen view |
| `screen_view_event_counter` | DOUBLE | Count: screen views |
| `create_instance_unique_users` | DOUBLE | Unique users: create instance |
| `deep_link_opened_unique_users` | DOUBLE | Unique users: deep link opened |
| `deposit_unique_users` | DOUBLE | Unique users: deposit |
| `deposit_event_counter` | DOUBLE | Count: deposit events |
| *(+ additional event columns)* | DOUBLE | Dynamically added per app |

**Note:** In-app event columns are added dynamically during ETL. Different apps may have different event columns depending on what's configured in AppsFlyer.

---

## Data Dictionary

### Cost Conversion

**Google Ads** costs are stored in **micros** (1/1,000,000 of the currency unit):

```sql
-- Convert cost_micros to actual cost
SELECT 
    campaign_name,
    cost_micros / 1000000.0 as cost_sgd,
    cost as cost_direct,  -- Already converted
    clicks,
    (cost_micros / 1000000.0) / NULLIF(clicks, 0) as actual_cpc
FROM gads_campaigns;
```

**Meta Ads** costs are stored in actual currency values (no conversion needed):

```sql
-- Meta costs are already in currency units
SELECT 
    campaign_name,
    spend as cost,
    clicks,
    spend / NULLIF(clicks, 0) as actual_cpc
FROM meta_campaign_insights;
```

### Common Dimension Values

| Dimension | Platform | Example Values |
|-----------|----------|----------------|
| `device` / `deviceCategory` | Google/GA4 | MOBILE, DESKTOP, TABLET |
| `device_platform` | Meta | mobile, desktop |
| `publisher_platform` | Meta | facebook, instagram, audience_network, messenger |
| `campaign_status` / `status` | Both | ENABLED/ACTIVE, PAUSED, REMOVED |
| `age` | Meta | 13-17, 18-24, 25-34, 35-44, 45-54, 55-64, 65+ |
| `gender` | Meta | male, female, unknown |

---

## Query Examples

### Cross-Platform Spend Comparison

```sql
-- Total spend across all platforms
SELECT 'Google Ads' as platform, SUM(cost) as total_spend
FROM gads_daily_summary
UNION ALL
SELECT 'Meta Ads', SUM(spend) FROM meta_daily_account
UNION ALL
SELECT 'Organic (SEO)', 0 FROM gsc_daily_totals LIMIT 1;
```

### Top Keywords (Google Ads)

```sql
SELECT 
    keyword_text,
    keyword_match_type,
    SUM(impressions) as impressions,
    SUM(clicks) as clicks,
    SUM(cost) as spend,
    SUM(conversions) as conversions
FROM gads_keywords
WHERE clicks > 0
GROUP BY keyword_text, keyword_match_type
ORDER BY conversions DESC
LIMIT 20;
```

### Top SEO Queries

```sql
SELECT 
    query,
    SUM(clicks) as clicks,
    SUM(impressions) as impressions,
    AVG(position) as avg_position
FROM gsc_queries
GROUP BY query
ORDER BY clicks DESC
LIMIT 20;
```

### Meta Campaign Performance

```sql
SELECT 
    campaign_name,
    SUM(impressions) as impressions,
    SUM(clicks) as clicks,
    SUM(spend) as spend,
    SUM(app_installs) as installs,
    CASE WHEN SUM(app_installs) > 0 
         THEN SUM(spend) / SUM(app_installs) 
         ELSE 0 END as cost_per_install
FROM meta_campaign_insights
GROUP BY campaign_name
ORDER BY spend DESC;
```

---

## View Layer Architecture

The database implements a **Bronze/Silver/Gold** data architecture using DuckDB views for cleaner querying and cross-platform analysis.

### Layer Overview

| Layer | Purpose | Naming Convention | Example |
|-------|---------|-------------------|---------|
| **Bronze** | Raw tables from ETL | `{platform}_{entity}` | `gads_campaigns` |
| **Silver** | Typed views with standardized columns | `{table}_v` | `gads_campaigns_v` |
| **Gold** | Unified reporting facts | `fact_{domain}_{grain}` | `fact_paid_daily` |
| **Dimensions** | De-duplicated metadata | `dim_{platform}_{entity}` | `dim_gads_campaign` |

### Initializing Views

```bash
# Create all views
python scripts/init_views.py

# Validate grains only
python scripts/init_views.py --validate-only

# Recreate views (drop and recreate)
python scripts/init_views.py --drop
```

### Silver Views

Silver views provide:
- Consistent `date_day` column (DATE type) across all platforms
- Proper numeric types for metrics
- Standardized column names (e.g., `ad_group_id` instead of `adset_id`)
- Platform identifier column

| Platform | Raw Table | Silver View | Key Changes |
|----------|-----------|-------------|-------------|
| GA4 | `ga4_sessions` | `ga4_sessions_v` | `date` → `date_day` (DATE), metrics → numeric |
| GSC | `gsc_queries` | `gsc_queries_v` | `date` → `date_day` (DATE), `position` → `avg_position` |
| Google Ads | `gads_campaigns` | `gads_campaigns_v` | `date` → `date_day` (DATE), cost conversion included |
| Meta Ads | `meta_campaign_insights` | `meta_campaign_insights_v` | `date` → `date_day`, `purchase_value` → `revenue` |

### Gold Fact Views

Unified views for cross-platform reporting:

| View | Description | Use Case |
|------|-------------|----------|
| `fact_paid_daily` | Unified Google Ads + Meta Ads daily metrics | Cross-platform paid spend analysis |
| `fact_paid_adgroup_daily` | Ad group/adset level metrics | Campaign structure analysis |
| `fact_organic_daily` | GSC daily totals | SEO performance tracking |
| `fact_organic_queries` | Query-level organic data | Keyword research |
| `fact_web_daily` | GA4 session aggregates | Website traffic analysis |
| `fact_web_traffic` | Traffic by source/medium | Channel attribution |

### Dimension Views

| View | Description |
|------|-------------|
| `dim_gads_campaign` | Google Ads campaign metadata |
| `dim_gads_ad_group` | Google Ads ad group metadata |
| `dim_gads_keyword` | Google Ads keyword metadata |
| `dim_meta_campaign` | Meta Ads campaign metadata |
| `dim_meta_adset` | Meta Ads ad set metadata |
| `dim_meta_ad` | Meta Ads ad metadata |

### Query Examples with Views

**Cross-Platform Daily Spend:**
```sql
SELECT 
    date_day,
    platform,
    SUM(spend) as total_spend,
    SUM(clicks) as total_clicks
FROM fact_paid_daily
WHERE date_day >= '2025-01-01'
GROUP BY date_day, platform
ORDER BY date_day;
```

**Unified Summary:**
```sql
SELECT * FROM summary_platform_totals;
```

### Grain Definitions

Each table has defined uniqueness constraints (grains):

| Table | Grain (Unique Key) |
|-------|--------------------|
| `gads_daily_summary` | `date`, `campaign_id` |
| `gads_campaigns` | `date`, `campaign_id` |
| `gsc_queries` | `_dataset`, `date`, `query` |
| `meta_campaign_insights` | `date`, `ad_account_id`, `campaign_id` |
| `ga4_traffic_overview` | `date`, `sessionSource`, `sessionMedium`, `sessionCampaignName` |
| `af_daily_sources` | `date`, `platform`, `app_id`, `media_source`, `campaign` |
| `af_daily_geo` | `date`, `country`, `platform`, `app_id`, `media_source`, `campaign` |

### Data Quality

Run grain validation to check for duplicates:

```bash
python scripts/init_views.py --validate-only
```

---

## ETL Process

### ETL Commands

| Data Source | Command | Purpose |
|-------------|---------|---------|
| All Sources | `python scripts/run_etl_unified.py --source all --lifetime` | Full historical data |
| All Sources | `python scripts/run_etl_unified.py --source all --lookback-days 3` | Daily incremental |
| GA4 (comprehensive) | `python scripts/run_etl_unified.py --source ga4 --lifetime --comprehensive` | All GA4 metrics |
| Google Ads | `python scripts/run_etl_unified.py --source gads --lifetime` | Google PPC data |
| Search Console | `python scripts/run_etl_unified.py --source gsc --lifetime` | SEO data |
| Meta Ads | `python scripts/run_etl_unified.py --source meta --lifetime` | Meta/Facebook data |
| AppsFlyer | `python scripts/run_etl_appsflyer.py --lookback-days 30` | Mobile app data |
| AppsFlyer | `python scripts/run_etl_appsflyer.py --lifetime` | Full AppsFlyer history |
| User Logs | `python scripts/run_etl_user_logs.py` | Load CSVs from data/user_logs into core + mart |
| User Logs | `python scripts/run_etl_user_logs.py --dry-run` | Validate CSVs only; no writes |
| User Logs | `python scripts/run_etl_user_logs.py --no-rebuild-marts` | Load core only; skip mart refresh |

### Data Loading Modes

- **Lifetime Mode** (`--lifetime`): Full table replace - use for initial load or rebuilding
- **Incremental Mode** (default): Upsert - updates existing records, preserves history

### Data Freshness

| Source | Delay | Recommended Update | Max History |
|--------|-------|-------------------|-------------|
| GA4 | 24-48 hours | Daily | Unlimited |
| Search Console | 2-3 days | Daily | 16 months |
| Google Ads | Same day | Daily | Unlimited |
| Meta Ads | Same day | Daily | 37 months |
| AppsFlyer | Same day | Daily | ~13 months |
| User Logs | Batch | Daily/weekly | Depends on CSV exports |

### Extracted At Timestamps

All tables now include an `extracted_at` timestamp for data lineage:
- **GA4, GSC, Google Ads**: Added via ETL extractors
- **Meta Ads, Twitter**: Already included
- **User Logs (core)**: Use `loaded_at`, `source_file_name`, and `source_row_number` on fact tables for lineage

---

**Last Updated:** 2026-02-12  
**Database Version:** 6.0 (User Logs warehouse: core + mart schemas)  
**DuckDB Version:** 0.9.0+
