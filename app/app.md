# RS Analytics Dashboard - Comprehensive Documentation

**Version:** 2.0  
**Last Updated:** February 12, 2026  
**Tech Stack:** Streamlit, DuckDB, Plotly, Pandas, scikit-learn

---

## Table of Contents

1. [Overview](#overview)
2. [Getting Started](#getting-started)
3. [Navigation Structure](#navigation-structure)
4. [Dashboard Tabs](#dashboard-tabs)
   - [Executive Dashboard](#1-executive-dashboard-)
   - [App Analytics](#2-app-analytics-)
   - [GA4 Analytics](#3-ga4-analytics-)
   - [AppsFlyer](#4-appsflyer-)
   - [Google Ads (PPC)](#5-google-ads-ppc-)
   - [Search Console (SEO)](#6-search-console-seo-)
   - [Meta Ads](#7-meta-ads-)
   - [Twitter/X](#8-twitterx-)
   - [Behavioral Analysis](#9-behavioral-analysis-)
   - [Forecasting](#10-forecasting-)
   - [Clustering](#11-clustering-)
   - [ETL Control](#12-etl-control-)
   - [Glossary](#13-glossary-)
5. [UI Components](#ui-components)
6. [Metric Definitions](#metric-definitions)
7. [Data Sources](#data-sources)
8. [Common Workflows](#common-workflows)

---

## Overview

RS Analytics is a unified marketing and product analytics dashboard that combines data from multiple sources into a single, actionable interface. It helps answer three fundamental questions:

1. **Are we acquiring the right users?** (Acquisition quality)
2. **Are they engaging with our product?** (Behavior & activation)
3. **Are they converting and paying?** (Monetization)

### Key Features

- **Unified Data Warehouse**: All data stored in DuckDB for fast local queries
- **Cross-Platform Analytics**: Google Ads, Meta Ads, GA4, GSC, AppsFlyer, App User Logs
- **Interactive Visualizations**: Plotly charts with hover details and drill-downs
- **Date Range Flexibility**: Preset and custom date ranges with period comparisons
- **ML-Powered Insights**: Behavioral clustering, churn prediction, intent classification
- **Real-time ETL Control**: Pull fresh data directly from the dashboard

---

## Getting Started

### Running the Dashboard

```bash
# From the project root
streamlit run app/main.py
```

The dashboard opens at `http://localhost:8501`

### First-Time Setup

1. **Configure API credentials** in `.env` file (GA4, GSC, Google Ads, Meta, AppsFlyer)
2. **Run initial ETL** to populate the data warehouse:
   ```bash
   python scripts/run_etl_unified.py --source all --lifetime
   python scripts/run_etl_user_logs.py
   ```
3. **Refresh dashboard** to see the data

---

## Navigation Structure

The sidebar organizes tabs into logical groups:

```
📈 Executive Dashboard          ← Always visible at top

🌐 Web & App
├── 📱 App Analytics           ← User Logs mart data
├── 📊 GA4 Analytics           ← Website behavior
└── 🛩️ AppsFlyer              ← Mobile app installs

📣 Marketing
├── 💰 Google Ads (PPC)        ← Paid search
├── 🔍 Search Console (SEO)    ← Organic search
├── 📘 Meta Ads                ← Facebook/Instagram
└── 🐦 Twitter/X               ← Social ads

🧠 Advanced Analytics
├── 🔄 Behavioral Analysis     ← ML segmentation
├── 📈 Forecasting            ← Churn prediction
└── 🎯 Clustering             ← Intent classification

⚙️ Tools
├── 🔧 ETL Control            ← Data pipeline control
└── 📖 Glossary               ← Metric definitions
```

### Sidebar Features

- **Data Status**: Quick view of row counts per data source (✅/⚠️ indicators)
- **Refresh Button**: Clear cache and reload data
- **Last Refresh Time**: Shows when data was last loaded

---

## Dashboard Tabs

### 1. Executive Dashboard 📈

**Purpose:** High-level KPIs combining all data sources for leadership review.

#### Sections

| Section | Description | Key Metrics |
|---------|-------------|-------------|
| **Mobile Acquisition Funnel** | Clicks → Installs → Sign-ups → Loyal | Conversion rates between stages |
| **Core Health KPIs** | 6 tiles showing critical metrics | Spend, Revenue, ROAS, Conversions, CPA, Installs |
| **Target Tracking** | RAG (Red/Amber/Green) progress bars | % to target with status colors |
| **Channel Contribution** | Performance by marketing channel | Spend, Revenue, ROAS per channel |
| **Trend Reality Check** | 30-day line charts | Spend, Revenue, Conversions over time |
| **What Changed** | Auto-generated insight cards | Week-over-week changes |
| **Risk Signals** | Alerts for anomalies | Budget pacing, ROAS drops, spend spikes |
| **Data Trust** | Data freshness indicators | Last update timestamp per source |

#### UI Elements
- Metric tiles with delta indicators (↑↓)
- Interactive Plotly line/bar charts
- Color-coded RAG status bars
- Alert cards (success/warning/error)

---

### 2. App Analytics 📱

**Purpose:** Comprehensive view of the app's user journey and monetization from User Logs warehouse data.

#### Sections

| Section | Description | Metrics |
|---------|-------------|---------|
| **Conversion Funnel** | Sign Up → Verified → VPS → Paid | Absolute counts, step %, overall %, drop-off |
| **Platform KPIs** | Summary tiles | Revenue, Signups, Verified, VPS, Top-ups |
| **Points Economy** | Points earned/spent analysis | Paid points, Free points, Velocity |
| **Revenue Trends** | Daily revenue charts | Revenue, Transactions, Avg Transaction Value |
| **User Activity** | Engagement trends | Active users, New signups, VPS activity |
| **User Demographics** | Pie charts | Verification %, Paying %, VPS adoption % |
| **Top Users** | Power user tables | By balance, By VPS count |
| **Weekly Overview** | Week-by-week table | All metrics aggregated weekly |

#### Conversion Funnel Details

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Sign Up   │ →  │  Verified   │ →  │ Created VPS │ →  │ Paid Money  │
│   👤 1,000  │    │  📱 650     │    │  🖥️ 200     │    │  💰 80      │
│   100%      │    │  65%        │    │  20%        │    │  8%         │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
                      ↓ -350                ↓ -450            ↓ -120
                   (35% drop)           (69% drop)        (60% drop)
```

**Comparison Mode:** When enabled, shows:
- Previous period counts
- % change in absolute numbers
- Percentage point change in conversion rates
- Auto-generated insights (✅ improvements, ⚠️ declines)

#### Points Economy Metrics

| Metric | Definition | Healthy Range |
|--------|------------|---------------|
| Points Earned (Paid) | From actual monetary top-ups only (payment.csv; 144 pts/SGD) | Higher = more revenue |
| Points Earned (Free) | From promos, bonuses (redeem-*.csv) | Monitor for abuse |
| Points Spent | Consumed for VPS (point.csv REDEEM) | Should correlate with activity |
| Velocity | Spent ÷ Earned × 100% | 50-80% is healthy |

*Note: point.csv TOP_UP is a balance addition only and does not distinguish paid vs free; it is not counted as "Paid".*

#### UI Elements
- Funnel visualization with arrows and drop-off indicators
- Donut charts for demographics
- Dual-axis line/bar charts
- Comparison tables with delta columns
- Expandable metric explainers (ℹ️)

---

### 3. GA4 Analytics 📊

**Purpose:** Website behavior analysis following the mental model: "Are the right people coming? Are they engaging? Where are we losing them?"

#### Sections

| Section | Description | Focus |
|---------|-------------|-------|
| **Executive Summary** | Top-line KPIs | Sessions, New Users, Engagement Rate |
| **Acquisition Quality** | Traffic source analysis | Source/Medium performance, Quality Score |
| **Landing Page Performance** | Page-level analysis | Bounce rate, Opportunity Score |
| **Funnel Health** | Conversion funnel | Drop-off at each step |
| **Behavior & Engagement** | Event tracking | Event counts, Session duration |
| **User Segments** | Segment comparisons | New vs Returning, Mobile vs Desktop |
| **Geo & Device** | Geographic/device breakdown | Country map, Device pie chart |
| **Trend Diagnostics** | Time-series analysis | Sessions, Engagement over time |
| **What Changed** | Auto-insights | Significant changes highlighted |

#### Key Concepts

**Traffic Quality Scoring:**
- ✅ **Winner**: High volume + high quality → Keep investing
- 🚀 **Scale**: Low volume + high quality → Increase spend
- 🔴 **Junk**: High volume + low quality → Reduce or cut
- 🟡 **Monitor**: Medium quality → Optimize landing pages

**Opportunity Score Formula:**
```
Opportunity = Sessions × (Site Avg Engagement - Page Engagement)
```
Higher score = bigger impact if page is improved

---

### 4. AppsFlyer 🛩️

**Purpose:** Mobile app acquisition and engagement analytics with iOS/Android comparison.

#### Sections

| Section | Description |
|---------|-------------|
| **Platform Selector** | Choose iOS, Android, or Both |
| **KPI Tiles** | Side-by-side iOS vs Android metrics |
| **Daily Installs Trend** | Line chart comparing platforms |
| **Top Countries** | Geographic breakdown by installs |
| **Media Source Breakdown** | Performance by acquisition channel |
| **In-App Events Funnel** | Install → Sign-up → Deposit journey |
| **Raw Data Explorer** | Filterable data table |

#### Key Metrics

| Metric | Definition |
|--------|------------|
| Installs | App downloads |
| Loyal Users | Users with 3+ app opens |
| Sessions | App open events |
| Sign-ups | In-app registrations |
| eCPI | Effective Cost Per Install |
| ARPU | Average Revenue Per User |

---

### 5. Google Ads (PPC) 💰

**Purpose:** Paid search advertising performance and optimization.

#### Sections

| Section | Key Metrics |
|---------|-------------|
| **Account Overview** | Spend, Clicks, Impressions, Conversions |
| **Campaign Performance** | By campaign: Spend, ROAS, CPA |
| **Ad Group Analysis** | Drill-down into ad groups |
| **Keyword Performance** | Keyword-level metrics, Match type |
| **Geographic Breakdown** | Performance by location |
| **Device Performance** | Desktop vs Mobile vs Tablet |
| **Hourly Trends** | Performance by hour of day |
| **Conversion Actions** | Breakdown by conversion type |

#### Key Formulas

```
CTR = Clicks ÷ Impressions × 100
CPC = Spend ÷ Clicks
CPA = Spend ÷ Conversions
ROAS = Revenue ÷ Spend
Conversion Rate = Conversions ÷ Clicks × 100
```

---

### 6. Search Console (SEO) 🔍

**Purpose:** Organic search performance and keyword rankings.

#### Sections

| Section | Focus |
|---------|-------|
| **Daily Totals** | Clicks, Impressions, CTR, Position |
| **Query Performance** | Top search queries |
| **Page Performance** | Top landing pages |
| **Country Breakdown** | Geographic search performance |
| **Device Breakdown** | Desktop vs Mobile rankings |
| **Query-Page Mapping** | Which queries drive which pages |

#### Key Metrics

| Metric | Definition | Good Range |
|--------|------------|------------|
| Position | Average ranking in search results | 1-10 is page 1 |
| CTR | Click-through rate | 2-5% typical |
| Impressions | Search appearances | More = more visibility |

---

### 7. Meta Ads 📘

**Purpose:** Facebook and Instagram advertising performance.

#### Sections

| Section | Metrics |
|---------|---------|
| **Account Overview** | Spend, Reach, Clicks, Conversions |
| **Campaign Performance** | By campaign with objective |
| **Ad Set Analysis** | Targeting-level performance |
| **Demographics** | Age/Gender breakdown |
| **Geographic** | Country-level performance |
| **Device/Platform** | Facebook vs Instagram, Mobile vs Desktop |

#### Meta-Specific Metrics

| Metric | Definition |
|--------|------------|
| Reach | Unique users who saw ads |
| Frequency | Average times each user saw ad |
| Link Clicks | Clicks to website |
| App Installs | Mobile app downloads |

---

### 8. Twitter/X 🐦

**Purpose:** Twitter advertising and organic engagement metrics.

#### Sections

| Section | Metrics |
|---------|---------|
| **Daily Overview** | Impressions, Engagements, Tweets |
| **Engagement Breakdown** | Likes, Retweets, Replies, Quotes |
| **Engagement Rate** | Trend over time |

---

### 9. Behavioral Analysis 🔄

**Purpose:** ML-powered customer segmentation using K-Means clustering.

#### How It Works

1. **Feature Collection**: Aggregates data from GA4, Meta demographics, AppsFlyer
2. **Clustering**: K-Means identifies distinct user segments
3. **Persona Inference**: Assigns behavioral labels to clusters

#### Output

| Component | Description |
|-----------|-------------|
| **Cluster Scatter Plot** | PCA-reduced 2D visualization |
| **Persona Cards** | Named segments with top traits |
| **Segment Comparison Table** | Feature values per segment |
| **Feature Importance** | Radar charts showing defining characteristics |

#### Example Personas

- **High-Value Engaged**: High sessions, low bounce, high conversion
- **Mobile-First Browser**: Mobile-dominant, high pageviews, moderate conversion
- **Price-Sensitive Shopper**: High sessions on deal pages, responds to discounts

---

### 10. Forecasting 📈

**Purpose:** Predict which cohorts/channels are at risk of declining engagement.

#### Methodology

1. **Data Preparation**: Aggregates daily engagement metrics by channel
2. **Feature Engineering**: Rolling averages, trends, seasonality
3. **Classification**: Random Forest/Logistic Regression predicts decline risk
4. **Trend Decomposition**: Separates trend, seasonality, residual

#### Output

| Component | Description |
|-----------|-------------|
| **Cohort Risk Scores** | Risk level per channel (High/Medium/Low) |
| **Feature Importance** | What drives predictions |
| **Trend Decomposition** | Underlying patterns |
| **Engagement Forecast** | Projected next-period engagement |

---

### 11. Clustering 🎯

**Purpose:** Classify search queries and keywords by user intent.

#### Intent Categories

| Intent | Description | Example Keywords |
|--------|-------------|------------------|
| **Transactional** | Ready to buy | "buy", "price", "discount", "order" |
| **Commercial Investigation** | Researching options | "best", "review", "vs", "compare" |
| **Informational** | Seeking knowledge | "how to", "what is", "guide" |
| **Navigational** | Finding specific site | "login", "dashboard", brand terms |
| **Local** | Location-based | "near me", "directions", "hours" |

#### Data Sources

- GSC queries (organic search)
- Google Ads keywords (paid search)

#### Output

| Component | Description |
|-----------|-------------|
| **Intent Distribution Pie** | % of queries per intent |
| **Intent vs Performance Scatter** | CTR/Conversions by intent |
| **Top Queries per Intent** | Best performers in each category |
| **Performance Clusters** | Similar keywords grouped |

---

### 12. ETL Control 🔧

**Purpose:** Manual control over data extraction pipelines.

#### Capabilities

| Action | Description |
|--------|-------------|
| **Pull Data** | Trigger ETL for specific source |
| **Date Range** | Specify lookback period |
| **Lifetime Mode** | Full historical reload |
| **View Logs** | Monitor ETL progress |
| **Connection Test** | Verify API credentials |

#### Available ETL Sources

- GA4 (Google Analytics 4)
- GSC (Google Search Console)
- Google Ads
- Meta Ads
- AppsFlyer
- User Logs (CSV import)

---

### 13. Glossary 📖

**Purpose:** Reference page defining all metrics and terminology.

#### Categories

- Core Marketing Metrics (Impressions, Clicks, Sessions, etc.)
- Derived Metrics with Formulas (CTR, CPC, ROAS, etc.)
- Platform Acronyms (GA4, GSC, PPC, etc.)
- AppsFlyer-Specific Terms
- Google Ads Terms
- Meta Ads Terms

Each term includes:
- Definition
- Formula (if applicable)
- Example
- Which platforms use it

---

## UI Components

### Date Range Picker

Available on all tabs with:

| Feature | Description |
|---------|-------------|
| **Presets** | Last 7/14/30/60/90/180/365 days |
| **Custom Range** | Calendar picker for specific dates |
| **Comparison Mode** | Previous Period, WoW, MoM, YoY |
| **Summary Bar** | Shows selected dates and day count |

### Metric Tiles

Standard KPI display with:
- Value (formatted with commas, currency symbols)
- Delta indicator (↑↓ with percentage)
- Tooltip explanation (hover on ⓘ)

### Charts

| Chart Type | Usage |
|------------|-------|
| Line Chart | Trends over time |
| Bar Chart | Comparisons between categories |
| Dual-Axis | Two metrics with different scales |
| Pie/Donut | Proportional breakdowns |
| Scatter Plot | Correlation analysis |
| Choropleth Map | Geographic data |
| Funnel | Conversion steps |

### Tables

- Sortable columns
- Formatted numbers (currency, percentages)
- Conditional formatting (colors)
- Export capability (via Streamlit)

### Expanders

- **ℹ️ Understanding...**: Explains section purpose
- **📊 Data Source Status**: Shows table availability
- **📖 Metric Definitions**: Full glossary inline

---

## Metric Definitions

### Revenue & Cost

| Metric | Formula | Description |
|--------|---------|-------------|
| Revenue | Sum of purchase values | Total income generated |
| Spend | Sum of ad costs | Total advertising expense |
| ROAS | Revenue ÷ Spend | Return on Ad Spend |
| CPA | Spend ÷ Conversions | Cost Per Acquisition |
| CPC | Spend ÷ Clicks | Cost Per Click |
| CPM | (Spend ÷ Impressions) × 1000 | Cost Per 1,000 Impressions |
| CPI | Spend ÷ Installs | Cost Per Install |

### Engagement

| Metric | Formula | Description |
|--------|---------|-------------|
| CTR | (Clicks ÷ Impressions) × 100 | Click-Through Rate |
| Bounce Rate | Single-page sessions ÷ Total sessions | Visitors who leave immediately |
| Engagement Rate | (1 - Bounce Rate) | Visitors who engage |
| Pages/Session | Pageviews ÷ Sessions | Content depth |
| Avg Session Duration | Total time ÷ Sessions | Time spent |

### Conversion

| Metric | Formula | Description |
|--------|---------|-------------|
| Conversion Rate | (Conversions ÷ Clicks) × 100 | Visitor to customer rate |
| Install Rate | (Installs ÷ Clicks) × 100 | Click to install rate |
| Sign-up Rate | (Sign-ups ÷ Installs) × 100 | Install to registration |
| Paying Rate | (Payers ÷ Users) × 100 | User to customer conversion |

### App-Specific (User Logs)

| Metric | Formula | Description |
|--------|---------|-------------|
| Points Earned (Paid) | Sum from payment.csv only | Actual monetary purchases; 144 pts/SGD. point.csv TOP_UP is not paid. |
| Points Earned (Free) | Sum of promo points | Bonuses, referrals (redeem-*.csv) |
| Points Spent | Sum of consumed points | Used for VPS (point.csv REDEEM) |
| Points Velocity | (Spent ÷ Earned) × 100 | Consumption rate |
| Net VPS | Created - Terminated | Active server change |
| Verification Rate | (Verified ÷ Signups) × 100 | Phone verification completion |

---

## Data Sources

### Database Structure

All data stored in `data/warehouse.duckdb`:

| Schema | Tables | Purpose |
|--------|--------|---------|
| (default) | ga4_*, gsc_*, gads_*, meta_*, af_*, twitter_* | Raw ETL data |
| core | dim_user, fact_* | User logs source of truth |
| mart | platform_daily_*, user_daily_* | Aggregated reporting |
| ref | dim_points_package | Reference data |

### Data Freshness

| Source | Typical Delay | Update Frequency |
|--------|---------------|------------------|
| GA4 | 24-48 hours | Daily |
| GSC | 2-3 days | Daily |
| Google Ads | Same day | Daily |
| Meta Ads | Same day | Daily |
| AppsFlyer | Same day | Daily |
| User Logs | Batch | Daily/Weekly |

---

## Common Workflows

### Daily Check

1. Open **Executive Dashboard**
2. Check **Risk Signals** for alerts
3. Review **What Changed** cards
4. Drill into specific tabs if anomalies detected

### Weekly Review

1. Set date range to "Last 7 days" with "Previous Period" comparison
2. Review **Executive Dashboard** targets
3. Check **App Analytics** funnel conversion rates
4. Review **GA4 Analytics** traffic quality
5. Check **Channel Contribution** for ROAS by source

### Campaign Launch

1. Note launch date
2. After 3-7 days, compare to pre-launch period
3. Check **Google Ads** or **Meta Ads** for campaign-level data
4. Review **AppsFlyer** for install quality
5. Monitor **App Analytics** funnel for downstream impact

### Investigating a Drop

1. Identify the metric that dropped (Executive Dashboard)
2. Use **What Changed** to see auto-detected changes
3. Drill into relevant tab (GA4, Google Ads, etc.)
4. Check **Geo & Device** for segment-level issues
5. Use **Behavioral Analysis** for deeper segmentation
6. Use **Forecasting** to assess risk level

---

## Support

For questions or issues:
- Check the **Glossary** tab for metric definitions
- Review **ℹ️ Understanding...** expanders on each section
- Run ETL Control connection tests for data issues

---

*Documentation generated for RS Analytics Dashboard v2.0*
