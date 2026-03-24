"""
Glossary Component for RS Analytics

A comprehensive reference page defining technical terms, acronyms, derived metrics,
and platform-specific terminology used throughout the analytics dashboard.

Categories:
- Core Marketing Metrics
- Derived/Calculated Metrics (with formulas)
- Platform Acronyms
- AppsFlyer-Specific Terms
- Google Analytics/Ads Terms
- Meta Ads Terms
- Data Architecture Terms

Usage in other components:
    from app.components.glossary import glossary_link, metric_label
    
    # Create a linked metric label for st.metric
    st.metric(label=metric_label("CPA"), value="$5.00")
    
    # Or use inline glossary link in markdown
    st.markdown(f"Your {glossary_link('CTR')} is above average!")
"""

import streamlit as st
from typing import Optional


# ============================================
# Glossary Link Helpers (for use in other components)
# ============================================

# Quick lookup: term -> short definition (for tooltips)
TERM_TOOLTIPS = {
    # Core metrics
    "Impressions": "Times your ad/content was displayed",
    "Clicks": "Times users clicked your ad/link",
    "Sessions": "User interaction groups (30min timeout)",
    "Users": "Unique visitors to your site/app",
    "New Users": "First-time visitors",
    "Conversions": "Completed valuable actions",
    "Installs": "App downloads",
    "Spend": "Total ad spend",
    "Revenue": "Money generated from conversions",
    "Reach": "Unique users who saw your ad",
    
    # Derived metrics
    "CTR": "Click-Through Rate = Clicks ÷ Impressions",
    "CPC": "Cost Per Click = Spend ÷ Clicks",
    "CPM": "Cost Per 1,000 Impressions",
    "CPA": "Cost Per Acquisition = Spend ÷ Conversions",
    "CPI": "Cost Per Install = Spend ÷ Installs",
    "ROAS": "Return on Ad Spend = Revenue ÷ Spend",
    "ROI": "Return on Investment",
    "Conversion Rate": "Conversions ÷ Clicks × 100",
    "Bounce Rate": "Single-page sessions %",
    "Frequency": "Avg times each user saw your ad",
    "ARPU": "Avg Revenue Per User",
    "Average Position": "Avg ranking in search results",
    
    # AppsFlyer
    "Loyal Users": "Users with 3+ app opens post-install",
    "Loyal Users per Install": "Loyal Users ÷ Installs",
    "Media Source": "Channel that drove the install",
    "Organic": "Non-paid app installs",
    "eCPI": "Effective Cost Per Install",
    "Sign-ups": "Users who registered in-app",
    "Deposits": "Users who made a payment",
    
    # Google Ads
    "Impression Share": "Your impressions ÷ eligible impressions",
    "Quality Score": "Ad relevance rating (1-10)",
    "Match Type": "How closely queries match keywords",
    
    # Meta
    "Ad Set": "Ads sharing targeting/budget settings",
    "Link Clicks": "Clicks to destination URL",
    "Unique Clicks": "Distinct users who clicked",
    
    # App-specific metrics (from app_analytics.py)
    "new_signups": "Total users who completed registration",
    "mobile_verified": "Users who completed mobile phone verification",
    "vps_created": "Total VPS instances launched by users",
    "vps_terminated": "Total VPS instances terminated/deleted",
    "net_vps": "Net change in VPS count (created - terminated)",
    "topups": "Number of payment/top-up transactions",
    "points_earned_paid": "Points earned from paid top-ups",
    "points_earned_free": "Points earned from promos/bonuses/referrals",
    "points_spent": "Points used for VPS or services",
    "points_balance": "Current points balance for user",
    "points_velocity": "Points spent ÷ earned. 50-80% is healthy.",
    "active_users": "Users with at least one action in period",
    "paying_users": "Users who made at least one purchase",
    "live_vps": "Currently active VPS instances",
    
    # Funnel stages
    "Signup → Verified": "Conversion rate from signup to mobile verification",
    "Verified → VPS": "Conversion rate from verified to VPS creation",
    "VPS → Paid": "Conversion rate from VPS user to paying customer",
    "Overall Conversion": "End-to-end signup to paid conversion rate",
    
    # Time-based metrics
    "Peak Day": "Day with highest value in the period",
    "Daily Average": "Average value per day in period",
    "Peak Active Users": "Highest daily active user count",
    "Total Transactions": "Sum of all transactions in period",
    "Avg Transaction Value": "Average revenue per transaction",
    "Unique Payer Days": "Sum of daily unique payers (same user counted per day)",
    
    # Geographic metrics
    "Total Countries": "Number of distinct countries with signups",
    "Top Country": "Country with most signups in period",
    "Top 3 Concentration": "Percentage of signups from top 3 countries",
    
    # Cohort metrics
    "Verification Rate": "% of users who verified mobile",
    "Product Adoption": "% of users who created a VPS",
    "Retention metric": "Metric used to measure user retention (active/paid)",
    
    # Other
    "Engagement Rate": "Interactions ÷ Impressions",
    "Avg CTR": "Average Click-Through Rate",
    "Conv. Value": "Total value of conversions",
    "Cost/Install": "Cost Per Install",
    "Paid Spend": "Total advertising spend across all channels",
}


def glossary_link(term: str, display_text: Optional[str] = None) -> str:
    """
    Generate a markdown link to the glossary with a tooltip.
    
    Use in st.markdown() calls to create clickable terms that
    show a tooltip on hover and link to the glossary.
    
    Args:
        term: The glossary term (must exist in TERM_TOOLTIPS)
        display_text: Optional display text (defaults to term)
    
    Returns:
        HTML string with tooltip and link styling
    
    Example:
        st.markdown(f"Your {glossary_link('CTR')} improved!")
    """
    display = display_text or term
    tooltip = TERM_TOOLTIPS.get(term, term)
    
    # Use HTML with title attribute for native tooltip
    # The link navigates to Glossary page (handled by Streamlit navigation)
    return (
        f'<span title="{tooltip}" style="border-bottom:1px dotted #888;'
        f'cursor:help;color:inherit;">{display}</span>'
    )


def metric_label(term: str, emoji: str = "", include_tooltip: bool = True) -> str:
    """
    Generate a metric label with optional emoji and tooltip indicator.
    
    For use as the `label` parameter in st.metric(). Adds a small "?" 
    indicator to show the term has a glossary definition.
    
    Args:
        term: The metric term (should exist in TERM_TOOLTIPS)
        emoji: Optional emoji prefix (e.g., "💰")
        include_tooltip: If True, adds a "ⓘ" indicator
    
    Returns:
        Formatted label string
    
    Example:
        st.metric(label=metric_label("CPA", "📉"), value="$5.00")
    """
    indicator = " ⓘ" if include_tooltip and term in TERM_TOOLTIPS else ""
    if emoji:
        return f"{emoji} {term}{indicator}"
    return f"{term}{indicator}"


def render_metric_with_help(
    label: str,
    value: str,
    delta: Optional[str] = None,
    delta_color: str = "normal",
    help_term: Optional[str] = None,
) -> None:
    """
    Render a st.metric with an accompanying help tooltip.
    
    Wraps st.metric and adds a help icon that shows the glossary
    definition when hovered.
    
    Args:
        label: Metric label
        value: Metric value
        delta: Optional delta value
        delta_color: Delta color mode ("normal", "inverse", "off")
        help_term: Glossary term for help text (defaults to label without emoji)
    """
    # Extract term from label (remove emoji prefix if present)
    term = help_term or label.split(" ", 1)[-1] if " " in label else label
    tooltip = TERM_TOOLTIPS.get(term, None)
    
    st.metric(
        label=label,
        value=value,
        delta=delta,
        delta_color=delta_color,
        help=tooltip,  # Streamlit's built-in help tooltip
    )


# ============================================
# Glossary Data Definitions
# ============================================

GLOSSARY_DATA = {
    # ─────────────────────────────────────────
    # Core Marketing Metrics
    # ─────────────────────────────────────────
    "Core Marketing Metrics": [
        {
            "term": "Impressions",
            "definition": "The number of times your ad or content was displayed to users. One user can generate multiple impressions if they see the same ad multiple times.",
            "formula": None,
            "example": "If your ad appears 1,000 times in search results, you have 1,000 impressions.",
            "platforms": ["Google Ads", "Meta Ads", "GSC", "AppsFlyer"],
        },
        {
            "term": "Clicks",
            "definition": "The number of times users clicked on your ad, link, or search result. Indicates user interest and intent.",
            "formula": None,
            "example": "100 clicks on a search ad means 100 users actively engaged with your ad.",
            "platforms": ["Google Ads", "Meta Ads", "GSC", "AppsFlyer"],
        },
        {
            "term": "Sessions",
            "definition": "A group of user interactions with your website/app within a given time frame (default 30 minutes of inactivity ends a session). One user can have multiple sessions.",
            "formula": None,
            "example": "A user visits your site in the morning and again in the evening = 2 sessions, 1 user.",
            "platforms": ["GA4", "AppsFlyer"],
        },
        {
            "term": "Users",
            "definition": "Unique individuals who have visited your website or app within the selected date range. Tracked via cookies or device IDs.",
            "formula": None,
            "example": "1,000 users means 1,000 distinct individuals visited your site.",
            "platforms": ["GA4", "AppsFlyer"],
        },
        {
            "term": "New Users",
            "definition": "Users who are interacting with your site or app for the first time ever (no previous session history).",
            "formula": None,
            "example": "If 800 of 1,000 users have never visited before, you have 800 new users.",
            "platforms": ["GA4"],
        },
        {
            "term": "Conversions",
            "definition": "Completed actions that you define as valuable to your business (e.g., purchases, sign-ups, form submissions, app installs).",
            "formula": None,
            "example": "50 app installs from an ad campaign = 50 conversions (if install is your conversion goal).",
            "platforms": ["Google Ads", "Meta Ads", "GA4"],
        },
        {
            "term": "Installs",
            "definition": "The number of times your mobile app was downloaded and installed on a device. A core mobile acquisition metric.",
            "formula": None,
            "example": "1,000 installs means the app was downloaded 1,000 times.",
            "platforms": ["AppsFlyer", "Meta Ads"],
        },
        {
            "term": "Spend",
            "definition": "The total amount of money spent on advertising during a time period. Also called 'cost' or 'ad spend'.",
            "formula": None,
            "example": "$5,000 spend means you invested $5,000 in ads.",
            "platforms": ["Google Ads", "Meta Ads"],
        },
        {
            "term": "Revenue",
            "definition": "The total monetary value generated from conversions, purchases, or in-app transactions.",
            "formula": None,
            "example": "$10,000 in purchase revenue from users acquired through ads.",
            "platforms": ["Google Ads", "Meta Ads", "GA4"],
        },
        {
            "term": "Reach",
            "definition": "The number of unique users who saw your ad at least once. Unlike impressions, reach counts each person only once.",
            "formula": None,
            "example": "10,000 reach with 30,000 impressions means each person saw the ad ~3 times on average.",
            "platforms": ["Meta Ads"],
        },
    ],

    # ─────────────────────────────────────────
    # Derived / Calculated Metrics
    # ─────────────────────────────────────────
    "Derived Metrics (Formulas)": [
        {
            "term": "CTR (Click-Through Rate)",
            "definition": "The percentage of impressions that resulted in a click. Measures how compelling your ad/content is to users.",
            "formula": "CTR = (Clicks ÷ Impressions) × 100",
            "example": "50 clicks from 1,000 impressions = 5% CTR. Higher CTR generally indicates more relevant/engaging content.",
            "platforms": ["Google Ads", "Meta Ads", "GSC", "AppsFlyer"],
        },
        {
            "term": "CPC (Cost Per Click)",
            "definition": "The average amount you pay each time someone clicks on your ad.",
            "formula": "CPC = Total Spend ÷ Total Clicks",
            "example": "$500 spend for 250 clicks = $2.00 CPC.",
            "platforms": ["Google Ads", "Meta Ads"],
        },
        {
            "term": "CPM (Cost Per Mille)",
            "definition": "The cost to achieve 1,000 impressions. 'Mille' is Latin for thousand. Used for brand awareness campaigns.",
            "formula": "CPM = (Total Spend ÷ Impressions) × 1,000",
            "example": "$100 spend for 50,000 impressions = $2.00 CPM.",
            "platforms": ["Google Ads", "Meta Ads"],
        },
        {
            "term": "CPA (Cost Per Acquisition/Action)",
            "definition": "The average cost to acquire one conversion. The 'acquisition' can be any defined action (install, sign-up, purchase).",
            "formula": "CPA = Total Spend ÷ Total Conversions",
            "example": "$1,000 spend for 50 installs = $20.00 CPA (cost per install).",
            "platforms": ["Google Ads", "Meta Ads", "AppsFlyer"],
        },
        {
            "term": "CPI (Cost Per Install)",
            "definition": "The average cost to acquire one app install. A mobile-specific version of CPA.",
            "formula": "CPI = Total Spend ÷ Total Installs",
            "example": "$2,000 spend for 1,000 installs = $2.00 CPI.",
            "platforms": ["AppsFlyer", "Meta Ads"],
        },
        {
            "term": "ROAS (Return on Ad Spend)",
            "definition": "The revenue generated for every dollar spent on advertising. A key profitability metric.",
            "formula": "ROAS = Revenue ÷ Ad Spend",
            "example": "$5,000 revenue from $1,000 spend = 5.0x ROAS (500% return).",
            "platforms": ["Google Ads", "Meta Ads"],
        },
        {
            "term": "ROI (Return on Investment)",
            "definition": "The profit generated relative to the cost, expressed as a percentage. Accounts for costs beyond ad spend.",
            "formula": "ROI = ((Revenue - Cost) ÷ Cost) × 100",
            "example": "$5,000 revenue, $2,000 total cost = 150% ROI.",
            "platforms": ["AppsFlyer"],
        },
        {
            "term": "Conversion Rate",
            "definition": "The percentage of clicks (or sessions) that result in a conversion.",
            "formula": "Conversion Rate = (Conversions ÷ Clicks) × 100",
            "example": "25 conversions from 500 clicks = 5% conversion rate.",
            "platforms": ["Google Ads", "Meta Ads", "GA4"],
        },
        {
            "term": "Bounce Rate",
            "definition": "The percentage of sessions where users left without any interaction (single-page session with no events).",
            "formula": "Bounce Rate = (Single-Page Sessions ÷ Total Sessions) × 100",
            "example": "300 bounces from 1,000 sessions = 30% bounce rate. Lower is generally better.",
            "platforms": ["GA4"],
        },
        {
            "term": "Frequency",
            "definition": "The average number of times each unique user saw your ad.",
            "formula": "Frequency = Impressions ÷ Reach",
            "example": "30,000 impressions with 10,000 reach = 3.0 frequency (each person saw ad 3 times).",
            "platforms": ["Meta Ads"],
        },
        {
            "term": "ARPU (Average Revenue Per User)",
            "definition": "The average revenue generated per user over a time period.",
            "formula": "ARPU = Total Revenue ÷ Total Users",
            "example": "$10,000 revenue from 5,000 users = $2.00 ARPU.",
            "platforms": ["AppsFlyer"],
        },
        {
            "term": "Average Position",
            "definition": "The average ranking position of your content/ad in search results. Position 1 is the top.",
            "formula": "Weighted average of positions where your result appeared",
            "example": "Average position of 3.5 means you typically appear between positions 3 and 4.",
            "platforms": ["GSC", "Google Ads"],
        },
    ],

    # ─────────────────────────────────────────
    # AppsFlyer-Specific Terms
    # ─────────────────────────────────────────
    "AppsFlyer Mobile Analytics": [
        {
            "term": "Loyal Users",
            "definition": "Users who opened the app at least 3 times after install. Indicates users who found value in the app beyond the initial download.",
            "formula": "Count of users with ≥ 3 app opens post-install",
            "example": "1,000 installs with 400 loyal users = 40% loyalty rate. Higher indicates better user retention.",
            "platforms": ["AppsFlyer"],
        },
        {
            "term": "Loyal Users per Install",
            "definition": "The ratio of loyal users to total installs. A quality metric for user acquisition sources.",
            "formula": "Loyal Users per Install = Loyal Users ÷ Installs",
            "example": "400 loyal users from 1,000 installs = 0.4 loyal users per install.",
            "platforms": ["AppsFlyer"],
        },
        {
            "term": "Media Source",
            "definition": "The advertising network, platform, or channel that drove the app install (e.g., 'Facebook Ads', 'Organic', 'googleadwords_int').",
            "formula": None,
            "example": "'Organic' = user found the app themselves; 'Facebook Ads' = paid Meta campaign drove the install.",
            "platforms": ["AppsFlyer"],
        },
        {
            "term": "Organic (AppsFlyer)",
            "definition": "App installs that occurred without paid advertising attribution — users found the app via app store search, word of mouth, or direct links.",
            "formula": None,
            "example": "High organic installs indicate strong brand awareness or ASO (App Store Optimization).",
            "platforms": ["AppsFlyer"],
        },
        {
            "term": "eCPI (Effective Cost Per Install)",
            "definition": "The actual/effective cost per install after accounting for all costs and attribution. May differ from raw CPI due to attribution windows.",
            "formula": "eCPI = Total Cost ÷ Attributed Installs",
            "example": "If attribution adjusts install counts, eCPI reflects the true cost.",
            "platforms": ["AppsFlyer"],
        },
        {
            "term": "Sign-ups (user_sign_up)",
            "definition": "In-app event tracking when a user completes account registration within the app.",
            "formula": None,
            "example": "500 installs with 200 sign-ups = 40% install-to-signup conversion rate.",
            "platforms": ["AppsFlyer"],
        },
        {
            "term": "Deposits (deposit_unique_users)",
            "definition": "Users who completed a monetary deposit/payment within the app. A key monetization milestone.",
            "formula": None,
            "example": "50 depositing users from 500 sign-ups = 10% deposit conversion rate.",
            "platforms": ["AppsFlyer"],
        },
        {
            "term": "Attribution Window",
            "definition": "The time period after a click/view during which an install can be credited to that ad interaction. Typically 7-30 days for clicks.",
            "formula": None,
            "example": "With a 7-day click attribution window, an install 10 days after clicking an ad won't be attributed to that ad.",
            "platforms": ["AppsFlyer"],
        },
        {
            "term": "Re-attribution",
            "definition": "When an existing user who previously installed the app re-engages via a new campaign, potentially changing their attributed source.",
            "formula": None,
            "example": "A churned user clicking a retargeting ad and re-opening the app may be re-attributed.",
            "platforms": ["AppsFlyer"],
        },
    ],

    # ─────────────────────────────────────────
    # Platform Acronyms
    # ─────────────────────────────────────────
    "Platform Acronyms": [
        {
            "term": "GA4",
            "definition": "Google Analytics 4 — Google's current web and app analytics platform, replacing Universal Analytics. Event-based data model.",
            "formula": None,
            "example": "GA4 tracks sessions, users, page views, and custom events for website analytics.",
            "platforms": ["GA4"],
        },
        {
            "term": "GSC",
            "definition": "Google Search Console — Google's tool for monitoring your website's presence in Google Search results. Provides SEO data.",
            "formula": None,
            "example": "GSC shows which search queries bring users to your site and your ranking positions.",
            "platforms": ["GSC"],
        },
        {
            "term": "SEO",
            "definition": "Search Engine Optimization — The practice of improving your website to increase visibility in organic (non-paid) search results.",
            "formula": None,
            "example": "SEO work includes keyword optimization, content creation, and technical site improvements.",
            "platforms": ["GSC"],
        },
        {
            "term": "PPC",
            "definition": "Pay-Per-Click — An advertising model where you pay each time someone clicks your ad. Google Ads and Meta Ads use this model.",
            "formula": None,
            "example": "A PPC campaign charges you $2.00 every time someone clicks your ad.",
            "platforms": ["Google Ads", "Meta Ads"],
        },
        {
            "term": "ETL",
            "definition": "Extract, Transform, Load — The process of pulling data from sources (Extract), cleaning/restructuring it (Transform), and storing it (Load).",
            "formula": None,
            "example": "The ETL process pulls data from Google Ads API, normalizes dates, and loads it into DuckDB.",
            "platforms": ["All"],
        },
        {
            "term": "MMP",
            "definition": "Mobile Measurement Partner — A third-party service like AppsFlyer that tracks and attributes mobile app installs to ad campaigns.",
            "formula": None,
            "example": "AppsFlyer is our MMP, providing attribution data for iOS and Android installs.",
            "platforms": ["AppsFlyer"],
        },
        {
            "term": "SDK",
            "definition": "Software Development Kit — Code integrated into your app to enable tracking, analytics, or other features.",
            "formula": None,
            "example": "The AppsFlyer SDK in the app sends install and event data to AppsFlyer servers.",
            "platforms": ["AppsFlyer", "GA4"],
        },
        {
            "term": "API",
            "definition": "Application Programming Interface — A way for software systems to communicate. We use APIs to pull data from Google, Meta, AppsFlyer.",
            "formula": None,
            "example": "The Meta Marketing API provides campaign performance data programmatically.",
            "platforms": ["All"],
        },
    ],

    # ─────────────────────────────────────────
    # Google Ads / Search Terms
    # ─────────────────────────────────────────
    "Google Ads & Search": [
        {
            "term": "Impression Share",
            "definition": "The percentage of impressions your ads received compared to the total available impressions you were eligible for.",
            "formula": "Impression Share = Your Impressions ÷ Eligible Impressions",
            "example": "70% impression share means your ads appeared for 70% of eligible searches.",
            "platforms": ["Google Ads"],
        },
        {
            "term": "Lost IS (Rank)",
            "definition": "Impression Share lost due to low Ad Rank (quality score + bid). Improve by raising bids or improving ad quality.",
            "formula": None,
            "example": "20% Lost IS (Rank) means you missed 20% of impressions due to competitors outranking you.",
            "platforms": ["Google Ads"],
        },
        {
            "term": "Lost IS (Budget)",
            "definition": "Impression Share lost because your daily budget was exhausted. Ads stopped showing when budget ran out.",
            "formula": None,
            "example": "10% Lost IS (Budget) means increasing budget could capture 10% more impressions.",
            "platforms": ["Google Ads"],
        },
        {
            "term": "Quality Score",
            "definition": "Google's rating (1-10) of your ad relevance, landing page experience, and expected CTR. Higher scores lower your CPC.",
            "formula": "Based on: Expected CTR + Ad Relevance + Landing Page Experience",
            "example": "Quality Score of 8/10 means your ad is highly relevant and well-optimized.",
            "platforms": ["Google Ads"],
        },
        {
            "term": "Match Type",
            "definition": "How closely a search query must match your keyword to trigger your ad: Broad, Phrase, or Exact.",
            "formula": None,
            "example": "Exact match [running shoes] only shows for 'running shoes'; Broad match running shoes shows for 'jogging sneakers' too.",
            "platforms": ["Google Ads"],
        },
        {
            "term": "Search Query",
            "definition": "The actual words a user typed into Google. May differ from your keyword due to match types.",
            "formula": None,
            "example": "Keyword 'red shoes' might match query 'buy red running shoes near me'.",
            "platforms": ["GSC", "Google Ads"],
        },
        {
            "term": "View-Through Conversion",
            "definition": "A conversion that occurred after a user saw (but didn't click) your ad, then later converted through another path.",
            "formula": None,
            "example": "User sees your display ad, doesn't click, but later searches and purchases = view-through conversion.",
            "platforms": ["Google Ads", "Meta Ads"],
        },
    ],

    # ─────────────────────────────────────────
    # Meta Ads Terms
    # ─────────────────────────────────────────
    "Meta (Facebook/Instagram) Ads": [
        {
            "term": "Ad Set",
            "definition": "A group of ads within a campaign that share the same targeting, budget, schedule, and bidding settings.",
            "formula": None,
            "example": "Campaign 'Summer Sale' might have ad sets for 'Women 25-34' and 'Men 25-34' with different targeting.",
            "platforms": ["Meta Ads"],
        },
        {
            "term": "Objective",
            "definition": "The goal you select when creating a campaign (Awareness, Consideration, Conversion). Affects how Meta optimizes delivery.",
            "formula": None,
            "example": "'App Installs' objective tells Meta to show ads to people most likely to install.",
            "platforms": ["Meta Ads"],
        },
        {
            "term": "Link Clicks",
            "definition": "Clicks that lead to a destination URL (your website/app store). Excludes clicks on likes, comments, shares.",
            "formula": None,
            "example": "500 total clicks with 400 link clicks means 100 were engagement clicks (likes, etc.).",
            "platforms": ["Meta Ads"],
        },
        {
            "term": "Unique Clicks",
            "definition": "The number of unique users who clicked your ad. One person clicking twice counts as 1 unique click.",
            "formula": None,
            "example": "1,000 clicks from 800 unique clickers = average of 1.25 clicks per person.",
            "platforms": ["Meta Ads"],
        },
        {
            "term": "Publisher Platform",
            "definition": "Where your Meta ad appeared: Facebook, Instagram, Messenger, or Audience Network (third-party apps).",
            "formula": None,
            "example": "Same ad might get 60% of impressions on Instagram, 40% on Facebook.",
            "platforms": ["Meta Ads"],
        },
        {
            "term": "Lookalike Audience",
            "definition": "A targeting option where Meta finds users similar to your existing customers/converters.",
            "formula": None,
            "example": "Upload list of purchasers → Meta finds users with similar demographics and behaviors.",
            "platforms": ["Meta Ads"],
        },
    ],

    # ─────────────────────────────────────────
    # Data Architecture Terms
    # ─────────────────────────────────────────
    "Data Architecture": [
        {
            "term": "DuckDB",
            "definition": "An embedded analytical database used in this project as a local data warehouse. Fast for analytics queries.",
            "formula": None,
            "example": "All marketing data is stored in data/warehouse.duckdb for fast local querying.",
            "platforms": ["All"],
        },
        {
            "term": "Bronze Layer",
            "definition": "Raw data tables as extracted from source APIs. Minimal transformation, preserving original data.",
            "formula": None,
            "example": "ga4_sessions, gads_campaigns are Bronze tables with raw API data.",
            "platforms": ["All"],
        },
        {
            "term": "Silver Layer",
            "definition": "Cleaned/typed views with standardized column names and data types. More queryable than Bronze.",
            "formula": None,
            "example": "gads_campaigns_v is a Silver view with proper DATE type for date_day column.",
            "platforms": ["All"],
        },
        {
            "term": "Gold Layer",
            "definition": "Unified reporting tables/views optimized for business analysis. Cross-platform aggregations.",
            "formula": None,
            "example": "fact_paid_daily combines Google Ads + Meta Ads into one unified spend table.",
            "platforms": ["All"],
        },
        {
            "term": "Grain",
            "definition": "The level of detail in a table — what combination of columns makes each row unique.",
            "formula": None,
            "example": "af_daily_geo grain is (date, country, platform, app_id, media_source, campaign) — one row per combination.",
            "platforms": ["All"],
        },
        {
            "term": "Fact Table",
            "definition": "A table containing measurable, quantitative data (metrics) for analysis. Usually has foreign keys to dimensions.",
            "formula": None,
            "example": "fact_paid_daily contains spend, clicks, impressions metrics across platforms.",
            "platforms": ["All"],
        },
        {
            "term": "Dimension Table",
            "definition": "A table containing descriptive attributes used for filtering/grouping facts. Usually has a primary key.",
            "formula": None,
            "example": "dim_gads_campaign contains campaign names, statuses, types — descriptive data.",
            "platforms": ["All"],
        },
    ],
}


# ============================================
# Render Functions
# ============================================


def _render_term_card(term_data: dict) -> None:
    """
    Render a single glossary term as an expandable card.

    Args:
        term_data: Dictionary with term, definition, formula, example, platforms
    """
    with st.expander(f"**{term_data['term']}**", expanded=False):
        # Definition
        st.markdown(f"**Definition:** {term_data['definition']}")

        # Formula (if present)
        if term_data.get("formula"):
            st.markdown(f"**Formula:** `{term_data['formula']}`")

        # Example
        if term_data.get("example"):
            st.info(f"💡 **Example:** {term_data['example']}")

        # Platforms
        if term_data.get("platforms"):
            platforms_str = ", ".join(term_data["platforms"])
            st.caption(f"📊 **Platforms:** {platforms_str}")


def render_glossary() -> None:
    """
    Render the full glossary page with searchable, categorized definitions.
    """
    st.header("📖 Glossary")
    st.markdown(
        "*Definitions for technical terms, acronyms, and metrics used in this dashboard.*"
    )

    st.divider()

    # ── Search bar ────────────────────────────────────────────
    search_query = st.text_input(
        "🔍 Search terms",
        placeholder="Type to search (e.g., CTR, loyal users, CPA)...",
        key="glossary_search",
    )

    # ── Filter by category ────────────────────────────────────
    categories = ["All Categories"] + list(GLOSSARY_DATA.keys())
    selected_category = st.selectbox(
        "Filter by category",
        categories,
        index=0,
        key="glossary_category",
    )

    st.divider()

    # ── Build filtered list ───────────────────────────────────
    filtered_terms = []
    search_lower = search_query.lower().strip() if search_query else ""

    for category, terms in GLOSSARY_DATA.items():
        # Category filter
        if selected_category != "All Categories" and category != selected_category:
            continue

        for term_data in terms:
            # Search filter (match term, definition, or formula)
            if search_lower:
                searchable = (
                    term_data["term"].lower()
                    + " "
                    + term_data["definition"].lower()
                    + " "
                    + (term_data.get("formula") or "").lower()
                )
                if search_lower not in searchable:
                    continue

            filtered_terms.append((category, term_data))

    # ── Display results ───────────────────────────────────────
    if not filtered_terms:
        st.warning("No terms match your search. Try different keywords.")
        return

    st.caption(f"Showing {len(filtered_terms)} term(s)")

    # Group by category for display
    current_category = None
    for category, term_data in filtered_terms:
        if category != current_category:
            st.subheader(category)
            current_category = category

        _render_term_card(term_data)

    # ── Quick Reference Tables ────────────────────────────────
    st.divider()
    st.subheader("📋 Quick Reference: Key Formulas")

    formula_data = [
        ("CTR", "Clicks ÷ Impressions × 100", "Engagement quality"),
        ("CPC", "Spend ÷ Clicks", "Click efficiency"),
        ("CPM", "Spend ÷ Impressions × 1000", "Awareness cost"),
        ("CPA / CPI", "Spend ÷ Conversions", "Acquisition cost"),
        ("ROAS", "Revenue ÷ Spend", "Return on ad spend"),
        ("Conversion Rate", "Conversions ÷ Clicks × 100", "Funnel efficiency"),
        ("Frequency", "Impressions ÷ Reach", "Ad repetition"),
        ("Loyal Rate", "Loyal Users ÷ Installs × 100", "User quality"),
    ]

    col1, col2 = st.columns(2)
    for i, (metric, formula, use_case) in enumerate(formula_data):
        col = col1 if i % 2 == 0 else col2
        with col:
            st.markdown(
                f"""
                <div style="padding:10px;border-radius:6px;background:#1e1e2f;margin-bottom:8px;">
                    <strong>{metric}</strong><br/>
                    <code style="color:#4fc3f7;">{formula}</code><br/>
                    <span style="color:#888;font-size:12px;">{use_case}</span>
                </div>
                """,
                unsafe_allow_html=True,
            )

    # ── Platform color legend ─────────────────────────────────
    st.divider()
    st.subheader("🎨 Platform Legend")

    platform_colors = {
        "GA4": "#F9AB00",
        "GSC": "#4285F4",
        "Google Ads": "#34A853",
        "Meta Ads": "#1877F2",
        "AppsFlyer": "#12CBC4",
    }

    cols = st.columns(len(platform_colors))
    for i, (platform, color) in enumerate(platform_colors.items()):
        with cols[i]:
            st.markdown(
                f"""
                <div style="text-align:center;padding:12px;border-radius:6px;
                            background:{color}20;border:2px solid {color};">
                    <strong style="color:{color};">{platform}</strong>
                </div>
                """,
                unsafe_allow_html=True,
            )
