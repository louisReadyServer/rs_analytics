"""
RS Analytics Dashboard Components

This package contains reusable dashboard components for the Streamlit app.

Components:
- executive_dashboard: Cross-platform executive summary
- ga4_analytics: GA4 BI dashboard
- app_analytics: App Analytics dashboard (User Logs mart data)
- appsflyer_dashboard: AppsFlyer mobile analytics
- gsc_dashboard: Google Search Console SEO
- gads_dashboard: Google Ads PPC
- meta_dashboard: Meta (Facebook/Instagram) Ads
- twitter_dashboard: Twitter/X organic analytics
- date_picker: Calendar date range selector
- glossary: Term definitions and tooltips
- lifecycle_acquire / lifecycle_activate / lifecycle_monetize: Lifecycle pages
- behavioral_analysis: Customer segmentation & persona inference (ML)
- forecasting: Churn/engagement prediction (ML)
- clustering: Keyword intent classification (ML)
"""

from .executive_dashboard import render_executive_dashboard
from .app_analytics import render_app_analytics
from .appsflyer_dashboard import render_appsflyer_dashboard
from .gsc_dashboard import render_gsc_dashboard
from .gads_dashboard import render_gads_dashboard
from .meta_dashboard import render_meta_dashboard
from .twitter_dashboard import render_twitter_dashboard
from .behavioral_analysis import render_behavioral_analysis
from .forecasting import render_forecasting
from .clustering import render_clustering
from .lifecycle_acquire import render_acquire_page
from .lifecycle_activate import render_activate_page
from .lifecycle_monetize import render_monetize_page

__all__ = [
    'render_executive_dashboard',
    'render_app_analytics',
    'render_appsflyer_dashboard',
    'render_gsc_dashboard',
    'render_gads_dashboard',
    'render_meta_dashboard',
    'render_twitter_dashboard',
    'render_behavioral_analysis',
    'render_forecasting',
    'render_clustering',
    'render_acquire_page',
    'render_activate_page',
    'render_monetize_page',
]
