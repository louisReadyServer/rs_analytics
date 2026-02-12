"""
RS Analytics Dashboard Components

This package contains reusable dashboard components for the Streamlit app.

Components:
- executive_dashboard: Cross-platform executive summary
- ga4_analytics: GA4 BI dashboard
- app_analytics: App Analytics dashboard (User Logs mart data)
- appsflyer_dashboard: AppsFlyer mobile analytics
- date_picker: Calendar date range selector
- glossary: Term definitions and tooltips
- behavioral_analysis: Customer segmentation & persona inference (ML)
- forecasting: Churn/engagement prediction (ML)
- clustering: Keyword intent classification (ML)
"""

from .executive_dashboard import render_executive_dashboard
from .app_analytics import render_app_analytics
from .behavioral_analysis import render_behavioral_analysis
from .forecasting import render_forecasting
from .clustering import render_clustering

__all__ = [
    'render_executive_dashboard',
    'render_app_analytics',
    'render_behavioral_analysis',
    'render_forecasting',
    'render_clustering',
]
