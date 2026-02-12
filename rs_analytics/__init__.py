"""
rs_analytics - Core analytics library for the RS Analytics Dashboard.

This package provides:
- db: DuckDB client with query timing and channel adapters
- metrics: Metric registry, contract validation, and MetricEngine
- quality: Data quality checks (freshness, PK, null rates)
- insights: Deterministic change detection and driver analysis
- utils: Shared formatting, logging, and time utilities
"""

__version__ = "3.0.0"
