"""
Shared formatting and safe-conversion utilities.

These functions are used across all dashboard components to handle
messy data safely (NaN, None, mixed types from DuckDB/Pandas).

Previously duplicated in executive_dashboard.py, app_analytics.py, etc.
Now centralized here as the single source of truth.
"""

from typing import Optional, Union

import numpy as np
import pandas as pd


# ============================================
# Safe Type Conversion
# ============================================

def safe_int(value, default: int = 0) -> int:
    """
    Safely convert a value to integer, handling NaN, None, and invalid values.

    Works with: int, float, np.integer, np.floating, str, None, NaN.

    Args:
        value: Value to convert (can be any type)
        default: Fallback value if conversion fails

    Returns:
        Integer value or default

    Examples:
        >>> safe_int(3.7)       # 3
        >>> safe_int(None)      # 0
        >>> safe_int(np.nan)    # 0
        >>> safe_int("42.5")   # 42
    """
    if value is None:
        return default
    if isinstance(value, (int, np.integer)):
        return int(value)
    if isinstance(value, (float, np.floating)):
        if pd.isna(value) or np.isnan(value):
            return default
        return int(value)
    if isinstance(value, str):
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return default
    return default


def safe_float(value, default: float = 0.0) -> float:
    """
    Safely convert a value to float, handling NaN, None, and invalid values.

    Args:
        value: Value to convert
        default: Fallback value if conversion fails

    Returns:
        Float value or default

    Examples:
        >>> safe_float("3.14")   # 3.14
        >>> safe_float(None)     # 0.0
        >>> safe_float(np.nan)   # 0.0
    """
    if value is None:
        return default
    if isinstance(value, (int, float, np.integer, np.floating)):
        if pd.isna(value):
            return default
        try:
            if np.isnan(float(value)):
                return default
        except (TypeError, ValueError):
            pass
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except (ValueError, TypeError):
            return default
    return default


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """
    Safe division that returns default instead of raising ZeroDivisionError.

    Args:
        numerator: The dividend
        denominator: The divisor
        default: Value to return when denominator is zero or None

    Returns:
        numerator / denominator, or default if division is not possible

    Examples:
        >>> safe_divide(100, 50)   # 2.0
        >>> safe_divide(100, 0)    # 0.0
        >>> safe_divide(100, None) # 0.0
    """
    num = safe_float(numerator)
    den = safe_float(denominator)
    if den == 0:
        return default
    return num / den


# ============================================
# Display Formatting
# ============================================

def format_currency(value, prefix: str = "$", decimals: int = 0) -> str:
    """
    Format a number as currency string.

    Args:
        value: Numeric value to format
        prefix: Currency symbol (default "$")
        decimals: Number of decimal places

    Returns:
        Formatted string like "$1,234" or "$1,234.56"

    Examples:
        >>> format_currency(1234.5)          # "$1,235"
        >>> format_currency(1234.5, decimals=2) # "$1,234.50"
        >>> format_currency(None)            # "$0"
    """
    num = safe_float(value)
    if decimals > 0:
        return f"{prefix}{num:,.{decimals}f}"
    return f"{prefix}{num:,.0f}"


def format_pct(value, decimals: int = 1, multiply: bool = False) -> str:
    """
    Format a number as percentage string.

    Args:
        value: Numeric value
        decimals: Number of decimal places
        multiply: If True, multiply by 100 first (for 0-1 ratios)

    Returns:
        Formatted string like "12.3%"

    Examples:
        >>> format_pct(12.345)              # "12.3%"
        >>> format_pct(0.123, multiply=True) # "12.3%"
        >>> format_pct(None)                # "0.0%"
    """
    num = safe_float(value)
    if multiply:
        num *= 100
    return f"{num:,.{decimals}f}%"


def format_number(value, decimals: int = 0) -> str:
    """
    Format a number with comma separators.

    Args:
        value: Numeric value
        decimals: Number of decimal places

    Returns:
        Formatted string like "1,234" or "1,234.56"

    Examples:
        >>> format_number(1234567)       # "1,234,567"
        >>> format_number(1234.5, decimals=1)  # "1,234.5"
    """
    num = safe_float(value)
    if decimals > 0:
        return f"{num:,.{decimals}f}"
    return f"{safe_int(value):,}"


def format_delta(
    current: float,
    previous: float,
    cap: float = 999.0,
) -> Optional[str]:
    """
    Calculate and format percentage change between two values.

    Caps extreme values at +/- cap% to prevent noisy display
    (e.g., when a metric goes from 1 to 1000).

    Args:
        current: Current period value
        previous: Previous period value
        cap: Maximum absolute percentage to display (default 999%)

    Returns:
        Formatted delta string like "+12.3%" or ">999%", or None if not calculable.

    Examples:
        >>> format_delta(110, 100)     # "+10.0%"
        >>> format_delta(90, 100)      # "-10.0%"
        >>> format_delta(100, 0)       # None
        >>> format_delta(10000, 1)     # ">999%"
    """
    prev = safe_float(previous)
    curr = safe_float(current)
    if prev == 0:
        return None
    pct = ((curr - prev) / prev) * 100
    if pct > cap:
        return f"+{int(cap)}%+"
    if pct < -cap:
        return f"-{int(cap)}%+"
    return f"{pct:+.1f}%"


def calculate_delta(current: float, previous: float) -> Optional[float]:
    """
    Calculate percentage change between two values (raw number, not formatted).

    Args:
        current: Current period value
        previous: Previous period value

    Returns:
        Percentage change as float (e.g., 10.0 for +10%), or None if not calculable.
    """
    prev = safe_float(previous)
    curr = safe_float(current)
    if prev == 0:
        return None
    return ((curr - prev) / prev) * 100
