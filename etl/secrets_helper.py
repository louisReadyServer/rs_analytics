"""
Streamlit Secrets Helper Module

Provides utility functions to read configuration from both Streamlit Cloud
secrets and local environment variables, with proper fallback logic.

Usage:
    from etl.secrets_helper import get_secret, is_streamlit_cloud
    
    api_key = get_secret("API_KEY", "default_value")
"""

import os
from typing import Any, Optional


def is_streamlit_cloud() -> bool:
    """
    Check if running on Streamlit Cloud.
    
    Returns:
        True if running on Streamlit Cloud, False otherwise
    """
    try:
        import streamlit as st
        return hasattr(st, 'secrets') and st.secrets is not None
    except (ImportError, RuntimeError):
        return False


def get_secret(key: str, default: Any = None) -> Any:
    """
    Get a secret from Streamlit secrets or environment variables.
    
    Priority order:
    1. Streamlit secrets (st.secrets[key])
    2. Environment variables (os.getenv(key))
    3. Default value
    
    Args:
        key: Secret key to retrieve
        default: Default value if not found (defaults to None)
        
    Returns:
        Secret value or default
        
    Example:
        api_key = get_secret("API_KEY", "fallback_key")
        db_path = get_secret("DUCKDB_PATH", "/tmp/db.duckdb")
    """
    # Try Streamlit secrets first (if available)
    try:
        import streamlit as st
        if hasattr(st, 'secrets') and st.secrets is not None:
            if key in st.secrets:
                return st.secrets[key]
    except (ImportError, RuntimeError, FileNotFoundError, KeyError):
        # Streamlit not available or secrets not configured
        pass
    
    # Fall back to environment variables
    env_value = os.getenv(key)
    if env_value is not None:
        return env_value
    
    # Return default if nothing found
    return default


def get_secret_section(section_name: str) -> Optional[dict]:
    """
    Get an entire section from Streamlit secrets (e.g., [GA4_SERVICE_ACCOUNT]).
    
    This is useful for nested TOML sections that contain multiple key-value pairs.
    
    Args:
        section_name: Name of the TOML section
        
    Returns:
        Dictionary of section contents, or None if not found
        
    Example:
        ga4_creds = get_secret_section("GA4_SERVICE_ACCOUNT")
        if ga4_creds:
            print(ga4_creds["client_email"])
    """
    try:
        import streamlit as st
        if hasattr(st, 'secrets') and st.secrets is not None:
            if section_name in st.secrets:
                # Convert to regular dict for easier handling
                return dict(st.secrets[section_name])
    except (ImportError, RuntimeError, FileNotFoundError, KeyError):
        pass
    
    return None
