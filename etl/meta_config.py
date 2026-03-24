"""
Meta (Facebook) Ads Configuration Module

Handles configuration and validation for Meta Ads API access.
Provides secure credential management and connection setup.

Usage:
    from etl.meta_config import get_meta_config, get_meta_api
    
    config = get_meta_config()
    api = get_meta_api()
"""

import os
import sys
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, List

from etl.utils import load_env_file, resolve_path

# Load environment variables
load_env_file()


class MetaConfigurationError(Exception):
    """Raised when Meta Ads configuration is invalid or missing."""
    pass


@dataclass
class MetaConfig:
    """
    Meta Ads configuration data class.
    
    Attributes:
        access_token: Meta API access token
        ad_account_ids: List of ad account IDs to extract data from
        primary_account_id: Primary ad account ID for single-account operations
        duckdb_path: Path to DuckDB database file
        api_version: Meta Graph API version
    """
    access_token: str
    ad_account_ids: List[str]
    primary_account_id: str
    duckdb_path: Path
    api_version: str = "v21.0"
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        # Ensure all account IDs have act_ prefix
        self.ad_account_ids = [
            f"act_{aid}" if not aid.startswith("act_") else aid
            for aid in self.ad_account_ids
        ]
        if not self.primary_account_id.startswith("act_"):
            self.primary_account_id = f"act_{self.primary_account_id}"


def get_meta_config() -> MetaConfig:
    """
    Load and validate Meta Ads configuration from environment variables.
    
    Returns:
        MetaConfig: Validated configuration object
        
    Raises:
        MetaConfigurationError: If required configuration is missing or invalid
    """
    # Get access token
    access_token = os.getenv("META_ACCESS_TOKEN")
    if not access_token:
        raise MetaConfigurationError(
            "META_ACCESS_TOKEN not found in environment.\n"
            "Please set it in your .env file:\n"
            "META_ACCESS_TOKEN=your_access_token_here"
        )
    
    if len(access_token) < 50 or access_token.startswith("your_"):
        raise MetaConfigurationError(
            "META_ACCESS_TOKEN appears to be invalid or a placeholder.\n"
            "Please set a valid access token in your .env file."
        )
    
    # Get ad account IDs
    primary_account = os.getenv("META_AD_ACCOUNT_ID", "")
    additional_accounts = os.getenv("META_ADDITIONAL_AD_ACCOUNTS", "")
    
    ad_account_ids = []
    if primary_account:
        ad_account_ids.append(primary_account)
    
    if additional_accounts:
        # Parse comma-separated list
        additional = [a.strip() for a in additional_accounts.split(",") if a.strip()]
        ad_account_ids.extend(additional)
    
    # Add hardcoded accounts if none found (from user's accounts)
    if not ad_account_ids:
        ad_account_ids = [
            "act_1368909211583634",  # [D.M] ReadyServer Ads
            "act_727951759704777"    # Ready Server Pte Ltd
        ]
    
    if not ad_account_ids:
        raise MetaConfigurationError(
            "No Meta ad account IDs configured.\n"
            "Please set META_AD_ACCOUNT_ID in your .env file:\n"
            "META_AD_ACCOUNT_ID=act_XXXXXXXXXX"
        )
    
    # Get DuckDB path
    duckdb_path = resolve_path(os.getenv("DUCKDB_PATH", None), "data/warehouse.duckdb")
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    
    return MetaConfig(
        access_token=access_token,
        ad_account_ids=ad_account_ids,
        primary_account_id=ad_account_ids[0],
        duckdb_path=duckdb_path
    )


def get_meta_api():
    """
    Initialize and return the Meta Ads API client.
    
    Returns:
        FacebookAdsApi: Initialized API client
        
    Raises:
        MetaConfigurationError: If API initialization fails
    """
    try:
        from facebook_business.api import FacebookAdsApi
        
        config = get_meta_config()
        api = FacebookAdsApi.init(access_token=config.access_token)
        
        return api
    except ImportError:
        raise MetaConfigurationError(
            "facebook-business package not installed.\n"
            "Install it with: pip install facebook-business"
        )
    except Exception as e:
        raise MetaConfigurationError(f"Failed to initialize Meta API: {str(e)}")


def validate_meta_connection() -> tuple[bool, str]:
    """
    Validate Meta Ads API connection.
    
    Returns:
        Tuple of (success: bool, message: str)
    """
    try:
        from facebook_business.api import FacebookAdsApi
        from facebook_business.adobjects.user import User
        
        config = get_meta_config()
        FacebookAdsApi.init(access_token=config.access_token)
        
        me = User(fbid='me')
        user_data = me.api_get(fields=['id', 'name'])
        
        return True, f"Connected as {user_data.get('name', 'Unknown')} (ID: {user_data.get('id')})"
    
    except MetaConfigurationError as e:
        return False, str(e)
    except Exception as e:
        return False, f"Connection failed: {str(e)}"
