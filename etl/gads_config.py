"""
Google Ads Configuration for rs_analytics

This module provides configuration management for Google Ads data extraction:
- Validates Google Ads credentials (YAML file)
- Provides Google Ads-specific settings
- Creates GoogleAdsClient instances

Usage:
    from etl.gads_config import get_gads_config, get_gads_client
    
    config = get_gads_config()
    client = get_gads_client()
"""

import logging
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import yaml

from etl.config import ConfigurationError
from etl.secrets_helper import get_secret, is_streamlit_cloud, get_secret_section
from etl.utils import load_env_file, resolve_path, ensure_directory_exists


# ============================================
# Google Ads Configuration Data Class
# ============================================

@dataclass
class GAdsConfig:
    """
    Google Ads configuration container.
    
    Attributes:
        yaml_path: Path to google_ads.yaml credentials file
        customer_id: Google Ads Customer ID (without dashes)
        developer_token: Developer token from the YAML
        duckdb_path: Path to DuckDB database file
        log_dir: Directory for log files
        log_level: Logging level
    """
    
    yaml_path: Path
    customer_id: str
    developer_token: str
    login_customer_id: Optional[str]
    duckdb_path: Path
    log_dir: Path
    log_level: str


# ============================================
# Configuration Loader
# ============================================

# Singleton instance
_gads_config_instance: Optional[GAdsConfig] = None


def get_gads_config(force_reload: bool = False) -> GAdsConfig:
    """
    Load and validate Google Ads configuration.
    
    Args:
        force_reload: If True, reload configuration even if cached
        
    Returns:
        Validated GAdsConfig object
        
    Raises:
        ConfigurationError: If configuration is invalid
    """
    global _gads_config_instance
    
    if _gads_config_instance is not None and not force_reload:
        return _gads_config_instance
    
    # Load .env file (skip on Streamlit Cloud)
    if not is_streamlit_cloud():
        load_env_file()
    
    logger = logging.getLogger("gads_config")
    
    # ============================================
    # Validate YAML Path or Secrets
    # ============================================
    
    yaml_path = None
    yaml_data = None
    
    if is_streamlit_cloud():
        try:
            # Check if GOOGLE_ADS section exists in secrets
            yaml_data = get_secret_section("GOOGLE_ADS")
            if yaml_data:
                # Create temporary YAML file for compatibility with GoogleAdsClient
                temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False)
                yaml.dump(yaml_data, temp_file)
                temp_file.close()
                yaml_path = Path(temp_file.name)
                logger.info(f"Created temporary Google Ads YAML from st.secrets")
            else:
                raise ConfigurationError(
                    message="GOOGLE_ADS not found in Streamlit secrets.",
                    fix="Add a [GOOGLE_ADS] section to your Streamlit secrets"
                )
        except Exception as e:
            if not isinstance(e, ConfigurationError):
                raise ConfigurationError(
                    message=f"Failed to load Google Ads credentials from Streamlit secrets: {e}",
                    fix="Check that your Streamlit secrets are properly formatted in TOML"
                )
            raise
    else:
        # Local development: use file path
        yaml_path_str = os.getenv("GOOGLE_ADS_YAML_PATH")
        if not yaml_path_str:
            raise ConfigurationError(
                message="Missing GOOGLE_ADS_YAML_PATH environment variable.",
                fix=(
                    "1. Create secrets/google_ads.yaml with your credentials\n"
                    "2. Set the path in .env:\n"
                    "   GOOGLE_ADS_YAML_PATH=/full/path/to/secrets/google_ads.yaml"
                )
            )
        
        yaml_path = resolve_path(yaml_path_str, yaml_path_str)
        
        if not yaml_path.exists():
            raise ConfigurationError(
                message=f"Google Ads YAML file not found: {yaml_path}",
                fix=(
                    "1. Create the google_ads.yaml file at the specified path\n"
                    "2. Include: developer_token, client_id, client_secret, refresh_token\n"
                    f"3. Expected location: {yaml_path}"
                )
            )
        
        # ============================================
        # Validate YAML Contents
        # ============================================
        
        try:
            with open(yaml_path, 'r') as f:
                yaml_data = yaml.safe_load(f)
        except Exception as e:
            raise ConfigurationError(
                message=f"Failed to parse google_ads.yaml: {e}",
                fix="Ensure the YAML file is properly formatted"
            )
    
    # Validate YAML data (whether from file or secrets)
    required_fields = ['developer_token', 'client_id', 'client_secret', 'refresh_token']
    missing_fields = [f for f in required_fields if not yaml_data.get(f) or yaml_data.get(f) == f'YOUR_{f.upper()}']
    
    if missing_fields:
        raise ConfigurationError(
            message=f"Google Ads YAML missing required fields: {missing_fields}",
            fix=(
                "Fill in all required fields in google_ads.yaml or Streamlit secrets:\n"
                "  developer_token: Your API developer token\n"
                "  client_id: OAuth client ID\n"
                "  client_secret: OAuth client secret\n"
                "  refresh_token: OAuth refresh token"
            )
        )
    
    developer_token = yaml_data.get('developer_token')
    login_customer_id = yaml_data.get('login_customer_id')
    
    # ============================================
    # Validate Customer ID
    # ============================================
    
    customer_id = get_secret("GOOGLE_ADS_CUSTOMER_ID")
    if not customer_id:
        # Try to get from login_customer_id in YAML
        customer_id = str(login_customer_id) if login_customer_id else None
        
    if not customer_id:
        raise ConfigurationError(
            message="Missing GOOGLE_ADS_CUSTOMER_ID environment variable.",
            fix=(
                "1. Find your Customer ID in Google Ads (top right corner)\n"
                "2. Set it in .env (without dashes):\n"
                "   GOOGLE_ADS_CUSTOMER_ID=1234567890"
            )
        )
    
    # Remove any dashes
    customer_id = customer_id.replace('-', '')
    
    # ============================================
    # Get Shared Settings
    # ============================================
    
    # DuckDB path
    duckdb_path = resolve_path(get_secret("DUCKDB_PATH", None), "data/warehouse.duckdb")
    
    # Log directory
    log_dir = ensure_directory_exists(resolve_path(get_secret("LOG_DIR", None), "logs"))
    
    # Log level
    log_level = str(get_secret("LOG_LEVEL", "INFO")).upper()
    
    # ============================================
    # Create Config Instance
    # ============================================
    
    _gads_config_instance = GAdsConfig(
        yaml_path=yaml_path,
        customer_id=customer_id,
        developer_token=developer_token,
        login_customer_id=str(login_customer_id) if login_customer_id else None,
        duckdb_path=duckdb_path,
        log_dir=log_dir,
        log_level=log_level,
    )
    
    logger.info("Google Ads Configuration loaded successfully")
    logger.info(f"  YAML Path: {yaml_path}")
    logger.info(f"  Customer ID: {customer_id}")
    logger.info(f"  Login Customer ID: {login_customer_id or 'Not set'}")
    
    return _gads_config_instance


def get_gads_client():
    """
    Create and return a GoogleAdsClient instance.
    
    Returns:
        GoogleAdsClient configured with credentials from YAML
    """
    from google.ads.googleads.client import GoogleAdsClient
    
    config = get_gads_config()
    
    # Load client from YAML file
    client = GoogleAdsClient.load_from_storage(str(config.yaml_path))
    
    # Explicitly set login_customer_id for manager account access
    # This is required when accessing client accounts through a manager account
    if config.login_customer_id:
        client.login_customer_id = config.login_customer_id
    
    return client


def validate_gads_credentials() -> tuple[bool, str]:
    """
    Validate Google Ads credentials by attempting to access the account.
    
    Returns:
        Tuple of (success: bool, message: str)
    """
    try:
        config = get_gads_config()
        client = get_gads_client()
        
        # Get the GoogleAdsService
        ga_service = client.get_service("GoogleAdsService")
        
        # Simple query to test access
        query = """
            SELECT
                customer.id,
                customer.descriptive_name,
                customer.currency_code,
                customer.time_zone
            FROM customer
            LIMIT 1
        """
        
        # Use login_customer_id if available, otherwise customer_id
        customer_id = config.login_customer_id or config.customer_id
        
        response = ga_service.search(customer_id=customer_id, query=query)
        
        for row in response:
            customer = row.customer
            return True, (
                f"Google Ads connection successful!\n"
                f"Account: {customer.descriptive_name}\n"
                f"Customer ID: {customer.id}\n"
                f"Currency: {customer.currency_code}\n"
                f"Timezone: {customer.time_zone}"
            )
        
        return True, "Google Ads connection successful! (No customer data returned)"
        
    except Exception as e:
        error_msg = str(e)
        
        if "PERMISSION_DENIED" in error_msg or "CUSTOMER_NOT_FOUND" in error_msg:
            return False, (
                "Google Ads permission denied or customer not found.\n\n"
                "HOW TO FIX:\n"
                "1. Verify GOOGLE_ADS_CUSTOMER_ID is correct (no dashes)\n"
                "2. Ensure the OAuth account has access to this Google Ads account\n"
                "3. If using a Manager Account, set login_customer_id in google_ads.yaml"
            )
        elif "DEVELOPER_TOKEN" in error_msg:
            return False, (
                "Developer token issue.\n\n"
                "HOW TO FIX:\n"
                "1. Verify your developer_token in google_ads.yaml\n"
                "2. If using a test token, ensure you're accessing a test account\n"
                "3. Apply for Standard Access if needed"
            )
        elif "OAUTH" in error_msg.upper() or "refresh_token" in error_msg.lower():
            return False, (
                "OAuth authentication failed.\n\n"
                "HOW TO FIX:\n"
                "1. Verify client_id and client_secret in google_ads.yaml\n"
                "2. Generate a new refresh_token using OAuth playground\n"
                "3. Ensure the OAuth consent screen is configured"
            )
        else:
            return False, f"Google Ads connection failed: {error_msg}"
