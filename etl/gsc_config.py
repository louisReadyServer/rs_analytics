"""
Google Search Console Configuration for rs_analytics

This module provides configuration management for GSC data extraction:
- Validates GSC credentials and site URL
- Provides GSC-specific settings
- Integrates with the main config system

Usage:
    from etl.gsc_config import get_gsc_config
    
    gsc_config = get_gsc_config()
    print(gsc_config.site_url)
"""

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from etl.config import ConfigurationError
from etl.utils import load_env_file, resolve_path, ensure_directory_exists


# ============================================
# GSC Configuration Data Class
# ============================================

@dataclass
class GSCConfig:
    """
    Google Search Console configuration container.
    
    Attributes:
        site_url: The Search Console property URL (e.g., https://example.com or sc-domain:example.com)
        credentials_path: Path to GSC service account JSON file
        duckdb_path: Path to DuckDB database file (shared with main config)
        log_dir: Directory for log files (shared with main config)
        log_level: Logging level (shared with main config)
    """
    
    site_url: str
    credentials_path: Path
    duckdb_path: Path
    log_dir: Path
    log_level: str


# ============================================
# GSC Configuration Loader
# ============================================

# Singleton instance for GSC config
_gsc_config_instance: Optional[GSCConfig] = None


def get_gsc_config(force_reload: bool = False) -> GSCConfig:
    """
    Load and validate GSC configuration from environment variables.
    
    Args:
        force_reload: If True, reload configuration even if cached
        
    Returns:
        Validated GSCConfig object
        
    Raises:
        ConfigurationError: If GSC configuration is invalid
    """
    global _gsc_config_instance
    
    if _gsc_config_instance is not None and not force_reload:
        return _gsc_config_instance
    
    # Load .env file
    load_env_file()
    
    logger = logging.getLogger("gsc_config")
    
    # ============================================
    # Validate GSC Site URL
    # ============================================
    
    gsc_site_url = os.getenv("GSC_SITE_URL")
    if not gsc_site_url:
        raise ConfigurationError(
            message="Missing GSC_SITE_URL environment variable.",
            fix=(
                "1. Set your Search Console property URL in .env:\n"
                "   GSC_SITE_URL=https://www.yoursite.com\n"
                "   or for domain properties:\n"
                "   GSC_SITE_URL=sc-domain:yoursite.com\n"
                "2. The URL must match exactly how it appears in Search Console"
            )
        )
    
    # Clean up the URL (remove trailing slash if present)
    gsc_site_url = gsc_site_url.rstrip('/')
    
    # ============================================
    # Validate GSC Credentials Path
    # ============================================
    
    gsc_credentials_str = os.getenv("GOOGLE_SEARCH_CONSOLE_CREDENTIALS")
    if not gsc_credentials_str:
        raise ConfigurationError(
            message="Missing GOOGLE_SEARCH_CONSOLE_CREDENTIALS environment variable.",
            fix=(
                "1. Create a service account in Google Cloud Console\n"
                "2. Download the JSON key file\n"
                "3. Save it to: secrets/gsc_service_account.json\n"
                "4. Set the path in .env:\n"
                "   GOOGLE_SEARCH_CONSOLE_CREDENTIALS=/full/path/to/secrets/gsc_service_account.json\n"
                "5. Add the service account email to Search Console as a user"
            )
        )
    
    gsc_credentials_path = resolve_path(gsc_credentials_str, gsc_credentials_str)
    
    if not gsc_credentials_path.exists():
        raise ConfigurationError(
            message=f"GSC credentials file not found: {gsc_credentials_path}",
            fix=(
                "1. Verify the file exists at the specified path\n"
                "2. Check GOOGLE_SEARCH_CONSOLE_CREDENTIALS in .env\n"
                f"3. Expected location: {gsc_credentials_path}"
            )
        )
    
    # ============================================
    # Get Shared Settings from Main Config
    # ============================================
    
    # DuckDB path
    duckdb_path = resolve_path(os.getenv("DUCKDB_PATH", None), "data/warehouse.duckdb")
    
    # Log directory
    log_dir = ensure_directory_exists(resolve_path(os.getenv("LOG_DIR", None), "logs"))
    
    # Log level
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    
    # ============================================
    # Create Config Instance
    # ============================================
    
    _gsc_config_instance = GSCConfig(
        site_url=gsc_site_url,
        credentials_path=gsc_credentials_path,
        duckdb_path=duckdb_path,
        log_dir=log_dir,
        log_level=log_level,
    )
    
    logger.info("GSC Configuration loaded successfully")
    logger.info(f"  Site URL: {gsc_site_url}")
    logger.info(f"  Credentials: {gsc_credentials_path}")
    
    return _gsc_config_instance


def validate_gsc_credentials() -> tuple[bool, str]:
    """
    Validate GSC credentials by attempting to list available sites.
    
    Returns:
        Tuple of (success: bool, message: str)
    """
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
        
        config = get_gsc_config()
        
        # Create credentials
        credentials = service_account.Credentials.from_service_account_file(
            str(config.credentials_path),
            scopes=['https://www.googleapis.com/auth/webmasters.readonly']
        )
        
        # Build service
        service = build('searchconsole', 'v1', credentials=credentials)
        
        # Try to access the site
        site_list = service.sites().list().execute()
        
        sites = site_list.get('siteEntry', [])
        site_urls = [s.get('siteUrl', '') for s in sites]
        
        if config.site_url in site_urls or config.site_url + '/' in site_urls:
            return True, f"GSC connection successful! Found {len(sites)} accessible sites."
        else:
            return False, (
                f"Site '{config.site_url}' not found in accessible sites.\n"
                f"Available sites: {site_urls}\n\n"
                "HOW TO FIX:\n"
                "1. Verify GSC_SITE_URL matches exactly how it appears in Search Console\n"
                "2. Add the service account email as a user in Search Console\n"
                "3. Grant 'Full' or 'Owner' permission"
            )
            
    except Exception as e:
        error_msg = str(e)
        
        if "403" in error_msg or "permission" in error_msg.lower():
            return False, (
                "GSC permission denied. Service account lacks access.\n\n"
                "HOW TO FIX:\n"
                "1. Go to Search Console → Settings → Users and permissions\n"
                "2. Click 'Add user'\n"
                "3. Enter the service account email from your JSON file\n"
                "4. Grant 'Full' permission\n"
                "5. Wait a few minutes for changes to propagate"
            )
        elif "API" in error_msg and "not enabled" in error_msg.lower():
            return False, (
                "Search Console API is not enabled.\n\n"
                "HOW TO FIX:\n"
                "1. Go to: https://console.cloud.google.com/apis/library/searchconsole.googleapis.com\n"
                "2. Select your project\n"
                "3. Click 'Enable'\n"
                "4. Wait a few minutes"
            )
        else:
            return False, f"GSC connection failed: {error_msg}"
