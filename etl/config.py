"""
Configuration Loader for rs_analytics

This module provides centralized configuration management with:
- Environment variable loading via python-dotenv
- Comprehensive validation at import time
- Human-readable error messages with actionable fixes
- Secure handling of credentials (paths only, never contents)

Usage:
    from etl.config import get_config
    
    config = get_config()
    print(config.ga4_property_id)

The configuration is validated on first access. If validation fails,
a ConfigurationError is raised with a clear explanation of what's wrong
and how to fix it.
"""

import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from etl.utils import load_env_file, resolve_path, ensure_directory_exists, get_project_root

# ============================================
# Custom Exception
# ============================================


class ConfigurationError(Exception):
    """
    Raised when configuration is invalid or incomplete.
    
    Attributes:
        message: Human-readable description of the error
        fix: Actionable instructions to resolve the issue
    """
    
    def __init__(self, message: str, fix: str = ""):
        self.message = message
        self.fix = fix
        super().__init__(self._format_error())
    
    def _format_error(self) -> str:
        """Format error message with fix instructions."""
        error_text = f"\n{'='*60}\nCONFIGURATION ERROR\n{'='*60}\n\n{self.message}"
        if self.fix:
            error_text += f"\n\nHOW TO FIX:\n{self.fix}"
        error_text += f"\n\n{'='*60}\n"
        return error_text


# ============================================
# Configuration Data Class
# ============================================


@dataclass
class Config:
    """
    Immutable configuration container.
    
    All configuration values are validated before this object is created.
    This ensures that if you have a Config instance, the configuration is valid.
    
    Attributes:
        duckdb_path: Path to DuckDB database file
        ga4_property_id: Google Analytics 4 Property ID
        google_credentials_path: Path to GA4 service account JSON
        lookback_days: Number of days to look back for ETL
        log_dir: Directory for log files
        log_level: Logging level string (DEBUG, INFO, etc.)
        enable_bq_mirror: Whether BigQuery mirroring is enabled
        bq_project_id: BigQuery project ID (if mirroring enabled)
        bq_dataset: BigQuery dataset name (if mirroring enabled)
        bq_credentials_path: Path to BQ service account JSON (if mirroring enabled)
    """
    
    # Required settings
    duckdb_path: Path
    ga4_property_id: str
    google_credentials_path: Path
    
    # ETL settings
    lookback_days: int
    
    # Logging settings
    log_dir: Path
    log_level: str
    
    # Optional BigQuery settings
    enable_bq_mirror: bool
    bq_project_id: Optional[str]
    bq_dataset: Optional[str]
    bq_credentials_path: Optional[Path]


# ============================================
# Configuration Loader
# ============================================

# Singleton instance to avoid repeated validation
_config_instance: Optional[Config] = None


def get_config(force_reload: bool = False) -> Config:
    """
    Load and validate configuration from environment variables.
    
    This function:
    1. Loads .env file if present
    2. Validates all required environment variables
    3. Returns a validated Config object
    
    The configuration is cached after first load. Use force_reload=True
    to reload from environment (useful for testing).
    
    Args:
        force_reload: If True, reload configuration even if already cached
        
    Returns:
        Validated Config object
        
    Raises:
        ConfigurationError: If any required configuration is missing or invalid
    """
    global _config_instance
    
    if _config_instance is not None and not force_reload:
        return _config_instance
    
    # Load .env file from project root
    env_loaded = load_env_file()
    
    # Setup basic logging for config loading messages
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    logger = logging.getLogger("config")
    
    if env_loaded:
        logger.info("Loaded environment configuration from .env file")
    else:
        logger.info("No .env file found, using system environment variables")
    
    # ============================================
    # Validate Required Settings
    # ============================================
    
    # Validate GA4 Property ID
    ga4_property_id = os.getenv("GA4_PROPERTY_ID")
    if not ga4_property_id or ga4_property_id == "YOUR_GA4_PROPERTY_ID":
        raise ConfigurationError(
            message="Missing or invalid GA4_PROPERTY_ID environment variable.",
            fix=(
                "1. Find your GA4 Property ID in Google Analytics:\n"
                "   GA4 → Admin → Property Settings → Property ID\n"
                "2. Set it in your .env file:\n"
                "   GA4_PROPERTY_ID=123456789\n"
                "3. The Property ID is a numeric string (e.g., '123456789')"
            )
        )
    
    # Validate Google Application Credentials
    google_credentials_path_str = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not google_credentials_path_str:
        raise ConfigurationError(
            message="Missing GOOGLE_APPLICATION_CREDENTIALS environment variable.",
            fix=(
                "1. Create a service account in Google Cloud Console:\n"
                "   - Go to IAM & Admin → Service Accounts\n"
                "   - Create a new service account\n"
                "   - Download the JSON key file\n"
                "2. Save the JSON file to: secrets/ga4_service_account.json\n"
                "3. Set the ABSOLUTE path in your .env file:\n"
                "   GOOGLE_APPLICATION_CREDENTIALS=/full/path/to/secrets/ga4_service_account.json\n"
                "4. Add the service account email to GA4 Property Access Management\n"
                "   with at least 'Viewer' role"
            )
        )
    
    google_credentials_path = Path(google_credentials_path_str)
    
    # Check if path is absolute (warn if relative)
    if not google_credentials_path.is_absolute():
        logger.warning(
            "GOOGLE_APPLICATION_CREDENTIALS is a relative path. "
            "For production use, please use an absolute path."
        )
        # Convert to absolute based on project root
        google_credentials_path = (get_project_root() / google_credentials_path).resolve()
    
    if not google_credentials_path.exists():
        raise ConfigurationError(
            message=f"Service account file not found: {google_credentials_path}",
            fix=(
                "1. Verify the file exists at the specified path\n"
                "2. Check that GOOGLE_APPLICATION_CREDENTIALS contains the correct path\n"
                "3. Ensure the file has correct permissions (chmod 600 on Linux/Mac)\n"
                f"4. Expected location: {google_credentials_path}"
            )
        )
    
    if not google_credentials_path.is_file():
        raise ConfigurationError(
            message=f"GOOGLE_APPLICATION_CREDENTIALS points to a directory, not a file: {google_credentials_path}",
            fix=(
                "GOOGLE_APPLICATION_CREDENTIALS must point to the service account JSON file,\n"
                "not a directory. Example:\n"
                "   GOOGLE_APPLICATION_CREDENTIALS=/path/to/secrets/ga4_service_account.json"
            )
        )
    
    # ============================================
    # Validate DuckDB Path
    # ============================================
    
    duckdb_path = resolve_path(os.getenv("DUCKDB_PATH", None), "data/warehouse.duckdb")
    
    # Ensure parent directory exists or can be created
    duckdb_parent = duckdb_path.parent
    if not duckdb_parent.exists():
        try:
            duckdb_parent.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created data directory: {duckdb_parent}")
        except PermissionError:
            raise ConfigurationError(
                message=f"Cannot create DuckDB directory: {duckdb_parent}",
                fix=(
                    f"1. Create the directory manually: mkdir -p {duckdb_parent}\n"
                    "2. Ensure you have write permissions to the parent directory\n"
                    "3. Or set DUCKDB_PATH to a different location in .env"
                )
            )
    
    # ============================================
    # Validate Logging Settings
    # ============================================
    
    log_dir = resolve_path(os.getenv("LOG_DIR", None), "logs")
    try:
        ensure_directory_exists(log_dir)
    except PermissionError:
        raise ConfigurationError(
            message=f"Cannot create log directory: {log_dir}",
            fix=(
                f"1. Create the directory manually: mkdir -p {log_dir}\n"
                "2. Ensure you have write permissions\n"
                "3. Or set LOG_DIR to a different location in .env"
            )
        )
    
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    valid_log_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
    if log_level not in valid_log_levels:
        raise ConfigurationError(
            message=f"Invalid LOG_LEVEL: {log_level}",
            fix=f"LOG_LEVEL must be one of: {', '.join(valid_log_levels)}"
        )
    
    # ============================================
    # Validate ETL Settings
    # ============================================
    
    lookback_days_str = os.getenv("LOOKBACK_DAYS", "7")
    try:
        lookback_days = int(lookback_days_str)
        if lookback_days < 1:
            raise ValueError("must be positive")
    except ValueError:
        raise ConfigurationError(
            message=f"Invalid LOOKBACK_DAYS value: {lookback_days_str}",
            fix="LOOKBACK_DAYS must be a positive integer (e.g., 7 for last 7 days)"
        )
    
    # ============================================
    # Validate Optional BigQuery Settings
    # ============================================
    
    enable_bq_mirror_str = os.getenv("ENABLE_BQ_MIRROR", "0")
    enable_bq_mirror = enable_bq_mirror_str.lower() in ("1", "true", "yes")
    
    bq_project_id: Optional[str] = None
    bq_dataset: Optional[str] = None
    bq_credentials_path: Optional[Path] = None
    
    if enable_bq_mirror:
        # Validate BQ Project ID
        bq_project_id = os.getenv("BQ_PROJECT_ID")
        if not bq_project_id:
            raise ConfigurationError(
                message="ENABLE_BQ_MIRROR=1 but BQ_PROJECT_ID is not set.",
                fix=(
                    "When BigQuery mirroring is enabled, you must set:\n"
                    "   BQ_PROJECT_ID=your-gcp-project-id\n"
                    "Or disable mirroring by setting ENABLE_BQ_MIRROR=0"
                )
            )
        
        # Validate BQ Dataset
        bq_dataset = os.getenv("BQ_DATASET")
        if not bq_dataset:
            raise ConfigurationError(
                message="ENABLE_BQ_MIRROR=1 but BQ_DATASET is not set.",
                fix=(
                    "When BigQuery mirroring is enabled, you must set:\n"
                    "   BQ_DATASET=your_dataset_name\n"
                    "Or disable mirroring by setting ENABLE_BQ_MIRROR=0"
                )
            )
        
        # Validate BQ Credentials
        bq_credentials_str = os.getenv("BQ_CREDENTIALS_JSON")
        if not bq_credentials_str:
            raise ConfigurationError(
                message="ENABLE_BQ_MIRROR=1 but BQ_CREDENTIALS_JSON is not set.",
                fix=(
                    "When BigQuery mirroring is enabled, you must set:\n"
                    "   BQ_CREDENTIALS_JSON=/path/to/secrets/bq_service_account.json\n"
                    "This can be the same as GOOGLE_APPLICATION_CREDENTIALS if the\n"
                    "service account has BigQuery permissions.\n"
                    "Or disable mirroring by setting ENABLE_BQ_MIRROR=0"
                )
            )
        
        bq_credentials_path = Path(bq_credentials_str)
        if not bq_credentials_path.is_absolute():
            bq_credentials_path = (get_project_root() / bq_credentials_path).resolve()
        
        if not bq_credentials_path.exists():
            raise ConfigurationError(
                message=f"BigQuery credentials file not found: {bq_credentials_path}",
                fix=(
                    "1. Verify the file exists at the specified path\n"
                    "2. Check that BQ_CREDENTIALS_JSON contains the correct path\n"
                    "3. Ensure the service account has BigQuery permissions"
                )
            )
    
    # ============================================
    # Create and Cache Config Instance
    # ============================================
    
    _config_instance = Config(
        duckdb_path=duckdb_path,
        ga4_property_id=ga4_property_id,
        google_credentials_path=google_credentials_path,
        lookback_days=lookback_days,
        log_dir=log_dir,
        log_level=log_level,
        enable_bq_mirror=enable_bq_mirror,
        bq_project_id=bq_project_id,
        bq_dataset=bq_dataset,
        bq_credentials_path=bq_credentials_path,
    )
    
    # Log successful configuration (without sensitive values)
    logger.info("Configuration loaded and validated successfully")
    logger.info(f"  GA4 Property ID: {ga4_property_id}")
    logger.info(f"  Credentials Path: {google_credentials_path}")
    logger.info(f"  DuckDB Path: {duckdb_path}")
    logger.info(f"  Lookback Days: {lookback_days}")
    logger.info(f"  Log Level: {log_level}")
    logger.info(f"  BigQuery Mirror: {'Enabled' if enable_bq_mirror else 'Disabled'}")
    
    return _config_instance


def setup_logging(config: Config) -> logging.Logger:
    """
    Configure application logging based on Config settings.
    
    Creates a logger with both console and file handlers.
    Log files are rotated daily.
    
    Args:
        config: Validated configuration object
        
    Returns:
        Configured root logger
    """
    from logging.handlers import TimedRotatingFileHandler
    
    # Create logger
    logger = logging.getLogger("rs_analytics")
    logger.setLevel(getattr(logging, config.log_level))
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, config.log_level))
    console_format = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)
    
    # File handler (rotated daily, keep 30 days)
    log_file = config.log_dir / "rs_analytics.log"
    file_handler = TimedRotatingFileHandler(
        log_file,
        when="midnight",
        interval=1,
        backupCount=30,
        encoding="utf-8"
    )
    file_handler.setLevel(getattr(logging, config.log_level))
    file_format = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s"
    )
    file_handler.setFormatter(file_format)
    logger.addHandler(file_handler)
    
    return logger


# ============================================
# Validation Helpers for External Use
# ============================================


def validate_ga4_api_enabled() -> tuple[bool, str]:
    """
    Check if the Google Analytics Data API is accessible.
    
    This performs a minimal API call to verify:
    1. Credentials are valid
    2. API is enabled in the GCP project
    3. Service account has access to the property
    
    Returns:
        Tuple of (success: bool, message: str)
    """
    try:
        from google.analytics.data_v1beta import BetaAnalyticsDataClient
        from google.analytics.data_v1beta.types import (
            DateRange,
            Dimension,
            Metric,
            RunReportRequest,
        )
        
        config = get_config()
        
        # Create client using GOOGLE_APPLICATION_CREDENTIALS (no explicit credentials)
        client = BetaAnalyticsDataClient()
        
        # Run minimal query for yesterday's data
        request = RunReportRequest(
            property=f"properties/{config.ga4_property_id}",
            dimensions=[Dimension(name="date")],
            metrics=[Metric(name="sessions")],
            date_ranges=[DateRange(start_date="yesterday", end_date="yesterday")],
        )
        
        response = client.run_report(request)
        
        # Extract result for message
        sessions = 0
        if response.rows:
            sessions = response.rows[0].metric_values[0].value
        
        return True, f"GA4 connection successful! Yesterday's sessions: {sessions}"
        
    except Exception as e:
        error_msg = str(e)
        
        # Provide specific guidance based on error type
        if "403" in error_msg or "permission" in error_msg.lower():
            return False, (
                "GA4 permission denied. The service account does not have access.\n\n"
                "HOW TO FIX:\n"
                "1. Go to GA4 → Admin → Property Access Management\n"
                "2. Click '+' to add a new user\n"
                "3. Enter the service account email (found in your JSON file)\n"
                "4. Grant at least 'Viewer' role\n"
                "5. Wait a few minutes for changes to propagate"
            )
        elif "API" in error_msg and "not enabled" in error_msg.lower():
            return False, (
                "Google Analytics Data API is not enabled in your GCP project.\n\n"
                "HOW TO FIX:\n"
                "1. Go to: https://console.cloud.google.com/apis/library/analyticsdata.googleapis.com\n"
                "2. Select your project\n"
                "3. Click 'Enable'\n"
                "4. Wait a few minutes for the change to take effect"
            )
        elif "invalid_grant" in error_msg.lower() or "credentials" in error_msg.lower():
            return False, (
                "Invalid or expired service account credentials.\n\n"
                "HOW TO FIX:\n"
                "1. Generate a new JSON key in GCP Console:\n"
                "   IAM & Admin → Service Accounts → Your Account → Keys → Add Key\n"
                "2. Download the new JSON file\n"
                "3. Replace secrets/ga4_service_account.json with the new file\n"
                "4. Ensure GOOGLE_APPLICATION_CREDENTIALS points to the new file"
            )
        elif "property" in error_msg.lower() and "not found" in error_msg.lower():
            return False, (
                f"GA4 Property not found: {get_config().ga4_property_id}\n\n"
                "HOW TO FIX:\n"
                "1. Verify your GA4_PROPERTY_ID is correct\n"
                "2. Find your Property ID: GA4 → Admin → Property Settings\n"
                "3. Update GA4_PROPERTY_ID in your .env file"
            )
        else:
            return False, f"GA4 connection failed: {error_msg}"


# ============================================
# Module-level validation (optional)
# ============================================
# Uncomment the line below to validate config on import
# This causes early failure if config is invalid
# _ = get_config()
