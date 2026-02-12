"""
AppsFlyer API Configuration Module

Handles configuration and validation for AppsFlyer Pull API access.
Provides secure credential management for both iOS and Android app IDs.

Usage:
    from etl.appsflyer_config import get_appsflyer_config

    config = get_appsflyer_config()
    print(config.api_token[:20])  # Truncated for security
    print(config.apps)            # List of {id, platform} dicts
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

from dotenv import load_dotenv

# Load environment variables from .env at project root
project_root = Path(__file__).parent.parent
load_dotenv(project_root / ".env")


class AppsFlyerConfigurationError(Exception):
    """Raised when AppsFlyer configuration is invalid or missing."""
    pass


@dataclass
class AppsFlyerConfig:
    """
    AppsFlyer configuration data class.

    Attributes:
        api_token: V2 Bearer token for authentication
        apps: List of app dicts with 'id' and 'platform' keys
        duckdb_path: Path to DuckDB database file
    """
    api_token: str
    apps: List[Dict[str, str]]
    duckdb_path: Path

    @property
    def ios_app_id(self) -> Optional[str]:
        """Get the iOS app ID if configured."""
        for app in self.apps:
            if app["platform"] == "ios":
                return app["id"]
        return None

    @property
    def android_app_id(self) -> Optional[str]:
        """Get the Android app ID if configured."""
        for app in self.apps:
            if app["platform"] == "android":
                return app["id"]
        return None


def get_appsflyer_config() -> AppsFlyerConfig:
    """
    Load and validate AppsFlyer configuration from environment variables.

    Required env vars:
        APPSFLYER_API_TOKEN: V2 Bearer token

    Optional env vars (at least one app ID required):
        APPSFLYER_IOS_APP_ID: iOS app identifier (e.g., id6739326850)
        APPSFLYER_ANDROID_APP_ID: Android app identifier (e.g., com.appcms.liveapp)

    Returns:
        AppsFlyerConfig: Validated configuration object

    Raises:
        AppsFlyerConfigurationError: If required configuration is missing
    """
    # ── API Token ──
    api_token = os.getenv("APPSFLYER_API_TOKEN")
    if not api_token:
        raise AppsFlyerConfigurationError(
            "APPSFLYER_API_TOKEN not found in environment.\n"
            "Set it in your .env file:\n"
            "  APPSFLYER_API_TOKEN=your_v2_bearer_token\n"
            "Get your V2 token from: AppsFlyer Dashboard > Security Center"
        )

    if len(api_token) < 50:
        raise AppsFlyerConfigurationError(
            "APPSFLYER_API_TOKEN appears too short to be valid.\n"
            "The V2 bearer token should be a long JWT string."
        )

    # ── App IDs ──
    apps: List[Dict[str, str]] = []

    ios_app_id = os.getenv("APPSFLYER_IOS_APP_ID")
    if ios_app_id:
        apps.append({"id": ios_app_id, "platform": "ios"})

    android_app_id = os.getenv("APPSFLYER_ANDROID_APP_ID")
    if android_app_id:
        apps.append({"id": android_app_id, "platform": "android"})

    if not apps:
        raise AppsFlyerConfigurationError(
            "No AppsFlyer app IDs found in environment.\n"
            "Set at least one in your .env file:\n"
            "  APPSFLYER_IOS_APP_ID=id6739326850\n"
            "  APPSFLYER_ANDROID_APP_ID=com.example.app"
        )

    # ── DuckDB Path ──
    duckdb_path_str = os.getenv("DUCKDB_PATH", "./data/warehouse.duckdb")
    duckdb_path = Path(duckdb_path_str)

    if not duckdb_path.is_absolute():
        duckdb_path = (project_root / duckdb_path).resolve()

    # Ensure the data directory exists
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)

    return AppsFlyerConfig(
        api_token=api_token,
        apps=apps,
        duckdb_path=duckdb_path,
    )
