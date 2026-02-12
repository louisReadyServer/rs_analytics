"""
AppsFlyer Pull API - Aggregate Data Connection Test Script

This script tests the AppsFlyer Pull API connection and pulls aggregate data:
1. Token validity check (simple API call)
2. Partners report (data grouped by media source and campaign)
3. Partners daily report (partners data broken down by date)
4. Daily report (data grouped by date, media source, and campaign)
5. Geo report (data grouped by country)
6. Geo daily report (geo data broken down by date)

Usage:
    python scripts/test_appsflyer_connection.py

Requirements:
    - APPSFLYER_API_TOKEN in .env file (V2 Bearer Token)
    - APPSFLYER_IOS_APP_ID and/or APPSFLYER_ANDROID_APP_ID in .env file
    - Your IP must be whitelisted in AppsFlyer dashboard

API Reference:
    https://dev.appsflyer.com/hc/reference/overview-11

Security Note:
    - Never commit your API token to git
    - The V2 bearer token grants broad access; keep it secure
    - Rotate tokens regularly via the AppsFlyer dashboard
"""

import io
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests

# Fix Windows console encoding for special characters
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Load environment variables from .env
from dotenv import load_dotenv
load_dotenv(project_root / ".env")


# ============================================
# Constants - AppsFlyer Pull API endpoints
# ============================================

# Base URL for AppsFlyer aggregate data Pull API
BASE_URL = "https://hq1.appsflyer.com/api/agg-data/export/app"

# Available aggregate report types and their descriptions
REPORT_TYPES = {
    "partners_report": "Partners (by media source & campaign)",
    "partners_by_date_report": "Partners Daily (by date, media source & campaign)",
    "daily_report": "Daily (by date, media source & campaign, no in-app events)",
    "geo_report": "Geo (by country)",
    "geo_by_date_report": "Geo Daily (by country & date)",
}


# ============================================
# Helper Functions
# ============================================


def get_appsflyer_config() -> Tuple[Optional[str], List[Dict[str, str]]]:
    """
    Retrieve AppsFlyer credentials from environment variables.

    Returns:
        Tuple of (api_token, list_of_app_dicts)
        Each app dict has keys: 'id', 'platform'
    """
    # Get the API token
    token = os.getenv("APPSFLYER_API_TOKEN")
    if not token:
        print("ERROR: APPSFLYER_API_TOKEN not found in .env file")
        print("\nTo fix this:")
        print("1. Open .env file")
        print("2. Set APPSFLYER_API_TOKEN=your_v2_bearer_token_here")
        print("3. Get your V2 token from AppsFlyer Dashboard > Security Center")
        return None, []

    # Collect app IDs from environment
    apps = []

    ios_app_id = os.getenv("APPSFLYER_IOS_APP_ID")
    if ios_app_id:
        apps.append({"id": ios_app_id, "platform": "iOS"})

    android_app_id = os.getenv("APPSFLYER_ANDROID_APP_ID")
    if android_app_id:
        apps.append({"id": android_app_id, "platform": "Android"})

    if not apps:
        print("ERROR: No app IDs found in .env file")
        print("\nTo fix this:")
        print("1. Set APPSFLYER_IOS_APP_ID=id123456789 (for iOS apps)")
        print("2. Set APPSFLYER_ANDROID_APP_ID=com.example.app (for Android apps)")
        return token, []

    return token, apps


def build_request_url(app_id: str, report_type: str) -> str:
    """
    Build the full API URL for a specific report.

    Args:
        app_id: The AppsFlyer app ID (e.g., 'id6739326850' or 'com.appcms.liveapp')
        report_type: One of the REPORT_TYPES keys (e.g., 'partners_report')

    Returns:
        Full API URL string
    """
    # Format: https://hq1.appsflyer.com/api/agg-data/export/app/{app_id}/{report_type}/v5
    return f"{BASE_URL}/{app_id}/{report_type}/v5"


def make_api_request(
    token: str,
    app_id: str,
    report_type: str,
    from_date: str,
    to_date: str,
) -> Tuple[bool, Optional[pd.DataFrame], str]:
    """
    Make a single API request to the AppsFlyer Pull API.

    Args:
        token: V2 Bearer token for authentication
        app_id: AppsFlyer app ID
        report_type: Report type key (e.g., 'partners_report')
        from_date: Start date in YYYY-MM-DD format
        to_date: End date in YYYY-MM-DD format

    Returns:
        Tuple of (success: bool, dataframe_or_none, message: str)
    """
    url = build_request_url(app_id, report_type)

    # Build request headers with Bearer authentication
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "text/csv",
    }

    # Build query parameters
    params = {
        "from": from_date,
        "to": to_date,
    }

    try:
        print(f"   Requesting: {url}")
        print(f"   Date range: {from_date} to {to_date}")

        # Make the GET request with a 60-second timeout
        response = requests.get(url, headers=headers, params=params, timeout=60)

        # Check HTTP status code
        if response.status_code == 200:
            # Parse CSV response into a pandas DataFrame
            content = response.text.strip()

            if not content:
                return True, None, "Request succeeded but returned empty data (no activity in this period)"

            # Use pandas to parse the CSV string
            dataframe = pd.read_csv(io.StringIO(content))

            if dataframe.empty:
                return True, None, "Request succeeded but returned empty DataFrame"

            return True, dataframe, f"Successfully fetched {len(dataframe)} rows"

        elif response.status_code == 401:
            return False, None, (
                "401 Unauthorized - Invalid or expired API token.\n"
                "   FIX: Check your APPSFLYER_API_TOKEN in .env\n"
                "   Get a fresh V2 token from AppsFlyer Dashboard > Security Center"
            )

        elif response.status_code == 403:
            return False, None, (
                "403 Forbidden - Your IP may not be whitelisted.\n"
                "   FIX: Add your current IP to the AppsFlyer API whitelist\n"
                "   Dashboard > Security Center > API Access > Whitelist IPs\n"
                f"   Currently whitelisted: {os.getenv('ALLOWED_IP', 'unknown')}"
            )

        elif response.status_code == 404:
            return False, None, (
                f"404 Not Found - App ID '{app_id}' not found or report not available.\n"
                "   FIX: Verify your app ID is correct in AppsFlyer Dashboard"
            )

        elif response.status_code == 429:
            return False, None, (
                "429 Too Many Requests - API rate limit exceeded.\n"
                "   FIX: Wait a few minutes and try again.\n"
                "   AppsFlyer limits Pull API to a certain number of calls per day."
            )

        else:
            return False, None, (
                f"HTTP {response.status_code}: {response.text[:500]}"
            )

    except requests.exceptions.Timeout:
        return False, None, "Request timed out after 60 seconds. Try again later."

    except requests.exceptions.ConnectionError as e:
        return False, None, f"Connection error: {str(e)}"

    except Exception as e:
        return False, None, f"Unexpected error: {str(e)}"


def display_dataframe_summary(df: pd.DataFrame, report_name: str) -> None:
    """
    Display a formatted summary of the returned DataFrame.

    Args:
        df: The pandas DataFrame to summarize
        report_name: Human-readable name of the report
    """
    print(f"\n   --- {report_name} Summary ---")
    print(f"   Rows: {len(df):,}")
    print(f"   Columns: {len(df.columns)}")
    print(f"   Column names: {', '.join(df.columns.tolist())}")

    # Show first few rows as a preview
    print(f"\n   First 5 rows (preview):")
    print("   " + "-" * 80)

    # Use pandas to_string for nice formatting, with limited width
    preview = df.head(5).to_string(index=False, max_colwidth=30)
    for line in preview.split("\n"):
        print(f"   {line}")

    print("   " + "-" * 80)

    # Show basic stats for numeric columns
    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
    if numeric_cols:
        print(f"\n   Numeric columns summary:")
        for col in numeric_cols[:5]:  # Show at most 5 numeric columns
            total = df[col].sum()
            print(f"   - {col}: total={total:,.2f}, mean={df[col].mean():,.2f}, min={df[col].min():,.2f}, max={df[col].max():,.2f}")


# ============================================
# Main Test Steps
# ============================================


def test_token_and_connectivity(token: str, app_id: str, platform: str) -> bool:
    """
    Step 1: Test that the token is valid and the API is reachable.

    We do this by making a minimal request (partners report for yesterday only).

    Args:
        token: AppsFlyer V2 Bearer token
        app_id: App ID to test with
        platform: Platform label (e.g., 'iOS', 'Android')

    Returns:
        True if the token/connectivity test passed
    """
    print("\n" + "=" * 60)
    print(f"STEP 1: Testing Token & Connectivity ({platform}: {app_id})")
    print("=" * 60)

    # Use yesterday as a minimal date range for the connectivity test
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    success, df, message = make_api_request(
        token=token,
        app_id=app_id,
        report_type="partners_report",
        from_date=yesterday,
        to_date=yesterday,
    )

    if success:
        print(f"   Token is valid! API is reachable.")
        if df is not None:
            print(f"   Got {len(df)} rows for yesterday's partners report.")
        else:
            print(f"   {message}")
        return True
    else:
        print(f"   FAILED: {message}")
        return False


def pull_all_aggregate_reports(
    token: str,
    app_id: str,
    platform: str,
    from_date: str,
    to_date: str,
) -> Dict[str, Optional[pd.DataFrame]]:
    """
    Step 2: Pull all 5 aggregate report types for an app.

    Args:
        token: AppsFlyer V2 Bearer token
        app_id: App ID to pull data for
        platform: Platform label
        from_date: Start date (YYYY-MM-DD)
        to_date: End date (YYYY-MM-DD)

    Returns:
        Dictionary mapping report_type -> DataFrame (or None if failed)
    """
    print("\n" + "=" * 60)
    print(f"STEP 2: Pulling All Aggregate Reports ({platform}: {app_id})")
    print(f"Date Range: {from_date} to {to_date}")
    print("=" * 60)

    results = {}

    for report_type, description in REPORT_TYPES.items():
        print(f"\n   {'='*50}")
        print(f"   Report: {description}")
        print(f"   Endpoint: {report_type}")
        print(f"   {'='*50}")

        success, df, message = make_api_request(
            token=token,
            app_id=app_id,
            report_type=report_type,
            from_date=from_date,
            to_date=to_date,
        )

        if success and df is not None:
            print(f"   SUCCESS: {message}")
            display_dataframe_summary(df, description)
            results[report_type] = df
        elif success:
            # Succeeded but no data
            print(f"   OK: {message}")
            results[report_type] = None
        else:
            # Failed
            print(f"   FAILED: {message}")
            results[report_type] = None

    return results


def print_final_summary(
    all_results: Dict[str, Dict[str, Optional[pd.DataFrame]]],
    connectivity_results: Dict[str, bool],
) -> None:
    """
    Print a final summary of all test results.

    Args:
        all_results: Nested dict of {app_label: {report_type: DataFrame_or_None}}
        connectivity_results: Dict of {app_label: bool (connected or not)}
    """
    print("\n" + "=" * 60)
    print("FINAL SUMMARY - AppsFlyer Aggregate Data Pull")
    print("=" * 60)

    for app_label, connected in connectivity_results.items():
        status_icon = "OK" if connected else "FAIL"
        print(f"\n  [{status_icon}] {app_label}")

        if app_label in all_results:
            for report_type, description in REPORT_TYPES.items():
                df = all_results[app_label].get(report_type)
                if df is not None:
                    print(f"       {description}: {len(df):,} rows")
                else:
                    print(f"       {description}: No data")
        elif not connected:
            print(f"       Skipped all reports (connectivity failed)")

    # Calculate totals
    total_rows = 0
    total_reports_with_data = 0
    for app_results in all_results.values():
        for df in app_results.values():
            if df is not None:
                total_rows += len(df)
                total_reports_with_data += 1

    print(f"\n  Total reports with data: {total_reports_with_data}")
    print(f"  Total rows across all reports: {total_rows:,}")
    print("=" * 60)


# ============================================
# Main Entry Point
# ============================================


def main():
    """Main entry point for AppsFlyer connection test."""

    print("\n" + "=" * 60)
    print("APPSFLYER PULL API - AGGREGATE DATA TEST")
    print("=" * 60)
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Step 0: Load credentials from environment
    token, apps = get_appsflyer_config()

    if not token:
        print("\nTest aborted: No API token found.")
        sys.exit(1)

    if not apps:
        print("\nTest aborted: No app IDs configured.")
        sys.exit(1)

    # Display configuration (token truncated for security)
    print(f"\nAPI Token: {token[:30]}...{token[-10:]} (truncated)")
    print(f"Apps to test: {len(apps)}")
    for app in apps:
        print(f"  - {app['platform']}: {app['id']}")

    # Define date range: last 30 days
    to_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")  # Yesterday
    from_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")  # 30 days ago
    print(f"\nDate range for reports: {from_date} to {to_date}")

    # Track results for all apps
    connectivity_results: Dict[str, bool] = {}
    all_results: Dict[str, Dict[str, Optional[pd.DataFrame]]] = {}

    # Process each app
    for app in apps:
        app_label = f"{app['platform']} ({app['id']})"

        # Step 1: Test connectivity
        connected = test_token_and_connectivity(token, app["id"], app["platform"])
        connectivity_results[app_label] = connected

        if not connected:
            print(f"\n   Skipping reports for {app_label} due to connectivity failure.")
            continue

        # Step 2: Pull all aggregate reports
        results = pull_all_aggregate_reports(
            token=token,
            app_id=app["id"],
            platform=app["platform"],
            from_date=from_date,
            to_date=to_date,
        )
        all_results[app_label] = results

    # Print final summary
    print_final_summary(all_results, connectivity_results)

    # Final status
    any_connected = any(connectivity_results.values())
    if any_connected:
        print("\nAppsFlyer Pull API aggregate data test complete!")
    else:
        print("\nAll connectivity tests FAILED. Check your token and IP whitelist.")
        sys.exit(1)


if __name__ == "__main__":
    main()
