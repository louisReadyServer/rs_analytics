#!/usr/bin/env python3
"""
Test GA4 Connection Script

This script verifies that:
1. Configuration is valid
2. Service account credentials are properly set up
3. Google Analytics Data API is enabled
4. Service account has access to the specified GA4 property

Usage:
    python scripts/test_ga4_connection.py
    
    # Or from project root:
    python -m scripts.test_ga4_connection

Exit Codes:
    0 - Success (all checks passed)
    1 - Configuration error
    2 - GA4 API error (authentication/authorization)
    3 - GA4 property access error

This script is safe to run in any environment and will not modify any data.
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def print_header(title: str) -> None:
    """Print a formatted section header."""
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}\n")


def print_success(message: str) -> None:
    """Print a success message with checkmark."""
    print(f"  [OK] {message}")


def print_error(message: str) -> None:
    """Print an error message with X mark."""
    print(f"  [X] {message}")


def print_info(message: str) -> None:
    """Print an info message."""
    print(f"  [i] {message}")


def main() -> int:
    """
    Run comprehensive GA4 connection tests.
    
    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    print_header("rs_analytics - GA4 Connection Test")
    
    # ========================================
    # Step 1: Load and Validate Configuration
    # ========================================
    print("Step 1: Loading configuration...")
    
    try:
        from etl.config import get_config, ConfigurationError
        config = get_config()
        print_success("Configuration loaded successfully")
        print_info(f"GA4 Property ID: {config.ga4_property_id}")
        print_info(f"Credentials Path: {config.google_credentials_path}")
    except ConfigurationError as e:
        print_error("Configuration validation failed")
        print(e)
        return 1
    except Exception as e:
        print_error(f"Unexpected error loading configuration: {e}")
        return 1
    
    # ========================================
    # Step 2: Verify Credentials File
    # ========================================
    print("\nStep 2: Verifying credentials file...")
    
    credentials_path = config.google_credentials_path
    
    if not credentials_path.exists():
        print_error(f"Credentials file not found: {credentials_path}")
        return 1
    
    print_success(f"Credentials file exists: {credentials_path}")
    
    # Check file permissions (informational on Windows)
    try:
        import stat
        file_stat = credentials_path.stat()
        mode = file_stat.st_mode
        
        # Check if file is readable
        if not (mode & stat.S_IRUSR):
            print_error("Credentials file is not readable by current user")
            return 1
        
        # Warn if file is world-readable (security concern)
        if mode & stat.S_IROTH:
            print_info(
                "WARNING: Credentials file is world-readable. "
                "Consider running: chmod 600 " + str(credentials_path)
            )
        else:
            print_success("Credentials file has appropriate permissions")
            
    except Exception as e:
        print_info(f"Could not check file permissions: {e}")
    
    # ========================================
    # Step 3: Verify Service Account JSON Structure
    # ========================================
    print("\nStep 3: Verifying service account JSON structure...")
    
    try:
        import json
        with open(credentials_path, 'r') as f:
            creds_data = json.load(f)
        
        # Check required fields (don't print sensitive values!)
        required_fields = ['type', 'project_id', 'client_email']
        missing_fields = [f for f in required_fields if f not in creds_data]
        
        if missing_fields:
            print_error(f"Service account JSON missing required fields: {missing_fields}")
            return 1
        
        if creds_data.get('type') != 'service_account':
            print_error(
                f"Invalid credential type: '{creds_data.get('type')}'. "
                "Expected 'service_account'. Make sure you're using a service account key, "
                "not an OAuth client secret."
            )
            return 1
        
        # Print non-sensitive info
        print_success("Service account JSON structure is valid")
        print_info(f"Service Account Type: {creds_data.get('type')}")
        print_info(f"GCP Project: {creds_data.get('project_id')}")
        print_info(f"Service Account Email: {creds_data.get('client_email')}")
        
        # Store email for later instructions
        service_account_email = creds_data.get('client_email')
        
    except json.JSONDecodeError as e:
        print_error(f"Invalid JSON in credentials file: {e}")
        return 1
    except Exception as e:
        print_error(f"Error reading credentials file: {e}")
        return 1
    
    # ========================================
    # Step 4: Test GA4 API Connection
    # ========================================
    print("\nStep 4: Testing GA4 API connection...")
    
    try:
        from google.analytics.data_v1beta import BetaAnalyticsDataClient
        from google.analytics.data_v1beta.types import (
            DateRange,
            Dimension,
            Metric,
            RunReportRequest,
        )
        
        # Create client using GOOGLE_APPLICATION_CREDENTIALS
        # This is the correct pattern - no explicit credentials passed
        print_info("Creating GA4 client (using GOOGLE_APPLICATION_CREDENTIALS)...")
        client = BetaAnalyticsDataClient()
        print_success("GA4 client created successfully")
        
    except ImportError as e:
        print_error(
            f"Missing required package: {e}\n"
            "  Install with: pip install google-analytics-data"
        )
        return 2
    except Exception as e:
        print_error(f"Failed to create GA4 client: {e}")
        return 2
    
    # ========================================
    # Step 5: Run Test Query
    # ========================================
    print("\nStep 5: Running test query...")
    
    try:
        # Request yesterday's data (most likely to exist)
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        
        print_info(f"Querying property {config.ga4_property_id} for date: {yesterday}")
        
        request = RunReportRequest(
            property=f"properties/{config.ga4_property_id}",
            dimensions=[Dimension(name="date")],
            metrics=[
                Metric(name="sessions"),
                Metric(name="activeUsers"),
                Metric(name="screenPageViews"),
            ],
            date_ranges=[DateRange(start_date="yesterday", end_date="yesterday")],
        )
        
        response = client.run_report(request)
        
        print_success("GA4 API query executed successfully!")
        
        # Display results
        print("\n  --- Query Results ---")
        if response.rows:
            row = response.rows[0]
            metrics = {
                "Sessions": row.metric_values[0].value,
                "Active Users": row.metric_values[1].value,
                "Page Views": row.metric_values[2].value,
            }
            for name, value in metrics.items():
                print(f"  {name}: {value}")
        else:
            print_info("No data returned for yesterday (this may be normal for new properties)")
        
    except Exception as e:
        error_msg = str(e)
        
        # Provide specific guidance based on error
        if "403" in error_msg or "permission" in error_msg.lower():
            print_error("Permission denied - Service account lacks GA4 access")
            print(f"""
  The service account does not have access to GA4 property {config.ga4_property_id}.
  
  HOW TO FIX:
  1. Go to Google Analytics → Admin → Property Access Management
  2. Click the '+' button to add a new user
  3. Enter this email: {service_account_email}
  4. Select 'Viewer' role (minimum required)
  5. Click 'Add'
  6. Wait 2-5 minutes for changes to propagate
  7. Run this test again
""")
            return 2
            
        elif "API" in error_msg and "not been used" in error_msg.lower():
            print_error("Google Analytics Data API is not enabled")
            print(f"""
  The Google Analytics Data API is not enabled in your GCP project.
  
  HOW TO FIX:
  1. Go to: https://console.cloud.google.com/apis/library/analyticsdata.googleapis.com
  2. Make sure the correct project is selected: {creds_data.get('project_id')}
  3. Click 'Enable'
  4. Wait 2-5 minutes for the change to take effect
  5. Run this test again
""")
            return 2
            
        elif "property" in error_msg.lower() and "not found" in error_msg.lower():
            print_error(f"GA4 Property not found: {config.ga4_property_id}")
            print("""
  The specified GA4 Property ID does not exist or is inaccessible.
  
  HOW TO FIX:
  1. Verify your GA4_PROPERTY_ID in .env is correct
  2. Find your Property ID: GA4 → Admin → Property Settings
  3. The Property ID should be a numeric string (e.g., '123456789')
  4. Update .env and run this test again
""")
            return 3
            
        else:
            print_error(f"GA4 API error: {error_msg}")
            return 2
    
    # ========================================
    # Success Summary
    # ========================================
    print_header("All Tests Passed!")
    print("""  Your GA4 credentials are correctly configured.
  
  Summary:
  - Configuration: Valid
  - Credentials File: Found and readable
  - Service Account: Valid JSON structure
  - GA4 API: Enabled and accessible
  - Property Access: Confirmed
  
  You can now run the ETL pipeline:
    python scripts/run_etl_unified.py --source ga4 --lookback-days 30
""")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
