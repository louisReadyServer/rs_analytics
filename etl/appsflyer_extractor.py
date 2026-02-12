"""
AppsFlyer Pull API Data Extractor

Extracts aggregate data from the AppsFlyer Pull API (V2 Bearer auth).
Focuses on daily-granular reports for both iOS and Android apps.

Reports extracted:
- Partners by Date: Daily data by media source & campaign (includes in-app events)
- Geo by Date: Daily data by country, media source & campaign

Usage:
    from etl.appsflyer_extractor import AppsFlyerExtractor

    extractor = AppsFlyerExtractor(api_token="...", app_id="id6739326850", platform="ios")
    daily_df = extractor.extract_partners_by_date("2026-01-01", "2026-02-08")

API Reference:
    https://dev.appsflyer.com/hc/reference/overview-11
"""

import io
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests

from etl.base import BaseExtractor

# Configure logger for this module
logger = logging.getLogger(__name__)

# ============================================
# Constants
# ============================================

# Base URL for AppsFlyer Pull API aggregate data
BASE_URL = "https://hq1.appsflyer.com/api/agg-data/export/app"

# Report types we extract (daily-granular only)
DAILY_REPORT_TYPES = {
    "partners_by_date_report": "Partners Daily (by date, media source & campaign)",
    "geo_by_date_report": "Geo Daily (by country & date)",
}


class AppsFlyerExtractor(BaseExtractor):
    """
    Extracts daily aggregate data from AppsFlyer Pull API.

    Inherits from BaseExtractor for consistent logging and error handling
    across all extractors in the project.

    Attributes:
        api_token: V2 Bearer token for authentication
        app_id: AppsFlyer app ID (e.g., 'id6739326850' or 'com.appcms.liveapp')
        platform: Platform label ('ios' or 'android')
    """

    def __init__(
        self,
        api_token: str,
        app_id: str,
        platform: str,
        logger_instance: Optional[logging.Logger] = None,
    ):
        """
        Initialize the AppsFlyer extractor.

        Args:
            api_token: V2 Bearer token for authentication
            app_id: AppsFlyer app ID
            platform: 'ios' or 'android'
            logger_instance: Optional custom logger
        """
        super().__init__(source_name="appsflyer", logger=logger_instance)
        self.api_token = api_token
        self.app_id = app_id
        self.platform = platform.lower()
        self.logger.info(
            f"Initialized AppsFlyerExtractor for {self.platform} app: {self.app_id}"
        )

    # ─── API Communication ──────────────────────────────────────────

    def _make_request(
        self,
        report_type: str,
        from_date: str,
        to_date: str,
    ) -> Tuple[bool, Optional[pd.DataFrame], str]:
        """
        Make a single GET request to the AppsFlyer Pull API.

        Args:
            report_type: The report endpoint key (e.g., 'partners_by_date_report')
            from_date: Start date YYYY-MM-DD
            to_date: End date YYYY-MM-DD

        Returns:
            Tuple of (success, DataFrame or None, message)
        """
        url = f"{BASE_URL}/{self.app_id}/{report_type}/v5"

        headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Accept": "text/csv",
        }
        params = {"from": from_date, "to": to_date}

        try:
            self.logger.info(f"  Requesting {report_type} ({from_date} to {to_date})")
            response = requests.get(url, headers=headers, params=params, timeout=120)

            if response.status_code == 200:
                content = response.text.strip()
                if not content:
                    return True, None, "Empty response (no data for this period)"

                df = pd.read_csv(io.StringIO(content))
                if df.empty:
                    return True, None, "Parsed CSV is empty"

                return True, df, f"Fetched {len(df)} rows"

            elif response.status_code == 401:
                return False, None, "401 Unauthorized – check APPSFLYER_API_TOKEN"
            elif response.status_code == 403:
                # 403 on some endpoints is normal for larger date ranges
                return False, None, "403 Forbidden – IP may not be whitelisted or date range too large"
            elif response.status_code == 404:
                return False, None, f"404 – app '{self.app_id}' not found"
            elif response.status_code == 429:
                return False, None, "429 Rate limit exceeded – wait and retry"
            else:
                return False, None, f"HTTP {response.status_code}: {response.text[:300]}"

        except requests.exceptions.Timeout:
            return False, None, "Request timed out (120s)"
        except requests.exceptions.ConnectionError as exc:
            return False, None, f"Connection error: {exc}"
        except Exception as exc:
            return False, None, f"Unexpected error: {exc}"

    # ─── Column Normalisation ───────────────────────────────────────

    @staticmethod
    def _normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert messy AppsFlyer CSV column names to clean snake_case.

        Examples:
            'Date'                     → 'date'
            'Media Source (pid)'       → 'media_source'
            'Campaign (c)'             → 'campaign'
            'Agency/PMD (af_prt)'      → 'agency'
            'Loyal Users/Installs'     → 'loyal_users_per_install'
            'Total Revenue'            → 'total_revenue'
            'app_install (Unique users)' → 'app_install_unique_users'
        """
        rename_map = {
            "Date": "date",
            "Country": "country",
            "Agency/PMD (af_prt)": "agency",
            "Media Source (pid)": "media_source",
            "Campaign (c)": "campaign",
            "Impressions": "impressions",
            "Clicks": "clicks",
            "CTR": "ctr",
            "Installs": "installs",
            "Conversion Rate": "conversion_rate",
            "Sessions": "sessions",
            "Loyal Users": "loyal_users",
            "Loyal Users/Installs": "loyal_users_per_install",
            "Total Revenue": "total_revenue",
            "Total Cost": "total_cost",
            "ROI": "roi",
            "ARPU": "arpu",
            "Average eCPI": "avg_ecpi",
        }

        # Handle dynamic in-app event columns like:
        #   "app_install (Unique users)", "deposit (Event counter)", etc.
        new_cols = {}
        for col in df.columns:
            if col in rename_map:
                new_cols[col] = rename_map[col]
            else:
                # Convert "event_name (Metric description)" → "event_name_metric_description"
                cleaned = (
                    col.replace("(", "")
                    .replace(")", "")
                    .replace("/", "_per_")
                    .replace(" ", "_")
                    .lower()
                )
                # Remove consecutive underscores
                while "__" in cleaned:
                    cleaned = cleaned.replace("__", "_")
                cleaned = cleaned.strip("_")
                new_cols[col] = cleaned

        return df.rename(columns=new_cols)

    # ─── Extraction Methods ─────────────────────────────────────────

    def _fill_null_key_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Replace NaN / None in columns used as primary-key parts.

        AppsFlyer returns NaN for Organic traffic's agency, media_source,
        and campaign columns.  DuckDB PRIMARY KEY columns cannot be NULL,
        so we fill them with a readable placeholder.
        """
        key_fill = {
            "agency": "(none)",
            "media_source": "(organic)",
            "campaign": "(none)",
            "country": "(unknown)",
        }
        for col, fill_val in key_fill.items():
            if col in df.columns:
                df[col] = df[col].fillna(fill_val)
        return df

    def _request_with_retry(
        self,
        report_type: str,
        from_date: str,
        to_date: str,
        max_retries: int = 2,
        retry_delay: float = 3.0,
    ) -> Tuple[bool, Optional[pd.DataFrame], str]:
        """
        Make a request with automatic retry on 403 (rate limit).

        AppsFlyer's Pull API returns 403 when too many requests are made
        in quick succession.  A short delay + retry usually resolves this.

        Args:
            report_type: API report key
            from_date: YYYY-MM-DD
            to_date: YYYY-MM-DD
            max_retries: Number of retries on 403
            retry_delay: Seconds to wait between retries (increases each time)

        Returns:
            Same tuple as _make_request
        """
        for attempt in range(max_retries + 1):
            success, df, msg = self._make_request(report_type, from_date, to_date)

            if success:
                return success, df, msg

            # On 403, wait and retry
            if "403" in msg and attempt < max_retries:
                wait = retry_delay * (attempt + 1)
                self.logger.info(
                    f"  Rate limited (403), waiting {wait:.0f}s before retry "
                    f"({attempt + 1}/{max_retries})..."
                )
                time.sleep(wait)
                continue

            # Non-retryable error or out of retries
            return success, df, msg

        return False, None, "Max retries exceeded"

    def extract_partners_by_date(
        self, from_date: str, to_date: str
    ) -> pd.DataFrame:
        """
        Extract the Partners-by-Date report (daily granularity).

        This is the richest daily report — includes in-app event breakdowns
        (installs, sign-ups, deposits, screen views, etc.).

        Args:
            from_date: Start date YYYY-MM-DD
            to_date: End date YYYY-MM-DD

        Returns:
            Cleaned DataFrame with platform & app_id columns added
        """
        success, df, msg = self._request_with_retry(
            "partners_by_date_report", from_date, to_date
        )

        if not success or df is None:
            self.logger.warning(f"partners_by_date_report: {msg}")
            return pd.DataFrame()

        self.logger.info(f"  partners_by_date_report: {msg}")

        # Normalise column names
        df = self._normalise_columns(df)

        # Fill NULL key columns for DuckDB PK constraint
        df = self._fill_null_key_columns(df)

        # Add metadata columns
        df["platform"] = self.platform
        df["app_id"] = self.app_id
        df["extracted_at"] = datetime.now().isoformat()

        return df

    def extract_geo_by_date(
        self, from_date: str, to_date: str
    ) -> pd.DataFrame:
        """
        Extract the Geo-by-Date report (daily + country granularity).

        Args:
            from_date: Start date YYYY-MM-DD
            to_date: End date YYYY-MM-DD

        Returns:
            Cleaned DataFrame with platform & app_id columns added
        """
        # Small delay between reports to avoid rate limiting
        time.sleep(2)

        success, df, msg = self._request_with_retry(
            "geo_by_date_report", from_date, to_date
        )

        if not success or df is None:
            self.logger.warning(f"geo_by_date_report: {msg}")
            return pd.DataFrame()

        self.logger.info(f"  geo_by_date_report: {msg}")

        df = self._normalise_columns(df)
        df = self._fill_null_key_columns(df)
        df["platform"] = self.platform
        df["app_id"] = self.app_id
        df["extracted_at"] = datetime.now().isoformat()

        return df

    # ─── BaseExtractor interface ────────────────────────────────────

    def test_connection(self) -> Tuple[bool, str]:
        """
        Quick connectivity check using a 1-day partners_by_date request.

        Returns:
            (success, message)
        """
        from datetime import timedelta

        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        success, df, msg = self._make_request(
            "partners_by_date_report", yesterday, yesterday
        )

        if success:
            rows = len(df) if df is not None else 0
            return True, f"Connected – {rows} rows for yesterday"
        return False, msg

    def extract_all(
        self, start_date: str, end_date: str, **kwargs
    ) -> Dict[str, pd.DataFrame]:
        """
        Extract all daily-granular reports.

        Args:
            start_date: YYYY-MM-DD
            end_date: YYYY-MM-DD

        Returns:
            Dict mapping report name to DataFrame
        """
        self._start_extraction()

        results = {
            "partners_by_date": self.extract_partners_by_date(start_date, end_date),
            "geo_by_date": self.extract_geo_by_date(start_date, end_date),
        }

        self._log_extraction_summary(results)
        return results
