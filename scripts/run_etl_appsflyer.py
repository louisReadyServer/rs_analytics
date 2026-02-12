"""
AppsFlyer Aggregate Data ETL Pipeline

Extracts daily-granular aggregate data from the AppsFlyer Pull API
and loads it into DuckDB tables for analysis.

Features:
- Pulls data for both iOS and Android apps
- Two daily-granular tables:
    * af_daily_sources: by date, media source, campaign (with in-app events)
    * af_daily_geo: by date, country, media source, campaign
- Upsert (DELETE + INSERT) to avoid duplicates on re-runs
- Platform column ('ios' / 'android') in every row

Usage:
    # Last 30 days (default)
    python scripts/run_etl_appsflyer.py

    # Lifetime / full history
    python scripts/run_etl_appsflyer.py --lifetime

    # Custom date range
    python scripts/run_etl_appsflyer.py --start-date 2026-01-01 --end-date 2026-02-08

    # Specific lookback
    python scripts/run_etl_appsflyer.py --lookback-days 90

Tables Created:
    - af_daily_sources: Daily performance by media source & campaign
    - af_daily_geo: Daily performance by country, media source & campaign
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Fix Windows console encoding
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import duckdb
import pandas as pd

from etl.appsflyer_config import get_appsflyer_config, AppsFlyerConfigurationError
from etl.appsflyer_extractor import AppsFlyerExtractor

# ── Logging ──────────────────────────────────────────────────────────
log_file = project_root / "logs" / "appsflyer_etl.log"
log_file.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file, encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


# ============================================
# DuckDB Table Schemas
# ============================================

# These tables store daily-granular aggregate data from AppsFlyer.
# The "core" metrics columns are always present; in-app event columns
# are added dynamically because different apps may track different events.
#
# Strategy: We define tables with the guaranteed core columns + a generous
# set of known event columns.  Extra event columns from the API are stored
# by dynamically ALTER-ing the table at load time.

TABLE_SCHEMAS = {
    "af_daily_sources": """
        CREATE TABLE IF NOT EXISTS af_daily_sources (
            date              DATE,
            platform          VARCHAR,
            app_id            VARCHAR,
            agency            VARCHAR,
            media_source      VARCHAR,
            campaign          VARCHAR,
            impressions       DOUBLE,
            clicks            DOUBLE,
            ctr               DOUBLE,
            installs          DOUBLE,
            conversion_rate   DOUBLE,
            sessions          DOUBLE,
            loyal_users       DOUBLE,
            loyal_users_per_install DOUBLE,
            total_revenue     DOUBLE,
            total_cost        DOUBLE,
            roi               DOUBLE,
            arpu              DOUBLE,
            avg_ecpi          DOUBLE,
            extracted_at      TIMESTAMP,
            PRIMARY KEY (date, platform, app_id, media_source, campaign)
        )
    """,
    "af_daily_geo": """
        CREATE TABLE IF NOT EXISTS af_daily_geo (
            date              DATE,
            country           VARCHAR,
            platform          VARCHAR,
            app_id            VARCHAR,
            agency            VARCHAR,
            media_source      VARCHAR,
            campaign          VARCHAR,
            impressions       DOUBLE,
            clicks            DOUBLE,
            ctr               DOUBLE,
            installs          DOUBLE,
            conversion_rate   DOUBLE,
            sessions          DOUBLE,
            loyal_users       DOUBLE,
            loyal_users_per_install DOUBLE,
            total_revenue     DOUBLE,
            total_cost        DOUBLE,
            roi               DOUBLE,
            arpu              DOUBLE,
            avg_ecpi          DOUBLE,
            extracted_at      TIMESTAMP,
            PRIMARY KEY (date, country, platform, app_id, media_source, campaign)
        )
    """,
}


# ============================================
# Database Helpers
# ============================================


def create_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create all AppsFlyer tables if they don't already exist."""
    logger.info("Creating / verifying AppsFlyer tables...")
    for table_name, ddl in TABLE_SCHEMAS.items():
        try:
            conn.execute(ddl)
            logger.info(f"  Table '{table_name}' ready")
        except Exception as exc:
            logger.error(f"  Error creating '{table_name}': {exc}")
            raise


def _ensure_columns_exist(
    conn: duckdb.DuckDBPyConnection, table_name: str, df: pd.DataFrame
) -> None:
    """
    Dynamically add any columns in the DataFrame that don't yet exist
    in the target DuckDB table.  This handles varying in-app event columns
    across different apps (e.g., 'deposit_unique_users', 'screen_view_event_counter').
    """
    # Get existing columns from the table
    existing_cols_df = conn.execute(
        f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'"
    ).fetchdf()
    existing_cols = set(existing_cols_df["column_name"].str.lower().tolist())

    for col in df.columns:
        col_lower = col.lower()
        if col_lower not in existing_cols:
            # Default all dynamic event columns to DOUBLE (they're numeric)
            try:
                conn.execute(f'ALTER TABLE {table_name} ADD COLUMN "{col_lower}" DOUBLE')
                logger.info(f"    Added column '{col_lower}' to {table_name}")
            except Exception:
                # Column might already exist (race / case mismatch) – ignore
                pass


def upsert_dataframe(
    conn: duckdb.DuckDBPyConnection,
    df: pd.DataFrame,
    table_name: str,
    key_columns: list,
) -> int:
    """
    Upsert a DataFrame into a DuckDB table using DELETE + INSERT.

    Args:
        conn: DuckDB connection
        df: DataFrame to load
        table_name: Target table name
        key_columns: Columns forming the primary key (for dedup)

    Returns:
        Number of rows inserted
    """
    if df.empty:
        logger.warning(f"  No data for {table_name} – skipping")
        return 0

    try:
        # Ensure all DataFrame columns exist in the table
        _ensure_columns_exist(conn, table_name, df)

        # Normalise column names to lowercase to match DuckDB
        df.columns = [c.lower() for c in df.columns]

        # Register as temp view
        conn.register("_tmp_af", df)

        # Delete existing rows that match the composite key
        key_conds = " AND ".join(
            [f't."{k}" IS NOT DISTINCT FROM _tmp_af."{k}"' for k in key_columns]
        )
        conn.execute(
            f"DELETE FROM {table_name} t WHERE EXISTS "
            f"(SELECT 1 FROM _tmp_af WHERE {key_conds})"
        )

        # Build column list from the DataFrame (intersection with table cols)
        table_cols_df = conn.execute(
            f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'"
        ).fetchdf()
        table_cols = set(table_cols_df["column_name"].str.lower().tolist())
        df_cols = [c for c in df.columns if c in table_cols]

        cols_quoted = ", ".join([f'"{c}"' for c in df_cols])
        conn.execute(
            f"INSERT INTO {table_name} ({cols_quoted}) SELECT {cols_quoted} FROM _tmp_af"
        )

        conn.unregister("_tmp_af")
        logger.info(f"  Upserted {len(df):,} rows into {table_name}")
        return len(df)

    except Exception as exc:
        logger.error(f"  Error upserting into {table_name}: {exc}")
        try:
            conn.unregister("_tmp_af")
        except Exception:
            pass
        return 0


# ============================================
# Main ETL Logic
# ============================================


def run_etl(
    days: int = None,
    start_date: str = None,
    end_date: str = None,
    lifetime: bool = False,
) -> dict:
    """
    Run the complete AppsFlyer ETL pipeline.

    1. Load config (token, app IDs, DuckDB path)
    2. Determine date range
    3. For each app: extract data → upsert into DuckDB

    Args:
        days: Lookback days (mutually exclusive with start/end)
        start_date: Custom start date YYYY-MM-DD
        end_date: Custom end date YYYY-MM-DD
        lifetime: Pull maximum history (~13 months)

    Returns:
        Statistics dict with row counts and timing
    """
    stats = {
        "apps_processed": 0,
        "tables_updated": 0,
        "total_rows": 0,
        "errors": [],
        "start_time": datetime.now(),
        "end_time": None,
    }

    # ── 1. Load configuration ──
    config = get_appsflyer_config()
    logger.info(f"DuckDB path: {config.duckdb_path}")
    logger.info(f"Apps to process: {len(config.apps)}")

    # ── 2. Determine date range ──
    if start_date and end_date:
        from_date, to_date = start_date, end_date
    elif lifetime:
        # AppsFlyer keeps ~13 months of aggregate data
        to_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        from_date = (datetime.now() - timedelta(days=400)).strftime("%Y-%m-%d")
    elif days:
        to_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        from_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    else:
        # Default: last 30 days
        to_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        from_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

    logger.info(f"Date range: {from_date} to {to_date}")

    # ── 3. Connect to DuckDB and create tables ──
    conn = duckdb.connect(str(config.duckdb_path))

    try:
        create_tables(conn)

        # ── 4. Process each app ──
        for app in config.apps:
            app_label = f"{app['platform']} ({app['id']})"
            logger.info(f"\n{'='*60}")
            logger.info(f"Processing: {app_label}")
            logger.info(f"{'='*60}")

            try:
                extractor = AppsFlyerExtractor(
                    api_token=config.api_token,
                    app_id=app["id"],
                    platform=app["platform"],
                )

                # Connectivity check
                ok, msg = extractor.test_connection()
                if not ok:
                    logger.error(f"Connection failed for {app_label}: {msg}")
                    stats["errors"].append(f"{app_label}: {msg}")
                    continue
                logger.info(f"  Connected: {msg}")

                # Brief pause after connectivity test to avoid rate-limiting
                import time
                time.sleep(3)

                # Extract all reports
                data = extractor.extract_all(from_date, to_date)

                # ── Upsert partners_by_date → af_daily_sources ──
                partners_df = data.get("partners_by_date", pd.DataFrame())
                if not partners_df.empty:
                    rows = upsert_dataframe(
                        conn,
                        partners_df,
                        "af_daily_sources",
                        ["date", "platform", "app_id", "media_source", "campaign"],
                    )
                    stats["total_rows"] += rows
                    stats["tables_updated"] += 1

                # ── Upsert geo_by_date → af_daily_geo ──
                geo_df = data.get("geo_by_date", pd.DataFrame())
                if not geo_df.empty:
                    rows = upsert_dataframe(
                        conn,
                        geo_df,
                        "af_daily_geo",
                        ["date", "country", "platform", "app_id", "media_source", "campaign"],
                    )
                    stats["total_rows"] += rows
                    stats["tables_updated"] += 1

                stats["apps_processed"] += 1
                logger.info(f"  {app_label} done")

            except Exception as exc:
                logger.error(f"Error processing {app_label}: {exc}", exc_info=True)
                stats["errors"].append(f"{app_label}: {exc}")

        conn.commit()

    finally:
        conn.close()

    stats["end_time"] = datetime.now()
    stats["duration_seconds"] = (
        stats["end_time"] - stats["start_time"]
    ).total_seconds()

    return stats


def print_summary(stats: dict) -> None:
    """Print a human-readable ETL summary."""
    print("\n" + "=" * 60)
    print("APPSFLYER ETL SUMMARY")
    print("=" * 60)
    print(f"Start:    {stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"End:      {stats['end_time'].strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Duration: {stats['duration_seconds']:.1f}s")
    print("-" * 60)
    print(f"Apps processed:  {stats['apps_processed']}")
    print(f"Tables updated:  {stats['tables_updated']}")
    print(f"Total rows:      {stats['total_rows']:,}")

    if stats["errors"]:
        print("-" * 60)
        print(f"Errors ({len(stats['errors'])}):")
        for err in stats["errors"]:
            print(f"  - {err}")

    print("=" * 60)

    if stats["apps_processed"] > 0:
        print("ETL completed successfully!")
    else:
        print("ETL FAILED – no apps processed.")


# ============================================
# CLI Entry Point
# ============================================


def main():
    parser = argparse.ArgumentParser(
        description="AppsFlyer Aggregate Data ETL – load into DuckDB"
    )
    parser.add_argument(
        "--lifetime", action="store_true", help="Pull max history (~13 months)"
    )
    parser.add_argument(
        "--lookback-days", type=int, default=None, help="Days to look back"
    )
    parser.add_argument("--start-date", type=str, default=None, help="YYYY-MM-DD")
    parser.add_argument("--end-date", type=str, default=None, help="YYYY-MM-DD")

    args = parser.parse_args()

    print("\n" + "=" * 60)
    print(" APPSFLYER AGGREGATE DATA ETL PIPELINE")
    print("=" * 60)
    print(f" Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if args.lifetime:
        print(" Mode: LIFETIME (~13 months)")
    elif args.start_date and args.end_date:
        print(f" Mode: DATE RANGE ({args.start_date} to {args.end_date})")
    elif args.lookback_days:
        print(f" Mode: LOOKBACK ({args.lookback_days} days)")
    else:
        print(" Mode: DEFAULT (last 30 days)")
        args.lookback_days = 30

    print("=" * 60 + "\n")

    try:
        stats = run_etl(
            days=args.lookback_days,
            start_date=args.start_date,
            end_date=args.end_date,
            lifetime=args.lifetime,
        )
        print_summary(stats)
        return 0 if stats["apps_processed"] > 0 else 1

    except AppsFlyerConfigurationError as exc:
        logger.error(f"Configuration error: {exc}")
        print(f"\nConfiguration Error: {exc}")
        return 1
    except Exception as exc:
        logger.error(f"Unexpected error: {exc}", exc_info=True)
        print(f"\nError: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
