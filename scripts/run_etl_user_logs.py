"""
User Logs ETL – load CSVs from data/user_logs into DuckDB core + mart schemas.

Creates and populates core (dim_user, fact_user_activity, fact_points_ledger,
fact_payment_topup, user_account_state) and mart (user_daily_activity,
user_daily_points, platform_daily_overview, platform_weekly_overview) without
modifying existing GA4/GSC/GAds/Meta/AppsFlyer tables.

Usage:
    python scripts/run_etl_user_logs.py
    python scripts/run_etl_user_logs.py --csv-dir data/user_logs --duckdb-path data/warehouse.duckdb
    python scripts/run_etl_user_logs.py --dry-run
    python scripts/run_etl_user_logs.py --no-rebuild-marts
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from etl.user_logs_loader import run_user_logs_etl, validate_csv_dir

# Logging: file + stdout (align with run_etl_appsflyer.py)
log_dir = project_root / "logs"
log_dir.mkdir(parents=True, exist_ok=True)
log_file = log_dir / "user_logs_etl.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file, encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


def get_duckdb_path() -> Path:
    """Resolve DuckDB path (env or default relative to project root)."""
    import os
    raw = os.getenv("DUCKDB_PATH", str(project_root / "data" / "warehouse.duckdb"))
    p = Path(raw)
    if not p.is_absolute():
        p = project_root / p
    return p


def get_csv_dir() -> Path:
    """Resolve CSV directory (default data/user_logs)."""
    return project_root / "data" / "user_logs"


def print_summary(stats: dict) -> None:
    """Print human-readable ETL summary."""
    print("\n" + "=" * 60)
    print("USER LOGS ETL SUMMARY")
    print("=" * 60)
    print(f"Start:    {stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"End:      {stats['end_time'].strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Duration: {stats.get('duration_seconds', 0):.1f}s")
    print("-" * 60)
    print(f"core.dim_user:           {stats.get('dim_user', 0):,}")
    print(f"core.fact_user_activity: {stats.get('fact_user_activity', 0):,}")
    print(f"core.fact_points_ledger:{stats.get('fact_points_ledger', 0):,}")
    print(f"core.fact_payment_topup: {stats.get('fact_payment_topup', 0):,}")
    print(f"core.user_account_state: {stats.get('user_account_state', 0):,}")
    if stats.get("mart"):
        print("-" * 60)
        for name, count in stats["mart"].items():
            print(f"  mart.{name}: {count:,}")
    if stats.get("errors"):
        print("-" * 60)
        print("Errors:")
        for err in stats["errors"]:
            print(f"  - {err}")
    print("=" * 60)
    if stats.get("errors"):
        print("ETL completed with errors.")
    else:
        print("ETL completed successfully.")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="User Logs ETL – load CSVs into DuckDB core + mart"
    )
    parser.add_argument(
        "--duckdb-path",
        type=str,
        default=None,
        help="Path to DuckDB file (default: data/warehouse.duckdb)",
    )
    parser.add_argument(
        "--csv-dir",
        type=str,
        default=None,
        help="Directory containing user_logs CSVs (default: data/user_logs)",
    )
    parser.add_argument(
        "--rebuild-marts",
        action="store_true",
        default=True,
        help="Refresh mart tables after core load (default: True)",
    )
    parser.add_argument(
        "--no-rebuild-marts",
        action="store_true",
        help="Skip mart refresh",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate CSVs and log only; no writes",
    )
    args = parser.parse_args()

    duckdb_path = args.duckdb_path or str(get_duckdb_path())
    csv_dir = args.csv_dir or str(get_csv_dir())
    rebuild_marts = args.rebuild_marts and not args.no_rebuild_marts

    print("\n" + "=" * 60)
    print(" USER LOGS ETL PIPELINE")
    print("=" * 60)
    print(f" Started:  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f" DuckDB:   {duckdb_path}")
    print(f" CSV dir:  {csv_dir}")
    print(f" Marts:    {'rebuild' if rebuild_marts else 'skip'}")
    if args.dry_run:
        print(" Mode:     DRY-RUN (no writes)")
    print("=" * 60 + "\n")

    try:
        stats = run_user_logs_etl(
            duckdb_path=duckdb_path,
            csv_dir=csv_dir,
            rebuild_marts=rebuild_marts,
            dry_run=args.dry_run,
            logger=logger,
        )
        print_summary(stats)
        return 0 if not stats.get("errors") else 1
    except Exception as exc:
        logger.exception("ETL failed")
        print(f"\nError: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
