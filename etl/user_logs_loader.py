"""
User Logs ETL Loader for rs_analytics

Loads CSV files from data/user_logs into a two-schema DuckDB model (core + mart).
Additive only: creates core and mart schemas and tables without touching existing
GA4/GSC/GAds/Meta/AppsFlyer tables.

Features:
- Idempotent file-based loads: re-running with same CSVs replaces that file's data
- core: dim_user, fact_user_activity, fact_points_ledger, fact_payment_topup, user_account_state
- mart: user_daily_activity, user_daily_points, platform_daily_overview, platform_weekly_overview
- Business rules: dev_testing% excluded, REDEEM = negative points, mobile_verified from verification file
- Lineage columns: source_file_name, source_row_number, loaded_at on all fact tables

Usage:
    from etl.user_logs_loader import run_user_logs_etl
    stats = run_user_logs_etl(duckdb_path="data/warehouse.duckdb", csv_dir="data/user_logs")
"""

import csv
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import duckdb
import pandas as pd
import requests

# Required CSV files (same names for future runs)
REQUIRED_CSV_FILES = [
    "activity.csv",
    "customer.csv",
    "payment.csv",
    "point.csv",
    "redeem-mobile-verification.csv",
    "redeem-promocode.csv",
    "redeem-reload.csv",
]

# Column expectations per file (after normalizing headers)
REQUIRED_COLUMNS = {
    "activity.csv": ["UserID", "Created Date", "Type", "Title"],
    "customer.csv": ["UserID", "Created Date", "Registration IP"],
    "payment.csv": ["UserID", "Created Date", "Purpose", "amount", "points"],
    "point.csv": ["UserID", "Created Date", "type", "point amount"],
    "redeem-mobile-verification.csv": ["UserID", "Created Date", "point"],
    "redeem-promocode.csv": ["UserID", "Activated Date", "Promo type", "point"],
    "redeem-reload.csv": ["UserID", "Created Date", "Promo type", "point"],
}

# Points per SGD (constant)
POINTS_PER_SGD = 144

# Promo types starting with this prefix are excluded from DB and KPIs
DEV_TESTING_PREFIX = "dev_testing"


# ============================================
# Schema and table DDL
# ============================================


def create_schemas_and_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Create core and mart schemas and all user-log tables.
    Additive only: CREATE SCHEMA IF NOT EXISTS, CREATE TABLE IF NOT EXISTS.
    """
    conn.execute("CREATE SCHEMA IF NOT EXISTS core")
    conn.execute("CREATE SCHEMA IF NOT EXISTS mart")
    conn.execute("CREATE SCHEMA IF NOT EXISTS ref")

    # Reference: points packages (SGD 50/100/200/500 -> 144 pts per SGD)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ref.dim_points_package (
            package_code VARCHAR PRIMARY KEY,
            cash_amount_sgd DECIMAL(12,2) NOT NULL,
            points_amount BIGINT NOT NULL,
            points_per_sgd DECIMAL(12,2) NOT NULL
        )
    """)
    # Seed ref.dim_points_package (idempotent: INSERT OR IGNORE)
    for code, cash, pts in [("SGD50", 50, 7200), ("SGD100", 100, 14400), ("SGD200", 200, 28800), ("SGD500", 500, 72000)]:
        try:
            conn.execute(
                "INSERT OR IGNORE INTO ref.dim_points_package (package_code, cash_amount_sgd, points_amount, points_per_sgd) VALUES (?, ?, ?, 144)",
                [code, cash, pts],
            )
        except duckdb.Error:
            pass

    # core.dim_user
    conn.execute("""
        CREATE TABLE IF NOT EXISTS core.dim_user (
            user_id VARCHAR PRIMARY KEY,
            registration_ts TIMESTAMP NOT NULL,
            registration_ip VARCHAR NOT NULL,
            registration_country_code VARCHAR,
            registration_country_name VARCHAR,
            mobile_verified BOOLEAN NOT NULL DEFAULT FALSE,
            mobile_verified_at TIMESTAMP,
            created_at TIMESTAMP NOT NULL DEFAULT current_timestamp
        )
    """)

    # core.fact_user_activity (duplicates allowed)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS core.fact_user_activity (
            activity_event_id BIGINT PRIMARY KEY,
            user_id VARCHAR NOT NULL,
            event_ts TIMESTAMP NOT NULL,
            activity_type VARCHAR NOT NULL,
            activity_title VARCHAR,
            source_file_name VARCHAR NOT NULL,
            source_row_number BIGINT NOT NULL,
            loaded_at TIMESTAMP NOT NULL DEFAULT current_timestamp
        )
    """)

    # core.fact_points_ledger (append-only accounting)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS core.fact_points_ledger (
            ledger_entry_id BIGINT PRIMARY KEY,
            user_id VARCHAR NOT NULL,
            ledger_ts TIMESTAMP NOT NULL,
            entry_type VARCHAR NOT NULL,
            points_delta BIGINT NOT NULL,
            points_source VARCHAR NOT NULL,
            related_cash_amount_sgd DECIMAL(12,2),
            related_cash_currency VARCHAR,
            promo_type VARCHAR,
            source_file_name VARCHAR NOT NULL,
            source_row_number BIGINT NOT NULL,
            notes VARCHAR,
            loaded_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
            CHECK (points_delta <> 0)
        )
    """)

    # core.fact_payment_topup
    conn.execute("""
        CREATE TABLE IF NOT EXISTS core.fact_payment_topup (
            payment_id BIGINT PRIMARY KEY,
            user_id VARCHAR NOT NULL,
            payment_ts TIMESTAMP NOT NULL,
            purpose VARCHAR NOT NULL,
            cash_amount_sgd DECIMAL(12,2) NOT NULL,
            points_credited BIGINT NOT NULL,
            points_per_sgd DECIMAL(12,2) NOT NULL,
            package_code VARCHAR,
            source_file_name VARCHAR NOT NULL,
            source_row_number BIGINT NOT NULL,
            loaded_at TIMESTAMP NOT NULL DEFAULT current_timestamp
        )
    """)

    # core.user_account_state (computed snapshot)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS core.user_account_state (
            user_id VARCHAR PRIMARY KEY,
            as_of_ts TIMESTAMP NOT NULL,
            current_points_balance BIGINT NOT NULL,
            total_points_earned_paid BIGINT NOT NULL,
            total_points_earned_free BIGINT NOT NULL,
            total_points_spent BIGINT NOT NULL,
            total_launch_count BIGINT NOT NULL,
            total_terminate_count BIGINT NOT NULL,
            current_vps_live BIGINT NOT NULL,
            last_activity_ts TIMESTAMP,
            last_ledger_ts TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT current_timestamp
        )
    """)

    # mart.user_daily_activity
    conn.execute("""
        CREATE TABLE IF NOT EXISTS mart.user_daily_activity (
            activity_date DATE NOT NULL,
            user_id VARCHAR NOT NULL,
            total_events BIGINT NOT NULL,
            launch_count BIGINT NOT NULL,
            terminate_count BIGINT NOT NULL,
            reboot_count BIGINT NOT NULL,
            suspend_count BIGINT NOT NULL,
            other_activity_count BIGINT NOT NULL,
            PRIMARY KEY (activity_date, user_id)
        )
    """)

    # mart.user_daily_points
    conn.execute("""
        CREATE TABLE IF NOT EXISTS mart.user_daily_points (
            activity_date DATE NOT NULL,
            user_id VARCHAR NOT NULL,
            points_earned_paid BIGINT NOT NULL,
            points_earned_free BIGINT NOT NULL,
            points_spent BIGINT NOT NULL,
            net_points_delta BIGINT NOT NULL,
            end_of_day_balance BIGINT,
            topup_count BIGINT NOT NULL,
            topup_sum_sgd DECIMAL(12,2) NOT NULL,
            PRIMARY KEY (activity_date, user_id)
        )
    """)

    # mart.platform_daily_overview
    conn.execute("""
        CREATE TABLE IF NOT EXISTS mart.platform_daily_overview (
            activity_date DATE PRIMARY KEY,
            new_signups BIGINT NOT NULL,
            mobile_verified_new BIGINT NOT NULL,
            active_users BIGINT NOT NULL,
            new_vps_created BIGINT NOT NULL,
            vps_terminated BIGINT NOT NULL,
            net_vps_change BIGINT NOT NULL,
            topups_count BIGINT NOT NULL,
            topups_sum_sgd DECIMAL(12,2) NOT NULL,
            payer_count BIGINT NOT NULL,
            points_earned_paid BIGINT NOT NULL,
            points_earned_free BIGINT NOT NULL,
            points_spent BIGINT NOT NULL,
            net_points_delta BIGINT NOT NULL
        )
    """)

    # mart.platform_weekly_overview
    conn.execute("""
        CREATE TABLE IF NOT EXISTS mart.platform_weekly_overview (
            week_start_date DATE PRIMARY KEY,
            new_signups BIGINT NOT NULL,
            mobile_verified_new BIGINT NOT NULL,
            active_users BIGINT NOT NULL,
            new_vps_created BIGINT NOT NULL,
            vps_terminated BIGINT NOT NULL,
            net_vps_change BIGINT NOT NULL,
            topups_count BIGINT NOT NULL,
            topups_sum_sgd DECIMAL(12,2) NOT NULL,
            payer_count BIGINT NOT NULL,
            points_earned_paid BIGINT NOT NULL,
            points_earned_free BIGINT NOT NULL,
            points_spent BIGINT NOT NULL,
            net_points_delta BIGINT NOT NULL
        )
    """)


# ============================================
# CSV read and normalize
# ============================================


def _normalize_header(name: str) -> str:
    """Strip leading/trailing space; normalize ' point' -> 'point' for promo/reload."""
    s = (name or "").strip()
    if s == " point":
        return "point"
    return s


def read_csv_normalized(csv_path: Path, logger: logging.Logger) -> pd.DataFrame:
    """
    Read CSV with UTF-8-sig, normalize column names (Created Date vs Activated Date, ' point').
    Returns DataFrame with expected column names where applicable.
    """
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if not header:
            return pd.DataFrame()
        header = [_normalize_header(h) for h in header]
        # Unify date column name for downstream: use 'event_ts' internally or keep both
        rows = list(reader)
    df = pd.DataFrame(rows, columns=header)
    # Normalize: 'Created Date' and 'Activated Date' both present in different files
    if "Activated Date" in df.columns and "Created Date" not in df.columns:
        df["Created Date"] = df["Activated Date"]
    return df


def validate_csv_dir(csv_dir: Path, logger: logging.Logger) -> Tuple[bool, List[str]]:
    """
    Check that all required CSV files exist and have required columns.
    Returns (success, list of error messages).
    """
    errors: List[str] = []
    for fname in REQUIRED_CSV_FILES:
        path = csv_dir / fname
        if not path.exists():
            errors.append(f"Missing file: {path}")
            continue
        expected = REQUIRED_COLUMNS.get(fname, [])
        df = read_csv_normalized(path, logger)
        if df.empty and expected:
            errors.append(f"Empty or header-only file: {fname}")
            continue
        for col in expected:
            if col not in df.columns:
                errors.append(f"{fname}: missing column '{col}'. Found: {list(df.columns)}")
                break
    return len(errors) == 0, errors


# ============================================
# Parsing helpers
# ============================================


def _parse_ts(s: Any) -> Optional[datetime]:
    """Parse timestamp from CSV (e.g. '2025-12-01 01:50:27')."""
    if pd.isna(s) or s is None or str(s).strip() == "":
        return None
    try:
        return pd.to_datetime(s)
    except Exception:
        return None


def _parse_amount_sgd(s: Any) -> Optional[float]:
    """Parse amount like 'SGD54.50' or '54.50' -> float."""
    if pd.isna(s) or s is None:
        return None
    s = str(s).strip().upper().replace("SGD", "").strip()
    try:
        return float(s)
    except ValueError:
        return None


def _parse_int(s: Any) -> Optional[int]:
    if pd.isna(s) or s is None or str(s).strip() == "":
        return None
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return None


def _exclude_dev_testing(promo_type: Any) -> bool:
    """True if promo should be excluded (starts with dev_testing)."""
    if pd.isna(promo_type):
        return False
    return str(promo_type).strip().lower().startswith(DEV_TESTING_PREFIX)


# ============================================
# Idempotent load: delete by source file then insert
# ============================================


def _next_sequence(conn: duckdb.DuckDBPyConnection, table_schema: str, table_name: str, id_column: str) -> int:
    """Get next ID for surrogate key (max + 1 or 1)."""
    q = f"SELECT coalesce(max({id_column}), 0) + 1 FROM {table_schema}.{table_name}"
    return int(conn.execute(q).fetchone()[0])


def load_customer(
    conn: duckdb.DuckDBPyConnection,
    csv_dir: Path,
    logger: logging.Logger,
    dry_run: bool = False,
) -> int:
    """
    Load customer.csv into core.dim_user.
    Full refresh: truncate core.dim_user then insert from CSV.
    Country from IP: placeholder (no geo lib); can be extended later.
    """
    path = csv_dir / "customer.csv"
    df = read_csv_normalized(path, logger)
    if df.empty:
        return 0
    df = df.dropna(subset=["UserID"])
    df["registration_ts"] = df["Created Date"].apply(_parse_ts)
    df = df.dropna(subset=["registration_ts"])
    df["registration_ip"] = df["Registration IP"].astype(str).str.strip()
    # Country is enriched in a post-load step (enrich_ip_geo) rather than
    # inline, because it requires network calls. Set NULL here as a placeholder.
    df["registration_country_code"] = None
    df["registration_country_name"] = None
    df["mobile_verified"] = False  # set later from redeem-mobile-verification
    df["mobile_verified_at"] = pd.NaT
    rows = df[["UserID", "registration_ts", "registration_ip", "registration_country_code", "registration_country_name", "mobile_verified", "mobile_verified_at"]].rename(columns={"UserID": "user_id"})
    if dry_run:
        logger.info(f"[DRY-RUN] Would load {len(rows)} rows into core.dim_user from customer.csv")
        return len(rows)
    conn.execute("DELETE FROM core.dim_user")
    if rows.empty:
        return 0
    conn.register("_dim_user_stage", rows)
    conn.execute("""
        INSERT INTO core.dim_user (user_id, registration_ts, registration_ip, registration_country_code, registration_country_name, mobile_verified, mobile_verified_at)
        SELECT user_id, registration_ts, registration_ip, registration_country_code, registration_country_name, mobile_verified, mobile_verified_at FROM _dim_user_stage
    """)
    conn.unregister("_dim_user_stage")
    logger.info(f"Loaded {len(rows)} rows into core.dim_user from customer.csv")
    return len(rows)


def apply_mobile_verified(
    conn: duckdb.DuckDBPyConnection,
    csv_dir: Path,
    logger: logging.Logger,
    dry_run: bool = False,
) -> int:
    """
    Set mobile_verified = TRUE and mobile_verified_at from redeem-mobile-verification.csv.
    """
    path = csv_dir / "redeem-mobile-verification.csv"
    df = read_csv_normalized(path, logger)
    if df.empty:
        return 0
    df = df.dropna(subset=["UserID"])
    df["event_ts"] = df["Created Date"].apply(_parse_ts)
    df = df.dropna(subset=["event_ts"])
    verified = df.groupby("UserID")["event_ts"].min().reset_index()
    verified.columns = ["user_id", "mobile_verified_at"]
    if dry_run:
        logger.info(f"[DRY-RUN] Would set mobile_verified for {len(verified)} users")
        return len(verified)
    for _, row in verified.iterrows():
        conn.execute(
            "UPDATE core.dim_user SET mobile_verified = TRUE, mobile_verified_at = ? WHERE user_id = ?",
            [row["mobile_verified_at"], row["user_id"]],
        )
    logger.info(f"Set mobile_verified for {len(verified)} users from redeem-mobile-verification.csv")
    return len(verified)


def load_activity(
    conn: duckdb.DuckDBPyConnection,
    csv_dir: Path,
    logger: logging.Logger,
    dry_run: bool = False,
) -> int:
    """
    Load activity.csv into core.fact_user_activity. Duplicates preserved.
    Idempotency: delete rows where source_file_name = 'activity.csv', then insert all.
    """
    path = csv_dir / "activity.csv"
    df = read_csv_normalized(path, logger)
    if df.empty:
        return 0
    source_name = "activity.csv"
    df = df.dropna(subset=["UserID", "Type"])
    df["event_ts"] = df["Created Date"].apply(_parse_ts)
    df = df.dropna(subset=["event_ts"])
    df["source_file_name"] = source_name
    df["source_row_number"] = range(2, len(df) + 2)  # 1-based header
    if dry_run:
        logger.info(f"[DRY-RUN] Would load {len(df)} rows into core.fact_user_activity")
        return len(df)
    conn.execute("DELETE FROM core.fact_user_activity WHERE source_file_name = ?", [source_name])
    start_id = _next_sequence(conn, "core", "fact_user_activity", "activity_event_id")
    df["activity_event_id"] = range(start_id, start_id + len(df))
    rows = df[["activity_event_id", "UserID", "event_ts", "Type", "Title", "source_file_name", "source_row_number"]].rename(
        columns={"UserID": "user_id", "Type": "activity_type", "Title": "activity_title"}
    )
    conn.register("_act_stage", rows)
    conn.execute("""
        INSERT INTO core.fact_user_activity (activity_event_id, user_id, event_ts, activity_type, activity_title, source_file_name, source_row_number)
        SELECT activity_event_id, user_id, event_ts, activity_type, activity_title, source_file_name, source_row_number FROM _act_stage
    """)
    conn.unregister("_act_stage")
    logger.info(f"Loaded {len(rows)} rows into core.fact_user_activity from activity.csv")
    return len(rows)


def load_payment(
    conn: duckdb.DuckDBPyConnection,
    csv_dir: Path,
    logger: logging.Logger,
    dry_run: bool = False,
) -> int:
    """
    Load payment.csv into core.fact_payment_topup and core.fact_points_ledger (PURCHASE_TOP_UP entries).
    """
    path = csv_dir / "payment.csv"
    df = read_csv_normalized(path, logger)
    if df.empty:
        return 0
    source_name = "payment.csv"
    df = df.dropna(subset=["UserID", "Purpose"])
    df["payment_ts"] = df["Created Date"].apply(_parse_ts)
    df = df.dropna(subset=["payment_ts"])
    df["amount_raw"] = df["amount"]
    df["cash_amount_sgd"] = df["amount"].apply(_parse_amount_sgd)
    df = df.dropna(subset=["cash_amount_sgd"])
    df["points_credited"] = df["points"].apply(_parse_int)
    df = df.dropna(subset=["points_credited"])
    df["points_per_sgd"] = POINTS_PER_SGD
    # Package code from amount (e.g. SGD50 -> 50)
    def to_package(a: float) -> str:
        if a is None:
            return None
        for code, amt in [("SGD500", 500), ("SGD200", 200), ("SGD100", 100), ("SGD50", 50)]:
            if abs(a - amt) < 0.01 or (amt == 50 and 49 <= a <= 55):
                return code
        return f"SGD{a:.0f}" if a else None
    df["package_code"] = df["cash_amount_sgd"].apply(to_package)
    df["source_file_name"] = source_name
    df["source_row_number"] = range(2, len(df) + 2)
    if dry_run:
        logger.info(f"[DRY-RUN] Would load {len(df)} payment rows and {len(df)} ledger entries")
        return len(df)
    conn.execute("DELETE FROM core.fact_payment_topup WHERE source_file_name = ?", [source_name])
    conn.execute("DELETE FROM core.fact_points_ledger WHERE source_file_name = ?", [source_name])
    start_pay = _next_sequence(conn, "core", "fact_payment_topup", "payment_id")
    start_ledger = _next_sequence(conn, "core", "fact_points_ledger", "ledger_entry_id")
    df["payment_id"] = range(start_pay, start_pay + len(df))
    df["ledger_entry_id"] = range(start_ledger, start_ledger + len(df))
    pay_df = df[["payment_id", "UserID", "payment_ts", "Purpose", "cash_amount_sgd", "points_credited", "points_per_sgd", "package_code", "source_file_name", "source_row_number"]].rename(columns={"UserID": "user_id", "Purpose": "purpose"})
    conn.register("_pay_stage", pay_df)
    conn.execute("""
        INSERT INTO core.fact_payment_topup (payment_id, user_id, payment_ts, purpose, cash_amount_sgd, points_credited, points_per_sgd, package_code, source_file_name, source_row_number)
        SELECT payment_id, user_id, payment_ts, purpose, cash_amount_sgd, points_credited, points_per_sgd, package_code, source_file_name, source_row_number FROM _pay_stage
    """)
    conn.unregister("_pay_stage")
    ledger_df = df[["ledger_entry_id", "UserID", "payment_ts", "points_credited", "cash_amount_sgd", "source_file_name", "source_row_number"]].copy()
    ledger_df = ledger_df.rename(columns={"UserID": "user_id", "payment_ts": "ledger_ts"})
    ledger_df["entry_type"] = "PURCHASE_TOP_UP"
    ledger_df["points_delta"] = ledger_df["points_credited"]
    ledger_df["points_source"] = "paid"
    ledger_df["related_cash_amount_sgd"] = ledger_df["cash_amount_sgd"]
    ledger_df["related_cash_currency"] = "SGD"
    ledger_df["promo_type"] = None
    ledger_df["notes"] = None
    conn.register("_ledger_pay", ledger_df[["ledger_entry_id", "user_id", "ledger_ts", "entry_type", "points_delta", "points_source", "related_cash_amount_sgd", "related_cash_currency", "promo_type", "source_file_name", "source_row_number", "notes"]])
    conn.execute("""
        INSERT INTO core.fact_points_ledger (ledger_entry_id, user_id, ledger_ts, entry_type, points_delta, points_source, related_cash_amount_sgd, related_cash_currency, promo_type, source_file_name, source_row_number, notes)
        SELECT ledger_entry_id, user_id, ledger_ts, entry_type, points_delta, points_source, related_cash_amount_sgd, related_cash_currency, promo_type, source_file_name, source_row_number, notes FROM _ledger_pay
    """)
    conn.unregister("_ledger_pay")
    logger.info(f"Loaded {len(df)} rows into core.fact_payment_topup and fact_points_ledger from payment.csv")
    return len(df)


def load_point(
    conn: duckdb.DuckDBPyConnection,
    csv_dir: Path,
    logger: logging.Logger,
    dry_run: bool = False,
) -> int:
    """
    Load point.csv into core.fact_points_ledger. TOP_UP = positive (balance_credit), REDEEM = negative (consumption).
    point.csv TOP_UP does not distinguish paid vs free; only payment.csv rows are treated as paid.
    """
    path = csv_dir / "point.csv"
    df = read_csv_normalized(path, logger)
    if df.empty:
        return 0
    source_name = "point.csv"
    df = df.dropna(subset=["UserID", "type"])
    df["ledger_ts"] = df["Created Date"].apply(_parse_ts)
    df = df.dropna(subset=["ledger_ts"])
    pt = df["point amount"].apply(_parse_int)
    df["point_amount"] = pt
    df = df.dropna(subset=["point_amount"])
    df["source_file_name"] = source_name
    df["source_row_number"] = range(2, len(df) + 2)
    # TOP_UP -> positive, REDEEM -> negative
    df["points_delta"] = df.apply(lambda r: r["point_amount"] if str(r["type"]).strip().upper() == "TOP_UP" else -int(r["point_amount"]), axis=1)
    # TOP_UP in point.csv is just balance addition (not necessarily paid); only payment.csv is "paid"
    df["entry_type"] = df["type"].apply(lambda t: "BALANCE_CREDIT" if str(t).strip().upper() == "TOP_UP" else "USAGE_REDEEM")
    df["points_source"] = df["type"].apply(lambda t: "balance_credit" if str(t).strip().upper() == "TOP_UP" else "consumption")
    if dry_run:
        logger.info(f"[DRY-RUN] Would load {len(df)} rows into core.fact_points_ledger from point.csv")
        return len(df)
    conn.execute("DELETE FROM core.fact_points_ledger WHERE source_file_name = ?", [source_name])
    start_id = _next_sequence(conn, "core", "fact_points_ledger", "ledger_entry_id")
    df["ledger_entry_id"] = range(start_id, start_id + len(df))
    rows = df[["ledger_entry_id", "UserID", "ledger_ts", "entry_type", "points_delta", "points_source", "source_file_name", "source_row_number"]].copy()
    rows["related_cash_amount_sgd"] = None
    rows["related_cash_currency"] = None
    rows["promo_type"] = None
    rows["notes"] = None
    rows = rows.rename(columns={"UserID": "user_id"})
    conn.register("_point_ledger", rows)
    conn.execute("""
        INSERT INTO core.fact_points_ledger (ledger_entry_id, user_id, ledger_ts, entry_type, points_delta, points_source, related_cash_amount_sgd, related_cash_currency, promo_type, source_file_name, source_row_number, notes)
        SELECT ledger_entry_id, user_id, ledger_ts, entry_type, points_delta, points_source, related_cash_amount_sgd, related_cash_currency, promo_type, source_file_name, source_row_number, notes FROM _point_ledger
    """)
    conn.unregister("_point_ledger")
    logger.info(f"Loaded {len(rows)} rows into core.fact_points_ledger from point.csv")
    return len(rows)


def load_redeem_mobile_verification(
    conn: duckdb.DuckDBPyConnection,
    csv_dir: Path,
    logger: logging.Logger,
    dry_run: bool = False,
) -> int:
    """
    Load redeem-mobile-verification.csv into core.fact_points_ledger (MOBILE_VERIFICATION_BONUS).
    """
    path = csv_dir / "redeem-mobile-verification.csv"
    df = read_csv_normalized(path, logger)
    if df.empty:
        return 0
    source_name = "redeem-mobile-verification.csv"
    df = df.dropna(subset=["UserID"])
    df["ledger_ts"] = df["Created Date"].apply(_parse_ts)
    df = df.dropna(subset=["ledger_ts"])
    df["point"] = df["point"].apply(_parse_int)
    df = df.dropna(subset=["point"])
    df["source_file_name"] = source_name
    df["source_row_number"] = range(2, len(df) + 2)
    df["entry_type"] = "MOBILE_VERIFICATION_BONUS"
    df["points_delta"] = df["point"]
    df["points_source"] = "free_claim"
    if dry_run:
        logger.info(f"[DRY-RUN] Would load {len(df)} rows into core.fact_points_ledger from redeem-mobile-verification.csv")
        return len(df)
    conn.execute("DELETE FROM core.fact_points_ledger WHERE source_file_name = ?", [source_name])
    start_id = _next_sequence(conn, "core", "fact_points_ledger", "ledger_entry_id")
    df["ledger_entry_id"] = range(start_id, start_id + len(df))
    rows = df[["ledger_entry_id", "UserID", "ledger_ts", "entry_type", "points_delta", "points_source", "source_file_name", "source_row_number"]].rename(columns={"UserID": "user_id"})
    rows["related_cash_amount_sgd"] = None
    rows["related_cash_currency"] = None
    rows["promo_type"] = None
    rows["notes"] = None
    conn.register("_mv_ledger", rows)
    conn.execute("""
        INSERT INTO core.fact_points_ledger (ledger_entry_id, user_id, ledger_ts, entry_type, points_delta, points_source, related_cash_amount_sgd, related_cash_currency, promo_type, source_file_name, source_row_number, notes)
        SELECT ledger_entry_id, user_id, ledger_ts, entry_type, points_delta, points_source, related_cash_amount_sgd, related_cash_currency, promo_type, source_file_name, source_row_number, notes FROM _mv_ledger
    """)
    conn.unregister("_mv_ledger")
    logger.info(f"Loaded {len(rows)} rows into core.fact_points_ledger from redeem-mobile-verification.csv")
    return len(rows)


def load_redeem_promocode(
    conn: duckdb.DuckDBPyConnection,
    csv_dir: Path,
    logger: logging.Logger,
    dry_run: bool = False,
) -> int:
    """
    Load redeem-promocode.csv into core.fact_points_ledger (PROMO_CODE_BONUS).
    Exclude rows where Promo type starts with dev_testing.
    """
    path = csv_dir / "redeem-promocode.csv"
    df = read_csv_normalized(path, logger)
    if df.empty:
        return 0
    df = df[~df["Promo type"].apply(_exclude_dev_testing)]
    if df.empty:
        logger.info("redeem-promocode.csv: all rows excluded (dev_testing); 0 ledger entries")
        return 0
    source_name = "redeem-promocode.csv"
    df = df.dropna(subset=["UserID", "Promo type"])
    date_col = "Activated Date" if "Activated Date" in df.columns else "Created Date"
    df["ledger_ts"] = df[date_col].apply(_parse_ts)
    df = df.dropna(subset=["ledger_ts"])
    df["point"] = df["point"].apply(lambda x: _parse_int(x) if isinstance(x, str) else _parse_int(x))
    df = df.dropna(subset=["point"])
    df["source_file_name"] = source_name
    df["source_row_number"] = range(2, len(df) + 2)
    df["entry_type"] = "PROMO_CODE_BONUS"
    df["points_delta"] = df["point"]
    df["points_source"] = "free_claim"
    df["promo_type"] = df["Promo type"].astype(str).str.strip()
    if dry_run:
        logger.info(f"[DRY-RUN] Would load {len(df)} rows into core.fact_points_ledger from redeem-promocode.csv (dev_testing excluded)")
        return len(df)
    conn.execute("DELETE FROM core.fact_points_ledger WHERE source_file_name = ?", [source_name])
    start_id = _next_sequence(conn, "core", "fact_points_ledger", "ledger_entry_id")
    df["ledger_entry_id"] = range(start_id, start_id + len(df))
    rows = df[["ledger_entry_id", "UserID", "ledger_ts", "entry_type", "points_delta", "points_source", "promo_type", "source_file_name", "source_row_number"]].rename(columns={"UserID": "user_id"})
    rows["related_cash_amount_sgd"] = None
    rows["related_cash_currency"] = None
    rows["notes"] = None
    conn.register("_promo_ledger", rows)
    conn.execute("""
        INSERT INTO core.fact_points_ledger (ledger_entry_id, user_id, ledger_ts, entry_type, points_delta, points_source, related_cash_amount_sgd, related_cash_currency, promo_type, source_file_name, source_row_number, notes)
        SELECT ledger_entry_id, user_id, ledger_ts, entry_type, points_delta, points_source, related_cash_amount_sgd, related_cash_currency, promo_type, source_file_name, source_row_number, notes FROM _promo_ledger
    """)
    conn.unregister("_promo_ledger")
    logger.info(f"Loaded {len(rows)} rows into core.fact_points_ledger from redeem-promocode.csv (dev_testing excluded)")
    return len(rows)


def load_redeem_reload(
    conn: duckdb.DuckDBPyConnection,
    csv_dir: Path,
    logger: logging.Logger,
    dry_run: bool = False,
) -> int:
    """
    Load redeem-reload.csv into core.fact_points_ledger (PROMO_RELOAD_BONUS).
    """
    path = csv_dir / "redeem-reload.csv"
    df = read_csv_normalized(path, logger)
    if df.empty:
        return 0
    source_name = "redeem-reload.csv"
    df = df.dropna(subset=["UserID", "Promo type"])
    df["ledger_ts"] = df["Created Date"].apply(_parse_ts)
    df = df.dropna(subset=["ledger_ts"])
    df["point"] = df["point"].apply(_parse_int)
    df = df.dropna(subset=["point"])
    df["source_file_name"] = source_name
    df["source_row_number"] = range(2, len(df) + 2)
    df["entry_type"] = "PROMO_RELOAD_BONUS"
    df["points_delta"] = df["point"]
    df["points_source"] = "free_claim"
    df["promo_type"] = df["Promo type"].astype(str).str.strip()
    if dry_run:
        logger.info(f"[DRY-RUN] Would load {len(df)} rows into core.fact_points_ledger from redeem-reload.csv")
        return len(df)
    conn.execute("DELETE FROM core.fact_points_ledger WHERE source_file_name = ?", [source_name])
    start_id = _next_sequence(conn, "core", "fact_points_ledger", "ledger_entry_id")
    df["ledger_entry_id"] = range(start_id, start_id + len(df))
    rows = df[["ledger_entry_id", "UserID", "ledger_ts", "entry_type", "points_delta", "points_source", "promo_type", "source_file_name", "source_row_number"]].rename(columns={"UserID": "user_id"})
    rows["related_cash_amount_sgd"] = None
    rows["related_cash_currency"] = None
    rows["notes"] = None
    conn.register("_reload_ledger", rows)
    conn.execute("""
        INSERT INTO core.fact_points_ledger (ledger_entry_id, user_id, ledger_ts, entry_type, points_delta, points_source, related_cash_amount_sgd, related_cash_currency, promo_type, source_file_name, source_row_number, notes)
        SELECT ledger_entry_id, user_id, ledger_ts, entry_type, points_delta, points_source, related_cash_amount_sgd, related_cash_currency, promo_type, source_file_name, source_row_number, notes FROM _reload_ledger
    """)
    conn.unregister("_reload_ledger")
    logger.info(f"Loaded {len(rows)} rows into core.fact_points_ledger from redeem-reload.csv")
    return len(rows)


# ============================================
# Computed: core.user_account_state
# ============================================


def refresh_user_account_state(
    conn: duckdb.DuckDBPyConnection,
    logger: logging.Logger,
    dry_run: bool = False,
) -> int:
    """
    Recompute core.user_account_state from fact_points_ledger and fact_user_activity.
    current_points_balance = sum(points_delta), total_points_spent = sum of negative deltas,
    current_vps_live = launch_count - terminate_count.
    """
    if dry_run:
        logger.info("[DRY-RUN] Would refresh core.user_account_state")
        return 0
    conn.execute("DELETE FROM core.user_account_state")
    conn.execute("""
        INSERT INTO core.user_account_state (
            user_id, as_of_ts, current_points_balance, total_points_earned_paid, total_points_earned_free,
            total_points_spent, total_launch_count, total_terminate_count, current_vps_live,
            last_activity_ts, last_ledger_ts, updated_at
        )
        WITH ledger AS (
            SELECT
                user_id,
                SUM(points_delta) AS current_points_balance,
                SUM(CASE WHEN points_source = 'paid' AND points_delta > 0 THEN points_delta ELSE 0 END) AS total_points_earned_paid,
                SUM(CASE WHEN points_source = 'free_claim' AND points_delta > 0 THEN points_delta ELSE 0 END) AS total_points_earned_free,
                SUM(CASE WHEN points_delta < 0 THEN -points_delta ELSE 0 END) AS total_points_spent,
                MAX(ledger_ts) AS last_ledger_ts
            FROM core.fact_points_ledger
            GROUP BY user_id
        ),
        activity AS (
            SELECT
                user_id,
                SUM(CASE WHEN activity_type = 'LAUNCH_SERVER' THEN 1 ELSE 0 END) AS total_launch_count,
                SUM(CASE WHEN activity_type = 'TERMINATE_SERVER' THEN 1 ELSE 0 END) AS total_terminate_count,
                MAX(event_ts) AS last_activity_ts
            FROM core.fact_user_activity
            GROUP BY user_id
        )
        SELECT
            u.user_id,
            current_timestamp AS as_of_ts,
            COALESCE(l.current_points_balance, 0) AS current_points_balance,
            COALESCE(l.total_points_earned_paid, 0) AS total_points_earned_paid,
            COALESCE(l.total_points_earned_free, 0) AS total_points_earned_free,
            COALESCE(l.total_points_spent, 0) AS total_points_spent,
            COALESCE(a.total_launch_count, 0) AS total_launch_count,
            COALESCE(a.total_terminate_count, 0) AS total_terminate_count,
            COALESCE(a.total_launch_count, 0) - COALESCE(a.total_terminate_count, 0) AS current_vps_live,
            a.last_activity_ts,
            l.last_ledger_ts,
            current_timestamp AS updated_at
        FROM core.dim_user u
        LEFT JOIN ledger l ON u.user_id = l.user_id
        LEFT JOIN activity a ON u.user_id = a.user_id
    """)
    count = conn.execute("SELECT count(*) FROM core.user_account_state").fetchone()[0]
    logger.info(f"Refreshed core.user_account_state: {count} rows")
    return count


# ============================================
# Mart refresh
# ============================================


def refresh_mart_tables(
    conn: duckdb.DuckDBPyConnection,
    logger: logging.Logger,
    dry_run: bool = False,
) -> Dict[str, int]:
    """
    Rebuild mart.user_daily_activity, user_daily_points, platform_daily_overview, platform_weekly_overview from core.
    """
    counts: Dict[str, int] = {}
    if dry_run:
        logger.info("[DRY-RUN] Would refresh all mart tables")
        return counts
    # user_daily_activity
    conn.execute("DELETE FROM mart.user_daily_activity")
    conn.execute("""
        INSERT INTO mart.user_daily_activity (activity_date, user_id, total_events, launch_count, terminate_count, reboot_count, suspend_count, other_activity_count)
        SELECT
            CAST(event_ts AS DATE) AS activity_date,
            user_id,
            COUNT(*) AS total_events,
            SUM(CASE WHEN activity_type = 'LAUNCH_SERVER' THEN 1 ELSE 0 END) AS launch_count,
            SUM(CASE WHEN activity_type = 'TERMINATE_SERVER' THEN 1 ELSE 0 END) AS terminate_count,
            SUM(CASE WHEN activity_type = 'REBOOT_SERVER' THEN 1 ELSE 0 END) AS reboot_count,
            SUM(CASE WHEN activity_type = 'SUSPEND_SERVER' THEN 1 ELSE 0 END) AS suspend_count,
            SUM(CASE WHEN activity_type NOT IN ('LAUNCH_SERVER','TERMINATE_SERVER','REBOOT_SERVER','SUSPEND_SERVER') THEN 1 ELSE 0 END) AS other_activity_count
        FROM core.fact_user_activity
        GROUP BY CAST(event_ts AS DATE), user_id
    """)
    counts["user_daily_activity"] = conn.execute("SELECT count(*) FROM mart.user_daily_activity").fetchone()[0]

    # user_daily_points (from ledger + topup summary)
    conn.execute("DELETE FROM mart.user_daily_points")
    conn.execute("""
        INSERT INTO mart.user_daily_points (
            activity_date, user_id, points_earned_paid, points_earned_free, points_spent, net_points_delta,
            end_of_day_balance, topup_count, topup_sum_sgd
        )
        WITH daily_ledger AS (
            SELECT
                CAST(ledger_ts AS DATE) AS activity_date,
                user_id,
                SUM(CASE WHEN points_source = 'paid' AND points_delta > 0 THEN points_delta ELSE 0 END) AS points_earned_paid,
                SUM(CASE WHEN points_source = 'free_claim' AND points_delta > 0 THEN points_delta ELSE 0 END) AS points_earned_free,
                SUM(CASE WHEN points_delta < 0 THEN -points_delta ELSE 0 END) AS points_spent,
                SUM(points_delta) AS net_points_delta
            FROM core.fact_points_ledger
            GROUP BY CAST(ledger_ts AS DATE), user_id
        ),
        daily_topup AS (
            SELECT
                CAST(payment_ts AS DATE) AS activity_date,
                user_id,
                COUNT(*) AS topup_count,
                SUM(cash_amount_sgd) AS topup_sum_sgd
            FROM core.fact_payment_topup
            GROUP BY CAST(payment_ts AS DATE), user_id
        ),
        balance AS (
            SELECT user_id, activity_date,
                SUM(net_points_delta) OVER (PARTITION BY user_id ORDER BY activity_date) AS end_of_day_balance
            FROM (SELECT user_id, activity_date, net_points_delta FROM daily_ledger) t
        )
        SELECT
            d.activity_date,
            d.user_id,
            COALESCE(d.points_earned_paid, 0),
            COALESCE(d.points_earned_free, 0),
            COALESCE(d.points_spent, 0),
            COALESCE(d.net_points_delta, 0),
            b.end_of_day_balance,
            COALESCE(t.topup_count, 0),
            COALESCE(t.topup_sum_sgd, 0)
        FROM daily_ledger d
        LEFT JOIN daily_topup t ON d.user_id = t.user_id AND d.activity_date = t.activity_date
        LEFT JOIN (SELECT user_id, activity_date, end_of_day_balance FROM balance) b ON d.user_id = b.user_id AND d.activity_date = b.activity_date
    """)
    counts["user_daily_points"] = conn.execute("SELECT count(*) FROM mart.user_daily_points").fetchone()[0]

    # platform_daily_overview
    conn.execute("DELETE FROM mart.platform_daily_overview")
    conn.execute("""
        INSERT INTO mart.platform_daily_overview (
            activity_date, new_signups, mobile_verified_new, active_users, new_vps_created, vps_terminated, net_vps_change,
            topups_count, topups_sum_sgd, payer_count, points_earned_paid, points_earned_free, points_spent, net_points_delta
        )
        SELECT
            u.activity_date,
            COALESCE(s.new_signups, 0),
            COALESCE(m.mobile_verified_new, 0),
            (SELECT COUNT(DISTINCT user_id) FROM mart.user_daily_activity WHERE activity_date = u.activity_date),
            (SELECT COALESCE(SUM(launch_count), 0) FROM mart.user_daily_activity WHERE activity_date = u.activity_date),
            (SELECT COALESCE(SUM(terminate_count), 0) FROM mart.user_daily_activity WHERE activity_date = u.activity_date),
            (SELECT COALESCE(SUM(launch_count), 0) - COALESCE(SUM(terminate_count), 0) FROM mart.user_daily_activity WHERE activity_date = u.activity_date),
            (SELECT COALESCE(SUM(topup_count), 0) FROM mart.user_daily_points WHERE activity_date = u.activity_date),
            (SELECT COALESCE(SUM(topup_sum_sgd), 0) FROM mart.user_daily_points WHERE activity_date = u.activity_date),
            (SELECT COUNT(DISTINCT user_id) FROM mart.user_daily_points WHERE activity_date = u.activity_date AND topup_count > 0),
            (SELECT COALESCE(SUM(points_earned_paid), 0) FROM mart.user_daily_points WHERE activity_date = u.activity_date),
            (SELECT COALESCE(SUM(points_earned_free), 0) FROM mart.user_daily_points WHERE activity_date = u.activity_date),
            (SELECT COALESCE(SUM(points_spent), 0) FROM mart.user_daily_points WHERE activity_date = u.activity_date),
            (SELECT COALESCE(SUM(net_points_delta), 0) FROM mart.user_daily_points WHERE activity_date = u.activity_date)
        FROM (SELECT DISTINCT activity_date FROM mart.user_daily_activity) u
        LEFT JOIN (SELECT CAST(registration_ts AS DATE) AS activity_date, COUNT(*) AS new_signups FROM core.dim_user GROUP BY 1) s ON u.activity_date = s.activity_date
        LEFT JOIN (SELECT CAST(mobile_verified_at AS DATE) AS activity_date, COUNT(*) AS mobile_verified_new FROM core.dim_user WHERE mobile_verified_at IS NOT NULL GROUP BY 1) m ON u.activity_date = m.activity_date
    """)
    counts["platform_daily_overview"] = conn.execute("SELECT count(*) FROM mart.platform_daily_overview").fetchone()[0]

    # platform_weekly_overview
    conn.execute("DELETE FROM mart.platform_weekly_overview")
    conn.execute("""
        INSERT INTO mart.platform_weekly_overview (
            week_start_date, new_signups, mobile_verified_new, active_users, new_vps_created, vps_terminated, net_vps_change,
            topups_count, topups_sum_sgd, payer_count, points_earned_paid, points_earned_free, points_spent, net_points_delta
        )
        SELECT
            date_trunc('week', activity_date)::DATE AS week_start_date,
            SUM(new_signups),
            SUM(mobile_verified_new),
            SUM(active_users),
            SUM(new_vps_created),
            SUM(vps_terminated),
            SUM(net_vps_change),
            SUM(topups_count),
            SUM(topups_sum_sgd),
            SUM(payer_count),
            SUM(points_earned_paid),
            SUM(points_earned_free),
            SUM(points_spent),
            SUM(net_points_delta)
        FROM mart.platform_daily_overview
        GROUP BY date_trunc('week', activity_date)::DATE
    """)
    counts["platform_weekly_overview"] = conn.execute("SELECT count(*) FROM mart.platform_weekly_overview").fetchone()[0]

    for name, c in counts.items():
        logger.info(f"Mart table {name}: {c} rows")
    return counts


# ============================================
# IP Geolocation Enrichment
# ============================================

IP_BATCH_SIZE = 100       # ip-api.com free tier limit per request
IP_REQUEST_DELAY = 1.5    # seconds between batches (stay under 45 req/min)


def _fetch_ip_batch(ips: List[str], log: logging.Logger) -> Dict[str, Dict[str, str]]:
    """
    Call ip-api.com/batch for up to 100 IPs.

    Returns dict mapping ip -> {"country_code": "SG", "country_name": "Singapore"}.
    Only includes IPs where the lookup succeeded.
    """
    payload = [{"query": ip, "fields": "query,status,countryCode,country"} for ip in ips]

    try:
        resp = requests.post("http://ip-api.com/batch", json=payload, timeout=30)
        resp.raise_for_status()
        results = resp.json()
    except Exception as exc:
        log.error(f"ip-api.com batch request failed: {exc}")
        return {}

    mapping: Dict[str, Dict[str, str]] = {}
    for item in results:
        if item.get("status") == "success":
            mapping[item["query"]] = {
                "country_code": item.get("countryCode", ""),
                "country_name": item.get("country", ""),
            }
    return mapping


def enrich_ip_geo(
    conn: duckdb.DuckDBPyConnection,
    log: logging.Logger,
    dry_run: bool = False,
) -> int:
    """
    Enrich core.dim_user rows that have registration_ip but no country data.

    Uses the free ip-api.com batch endpoint (max 100 IPs/request).
    Returns the number of distinct IPs successfully enriched.
    """
    # Find IPs that still need enrichment
    rows = conn.execute("""
        SELECT DISTINCT registration_ip
        FROM core.dim_user
        WHERE registration_ip IS NOT NULL
          AND registration_ip != ''
          AND (registration_country_code IS NULL OR registration_country_code = '')
    """).fetchall()

    unique_ips = [r[0] for r in rows]
    log.info(f"IP Geo: {len(unique_ips)} unique IPs need enrichment")

    if not unique_ips:
        return 0

    if dry_run:
        log.info("[DRY-RUN] Would enrich IPs via ip-api.com")
        return len(unique_ips)

    # Batch lookup
    all_results: Dict[str, Dict[str, str]] = {}
    total_batches = (len(unique_ips) + IP_BATCH_SIZE - 1) // IP_BATCH_SIZE

    for batch_idx in range(total_batches):
        start = batch_idx * IP_BATCH_SIZE
        batch_ips = unique_ips[start : start + IP_BATCH_SIZE]

        log.info(f"  Batch {batch_idx + 1}/{total_batches}: {len(batch_ips)} IPs")
        batch_results = _fetch_ip_batch(batch_ips, log)
        all_results.update(batch_results)

        # Rate-limit (skip delay on last batch)
        if batch_idx < total_batches - 1:
            time.sleep(IP_REQUEST_DELAY)

    log.info(f"IP Geo: resolved {len(all_results)}/{len(unique_ips)} IPs")

    # Write results back to dim_user
    enriched = 0
    for ip, geo in all_results.items():
        conn.execute(
            """
            UPDATE core.dim_user
            SET registration_country_code = ?,
                registration_country_name = ?
            WHERE registration_ip = ?
              AND (registration_country_code IS NULL OR registration_country_code = '')
            """,
            [geo["country_code"], geo["country_name"], ip],
        )
        enriched += 1

    log.info(f"IP Geo: updated {enriched} distinct IPs in core.dim_user")
    return enriched


# ============================================
# Main ETL entry
# ============================================


def run_user_logs_etl(
    duckdb_path: str,
    csv_dir: str,
    rebuild_marts: bool = True,
    dry_run: bool = False,
    logger: Optional[logging.Logger] = None,
) -> Dict[str, Any]:
    """
    Run full user logs ETL: create schemas/tables, load CSVs, refresh user_account_state and marts.

    Args:
        duckdb_path: Path to DuckDB file (e.g. data/warehouse.duckdb)
        csv_dir: Directory containing required CSV files
        rebuild_marts: If True, refresh mart tables after core load
        dry_run: If True, validate and log only; no writes
        logger: Optional logger

    Returns:
        Stats dict with row counts and errors
    """
    log = logger or logging.getLogger(__name__)
    project_root = Path(__file__).resolve().parent.parent
    csv_path = Path(csv_dir) if Path(csv_dir).is_absolute() else project_root / csv_dir
    db_path = Path(duckdb_path) if Path(duckdb_path).is_absolute() else project_root / duckdb_path

    stats: Dict[str, Any] = {
        "start_time": datetime.now(),
        "end_time": None,
        "dim_user": 0,
        "fact_user_activity": 0,
        "fact_points_ledger": 0,
        "fact_payment_topup": 0,
        "user_account_state": 0,
        "mart": {},
        "errors": [],
    }

    ok, errs = validate_csv_dir(csv_path, log)
    if not ok:
        stats["errors"] = errs
        for e in errs:
            log.error(e)
        return stats

    if dry_run:
        log.info("DRY-RUN: no data will be written")

    conn = duckdb.connect(str(db_path))
    try:
        create_schemas_and_tables(conn)
        load_customer(conn, csv_path, log, dry_run=dry_run)
        apply_mobile_verified(conn, csv_path, log, dry_run=dry_run)

        # Enrich registration IPs with country data (calls ip-api.com)
        try:
            enrich_ip_geo(conn, log, dry_run=dry_run)
        except Exception as geo_err:
            # Non-fatal: geo enrichment failure should not block the ETL
            log.warning(f"IP geo-enrichment failed (non-fatal): {geo_err}")
            stats["errors"].append(f"IP geo-enrichment skipped: {geo_err}")

        stats["dim_user"] = conn.execute("SELECT count(*) FROM core.dim_user").fetchone()[0]

        load_activity(conn, csv_path, log, dry_run=dry_run)
        stats["fact_user_activity"] = conn.execute("SELECT count(*) FROM core.fact_user_activity").fetchone()[0]

        load_payment(conn, csv_path, log, dry_run=dry_run)
        load_point(conn, csv_path, log, dry_run=dry_run)
        load_redeem_mobile_verification(conn, csv_path, log, dry_run=dry_run)
        load_redeem_promocode(conn, csv_path, log, dry_run=dry_run)
        load_redeem_reload(conn, csv_path, log, dry_run=dry_run)
        stats["fact_points_ledger"] = conn.execute("SELECT count(*) FROM core.fact_points_ledger").fetchone()[0]
        stats["fact_payment_topup"] = conn.execute("SELECT count(*) FROM core.fact_payment_topup").fetchone()[0]

        refresh_user_account_state(conn, log, dry_run=dry_run)
        stats["user_account_state"] = conn.execute("SELECT count(*) FROM core.user_account_state").fetchone()[0]

        if rebuild_marts:
            stats["mart"] = refresh_mart_tables(conn, log, dry_run=dry_run)

        conn.commit()
    except Exception as e:
        conn.rollback()
        log.exception("ETL failed")
        stats["errors"].append(str(e))
    finally:
        conn.close()

    stats["end_time"] = datetime.now()
    stats["duration_seconds"] = (stats["end_time"] - stats["start_time"]).total_seconds()
    return stats
