"""
MBA App Performance PDF Report Generator
========================================

Generates a professional PDF report for leadership reviews, covering:
    1. Executive Summary (top-line KPIs)
    2. Funnel Performance (signup -> verified -> VPS -> paid)
    3. Country Conversion and Monetization
    4. Revenue and Package Mix
    5. Engagement and Product Usage
    6. Strategic MBA Recommendations

Output (written to analysis/reports/output/):
    - app_performance_report.pdf

Usage:
    python -m analysis.reports.app_performance_report
    python analysis/reports/app_performance_report.py
"""

import logging
import argparse
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import duckdb
import pandas as pd
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

# ── Paths ───────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_ROOT / "data" / "warehouse.duckdb"
OUTPUT_DIR = Path(__file__).resolve().parent / "output"

# ── Logging ─────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("app_performance_report")

# ── Constants ───────────────────────────────────────────────────
POINTS_PER_SGD = 144  # conversion rate: 1 SGD = 144 points


# ============================================================
# Helper: run a query and return a DataFrame
# ============================================================

def query(conn: duckdb.DuckDBPyConnection, sql: str) -> pd.DataFrame:
    """Execute SQL against the DuckDB connection and return a DataFrame."""
    try:
        return conn.execute(sql).fetchdf()
    except Exception as exc:
        logger.error(f"Query failed: {exc}\nSQL: {sql[:200]}…")
        return pd.DataFrame()


def _parse_period(month: str = "", start_date: str = "", end_date: str = "") -> tuple[str, str]:
    """
    Resolve date period from either:
    - month in YYYY-MM
    - explicit start_date/end_date in YYYY-MM-DD
    """
    if month:
        try:
            month_start = datetime.strptime(f"{month}-01", "%Y-%m-%d").date()
        except ValueError as exc:
            raise ValueError("Invalid --month format. Use YYYY-MM (e.g., 2026-01).") from exc

        if month_start.month == 12:
            next_month = month_start.replace(year=month_start.year + 1, month=1, day=1)
        else:
            next_month = month_start.replace(month=month_start.month + 1, day=1)
        month_end = next_month - pd.Timedelta(days=1)
        return month_start.strftime("%Y-%m-%d"), month_end.strftime("%Y-%m-%d")

    if bool(start_date) != bool(end_date):
        raise ValueError("Provide both --start-date and --end-date together.")
    if start_date and end_date:
        return start_date, end_date

    # Default: current month to-date
    today = datetime.now().date()
    month_start = today.replace(day=1)
    return month_start.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")


# ============================================================
# Section 1: Executive Summary KPIs
# ============================================================

def build_executive_summary(
    conn: duckdb.DuckDBPyConnection,
    start_date: str,
    end_date: str,
) -> Dict[str, Any]:
    """
    Compute top-line KPIs for the entire platform lifetime.

    Returns a dict with:
        total_users, verified_users, paying_users, vps_users,
        total_revenue_sgd, total_topups, avg_transaction_sgd,
        total_points_earned, total_points_spent, points_velocity,
        first_date, last_date, active_days, countries
    """
    logger.info("Building executive summary…")

    # ── User counts ─────────────────────────────────────────────
    user_stats = query(conn, f"""
        SELECT
            COUNT(*)                                                  AS total_users,
            SUM(CASE WHEN mobile_verified THEN 1 ELSE 0 END)         AS verified_users,
            COUNT(DISTINCT registration_country_name)                 AS country_count
        FROM core.dim_user
        WHERE DATE(registration_ts) BETWEEN DATE '{start_date}' AND DATE '{end_date}'
    """)

    paying_stats = query(conn, f"""
        SELECT
            COUNT(DISTINCT s.user_id)                                AS paying_users,
            COUNT(DISTINCT CASE WHEN s.current_vps_live > 0
                                THEN s.user_id END)                  AS vps_users
        FROM core.user_account_state s
        JOIN core.dim_user u ON s.user_id = u.user_id
        WHERE DATE(u.registration_ts) BETWEEN DATE '{start_date}' AND DATE '{end_date}'
    """)

    paying_only = query(conn, f"""
        SELECT COUNT(DISTINCT s.user_id) AS paying_users
        FROM core.user_account_state s
        JOIN core.dim_user u ON s.user_id = u.user_id
        WHERE s.total_points_earned_paid > 0
          AND DATE(u.registration_ts) BETWEEN DATE '{start_date}' AND DATE '{end_date}'
    """)

    # ── Revenue ─────────────────────────────────────────────────
    revenue = query(conn, f"""
        SELECT
            COALESCE(SUM(cash_amount_sgd), 0)  AS total_revenue_sgd,
            COUNT(*)                            AS total_topups,
            COALESCE(AVG(cash_amount_sgd), 0)  AS avg_transaction_sgd,
            MIN(payment_ts)                     AS first_payment,
            MAX(payment_ts)                     AS last_payment
        FROM core.fact_payment_topup
        WHERE DATE(payment_ts) BETWEEN DATE '{start_date}' AND DATE '{end_date}'
    """)

    # ── Points economy ──────────────────────────────────────────
    points = query(conn, f"""
        SELECT
            COALESCE(SUM(CASE WHEN points_delta > 0 THEN points_delta ELSE 0 END), 0)
                AS total_points_earned,
            COALESCE(SUM(CASE WHEN points_delta < 0 THEN ABS(points_delta) ELSE 0 END), 0)
                AS total_points_spent
        FROM core.fact_points_ledger
        WHERE DATE(ledger_ts) BETWEEN DATE '{start_date}' AND DATE '{end_date}'
    """)

    # ── Date range ──────────────────────────────────────────────
    date_range = query(conn, f"""
        SELECT MIN(activity_date) AS first_date, MAX(activity_date) AS last_date,
               COUNT(DISTINCT activity_date) AS active_days
        FROM mart.platform_daily_overview
        WHERE activity_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
    """)

    # ── Assemble ────────────────────────────────────────────────
    total_earned = int(points.iloc[0]["total_points_earned"]) if not points.empty else 0
    total_spent  = int(points.iloc[0]["total_points_spent"])  if not points.empty else 0

    kpis: Dict[str, Any] = {
        "total_users":        int(user_stats.iloc[0]["total_users"]),
        "verified_users":     int(user_stats.iloc[0]["verified_users"]),
        "paying_users":       int(paying_only.iloc[0]["paying_users"]),
        "vps_users":          int(paying_stats.iloc[0]["vps_users"]),
        "country_count":      int(user_stats.iloc[0]["country_count"]),
        "total_revenue_sgd":  float(revenue.iloc[0]["total_revenue_sgd"]),
        "total_topups":       int(revenue.iloc[0]["total_topups"]),
        "avg_transaction_sgd": float(revenue.iloc[0]["avg_transaction_sgd"]),
        "total_points_earned": total_earned,
        "total_points_spent":  total_spent,
        "points_velocity":    round(total_spent / total_earned * 100, 1) if total_earned else 0,
        "first_date":         str(date_range.iloc[0]["first_date"])[:10] if not date_range.empty else "N/A",
        "last_date":          str(date_range.iloc[0]["last_date"])[:10]  if not date_range.empty else "N/A",
        "active_days":        int(date_range.iloc[0]["active_days"])      if not date_range.empty else 0,
    }

    # Derived MBA metrics
    kpis["verification_rate"] = round(kpis["verified_users"] / kpis["total_users"] * 100, 1) if kpis["total_users"] else 0
    kpis["conversion_rate"]   = round(kpis["paying_users"]   / kpis["total_users"] * 100, 1) if kpis["total_users"] else 0
    kpis["arpu"]              = round(kpis["total_revenue_sgd"] / kpis["total_users"], 2)      if kpis["total_users"] else 0
    kpis["arppu"]             = round(kpis["total_revenue_sgd"] / kpis["paying_users"], 2)     if kpis["paying_users"] else 0
    kpis["vps_adoption_rate"] = round(kpis["vps_users"]       / kpis["total_users"] * 100, 1)  if kpis["total_users"] else 0
    kpis["revenue_per_day"]   = round(kpis["total_revenue_sgd"] / kpis["active_days"], 2)      if kpis["active_days"] else 0

    return kpis


# ============================================================
# Section 2: Funnel Analysis
# ============================================================

def build_funnel(
    conn: duckdb.DuckDBPyConnection,
    start_date: str,
    end_date: str,
) -> Dict[str, Any]:
    """
    Build the full-lifecycle funnel:
        signup → mobile verified → created VPS → paid money

    Returns a dict with counts & step conversion rates.
    """
    logger.info("Building funnel analysis…")

    funnel = query(conn, f"""
        WITH base AS (
            SELECT
                u.user_id,
                u.mobile_verified,
                COALESCE(s.total_launch_count, 0)       AS launches,
                COALESCE(s.total_points_earned_paid, 0)  AS paid_points
            FROM core.dim_user u
            LEFT JOIN core.user_account_state s ON u.user_id = s.user_id
            WHERE DATE(u.registration_ts) BETWEEN DATE '{start_date}' AND DATE '{end_date}'
        )
        SELECT
            COUNT(*)                                             AS signups,
            SUM(CASE WHEN mobile_verified THEN 1 ELSE 0 END)    AS verified,
            SUM(CASE WHEN launches > 0 THEN 1 ELSE 0 END)       AS created_vps,
            SUM(CASE WHEN paid_points > 0 THEN 1 ELSE 0 END)    AS paid
        FROM base
    """)

    if funnel.empty:
        return {}

    row = funnel.iloc[0]
    signups     = int(row["signups"])
    verified    = int(row["verified"])
    created_vps = int(row["created_vps"])
    paid        = int(row["paid"])

    return {
        "signups":               signups,
        "verified":              verified,
        "created_vps":           created_vps,
        "paid":                  paid,
        "signup_to_verified":    round(verified / signups * 100, 1)      if signups else 0,
        "verified_to_vps":       round(created_vps / verified * 100, 1)  if verified else 0,
        "vps_to_paid":           round(paid / created_vps * 100, 1)      if created_vps else 0,
        "overall_conversion":    round(paid / signups * 100, 1)          if signups else 0,
    }


# ============================================================
# Section 3: Country Performance
# ============================================================

def build_country_analysis(
    conn: duckdb.DuckDBPyConnection,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """
    Per-country funnel analysis:
        signups, verified, VPS, paid, revenue, ARPU, ARPPU, conversion rates.

    Returns a DataFrame sorted by revenue descending.
    """
    logger.info("Building country analysis…")

    df = query(conn, f"""
        WITH user_base AS (
            SELECT
                u.user_id,
                COALESCE(NULLIF(u.registration_country_name, ''), 'Unknown') AS country,
                u.mobile_verified,
                COALESCE(s.total_launch_count, 0)       AS launches,
                COALESCE(s.total_points_earned_paid, 0)  AS paid_points,
                COALESCE(s.total_points_spent, 0)        AS spent_points,
                COALESCE(s.current_vps_live, 0)          AS live_vps
            FROM core.dim_user u
            LEFT JOIN core.user_account_state s ON u.user_id = s.user_id
            WHERE DATE(u.registration_ts) BETWEEN DATE '{start_date}' AND DATE '{end_date}'
        ),
        country_payments AS (
            SELECT
                COALESCE(NULLIF(u.registration_country_name, ''), 'Unknown') AS country,
                SUM(p.cash_amount_sgd)   AS total_revenue_sgd,
                COUNT(*)                 AS total_topups
            FROM core.fact_payment_topup p
            JOIN core.dim_user u ON p.user_id = u.user_id
            WHERE DATE(p.payment_ts) BETWEEN DATE '{start_date}' AND DATE '{end_date}'
            GROUP BY country
        )
        SELECT
            ub.country,
            COUNT(*)                                                   AS signups,
            SUM(CASE WHEN ub.mobile_verified THEN 1 ELSE 0 END)       AS verified,
            SUM(CASE WHEN ub.launches > 0 THEN 1 ELSE 0 END)          AS created_vps,
            SUM(CASE WHEN ub.paid_points > 0 THEN 1 ELSE 0 END)       AS paying_users,
            SUM(CASE WHEN ub.live_vps > 0 THEN 1 ELSE 0 END)          AS active_vps_users,
            COALESCE(cp.total_revenue_sgd, 0)                          AS revenue_sgd,
            COALESCE(cp.total_topups, 0)                               AS topup_count
        FROM user_base ub
        LEFT JOIN country_payments cp ON ub.country = cp.country
        GROUP BY ub.country, cp.total_revenue_sgd, cp.total_topups
        ORDER BY revenue_sgd DESC
    """)

    if df.empty:
        return df

    # Derived columns
    df["verification_rate"]  = (df["verified"] / df["signups"] * 100).round(1)
    df["vps_conversion"]     = (df["created_vps"] / df["signups"] * 100).round(1)
    df["paid_conversion"]    = (df["paying_users"] / df["signups"] * 100).round(1)
    df["arpu"]               = (df["revenue_sgd"] / df["signups"]).round(2)
    df["arppu"]              = df.apply(
        lambda r: round(r["revenue_sgd"] / r["paying_users"], 2) if r["paying_users"] > 0 else 0,
        axis=1,
    )
    df["avg_topup_sgd"]      = df.apply(
        lambda r: round(r["revenue_sgd"] / r["topup_count"], 2) if r["topup_count"] > 0 else 0,
        axis=1,
    )
    df["revenue_share_pct"]  = (df["revenue_sgd"] / df["revenue_sgd"].sum() * 100).round(1) if df["revenue_sgd"].sum() > 0 else 0

    return df


# ============================================================
# Section 4: Revenue & Monetization Deep-Dive
# ============================================================

def build_revenue_analysis(
    conn: duckdb.DuckDBPyConnection,
    start_date: str,
    end_date: str,
) -> Dict[str, Any]:
    """
    Revenue analysis: package mix, daily trends, paying user behavior.
    """
    logger.info("Building revenue analysis…")

    # Package mix
    package_mix = query(conn, f"""
        SELECT
            package_code,
            COUNT(*) AS transactions,
            SUM(cash_amount_sgd) AS revenue_sgd,
            AVG(cash_amount_sgd) AS avg_amount
        FROM core.fact_payment_topup
        WHERE package_code IS NOT NULL AND package_code != ''
          AND DATE(payment_ts) BETWEEN DATE '{start_date}' AND DATE '{end_date}'
        GROUP BY package_code
        ORDER BY revenue_sgd DESC
    """)

    # Daily revenue
    daily_revenue = query(conn, f"""
        SELECT
            activity_date AS date,
            COALESCE(topups_sum_sgd, 0) AS revenue_sgd,
            COALESCE(topups_count, 0)   AS topups,
            COALESCE(payer_count, 0)    AS payers,
            COALESCE(new_signups, 0)    AS signups
        FROM mart.platform_daily_overview
        WHERE activity_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
        ORDER BY activity_date
    """)

    # Paying user stats
    payer_stats = query(conn, f"""
        SELECT
            COUNT(DISTINCT user_id)                               AS total_payers,
            SUM(cash_amount_sgd)                                  AS total_revenue,
            AVG(cash_amount_sgd)                                  AS avg_per_txn,
            -- Users who paid more than once
            (SELECT COUNT(*) FROM (
                SELECT user_id, COUNT(*) AS txn_count
                FROM core.fact_payment_topup
                WHERE DATE(payment_ts) BETWEEN DATE '{start_date}' AND DATE '{end_date}'
                GROUP BY user_id
                HAVING COUNT(*) > 1
            )) AS repeat_payers,
            -- Users who paid only once
            (SELECT COUNT(*) FROM (
                SELECT user_id, COUNT(*) AS txn_count
                FROM core.fact_payment_topup
                WHERE DATE(payment_ts) BETWEEN DATE '{start_date}' AND DATE '{end_date}'
                GROUP BY user_id
                HAVING COUNT(*) = 1
            )) AS one_time_payers
        FROM core.fact_payment_topup
        WHERE DATE(payment_ts) BETWEEN DATE '{start_date}' AND DATE '{end_date}'
    """)

    # Top spending users
    top_spenders = query(conn, f"""
        SELECT
            p.user_id,
            u.registration_country_name AS country,
            COUNT(*) AS txn_count,
            SUM(p.cash_amount_sgd) AS total_spent_sgd,
            MIN(p.payment_ts) AS first_payment,
            MAX(p.payment_ts) AS last_payment
        FROM core.fact_payment_topup p
        JOIN core.dim_user u ON p.user_id = u.user_id
        WHERE DATE(p.payment_ts) BETWEEN DATE '{start_date}' AND DATE '{end_date}'
        GROUP BY p.user_id, u.registration_country_name
        ORDER BY total_spent_sgd DESC
        LIMIT 10
    """)

    return {
        "package_mix":    package_mix,
        "daily_revenue":  daily_revenue,
        "payer_stats":    payer_stats,
        "top_spenders":   top_spenders,
    }


# ============================================================
# Section 5: Product Engagement
# ============================================================

def build_engagement_analysis(
    conn: duckdb.DuckDBPyConnection,
    start_date: str,
    end_date: str,
) -> Dict[str, Any]:
    """
    Engagement analysis: DAU/MAU, VPS usage patterns, points velocity.
    """
    logger.info("Building engagement analysis…")

    # DAU trend
    dau = query(conn, f"""
        SELECT
            activity_date AS date,
            COALESCE(active_users, 0) AS dau
        FROM mart.platform_daily_overview
        WHERE activity_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
        ORDER BY activity_date
    """)

    # Monthly active users (approximate via user_daily_activity)
    mau = query(conn, f"""
        SELECT
            DATE_TRUNC('month', activity_date) AS month,
            COUNT(DISTINCT user_id) AS mau
        FROM mart.user_daily_activity
        WHERE activity_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
        GROUP BY DATE_TRUNC('month', activity_date)
        ORDER BY month
    """)

    # VPS usage distribution
    vps_dist = query(conn, f"""
        SELECT
            CASE
                WHEN current_vps_live = 0 THEN '0 (none)'
                WHEN current_vps_live = 1 THEN '1'
                WHEN current_vps_live BETWEEN 2 AND 3 THEN '2-3'
                WHEN current_vps_live BETWEEN 4 AND 10 THEN '4-10'
                ELSE '10+'
            END AS vps_bucket,
            COUNT(*) AS user_count
        FROM core.user_account_state s
        JOIN core.dim_user u ON s.user_id = u.user_id
        WHERE DATE(u.registration_ts) BETWEEN DATE '{start_date}' AND DATE '{end_date}'
        GROUP BY vps_bucket
        ORDER BY MIN(current_vps_live)
    """)

    # Activity type breakdown
    activity_types = query(conn, f"""
        SELECT
            activity_type,
            COUNT(*) AS event_count,
            COUNT(DISTINCT user_id) AS unique_users
        FROM core.fact_user_activity
        WHERE DATE(event_ts) BETWEEN DATE '{start_date}' AND DATE '{end_date}'
        GROUP BY activity_type
        ORDER BY event_count DESC
    """)

    return {
        "dau":             dau,
        "mau":             mau,
        "vps_distribution": vps_dist,
        "activity_types":  activity_types,
    }


# ============================================================
# Section 6: Weekly Funnel Trends
# ============================================================

def build_weekly_funnel(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Week-by-week funnel snapshot.
    """
    logger.info("Building weekly funnel trends…")

    df = query(conn, """
        SELECT
            week_start_date,
            COALESCE(new_signups, 0) AS signups,
            COALESCE(mobile_verified_new, 0) AS verified,
            COALESCE(new_vps_created, 0) AS vps_created,
            COALESCE(topups_count, 0) AS topups,
            COALESCE(topups_sum_sgd, 0) AS revenue_sgd,
            COALESCE(payer_count, 0) AS payers,
            COALESCE(active_users, 0) AS active_users
        FROM mart.platform_weekly_overview
        ORDER BY week_start_date
    """)
    return df


# ============================================================
# Section 7: Signals & Recommendations
# ============================================================

def generate_recommendations(
    kpis: Dict[str, Any],
    funnel: Dict[str, Any],
    country_df: pd.DataFrame,
) -> List[str]:
    """
    Generate MBA-style data-driven recommendations from the analysis.
    """
    logger.info("Generating recommendations…")
    recs: List[str] = []

    # ── Funnel bottleneck ───────────────────────────────────────
    if funnel.get("signup_to_verified", 0) < 50:
        recs.append(
            f"**Verification Bottleneck** — Only {funnel['signup_to_verified']}% of signups "
            f"verify their mobile number (target: 60-80%). Consider simplifying the "
            f"verification flow or adding incentives (bonus points) for verification."
        )

    if funnel.get("vps_to_paid", 0) < 15:
        recs.append(
            f"**Monetization Gap** — Only {funnel['vps_to_paid']}% of VPS creators "
            f"convert to paid (target: 20-40%). Evaluate whether the free tier is too "
            f"generous or if pricing is a barrier. A/B test trial-to-paid nudges."
        )

    if funnel.get("verified_to_vps", 0) < 20:
        recs.append(
            f"**Activation Friction** — Only {funnel['verified_to_vps']}% of verified "
            f"users create a VPS. Investigate onboarding UX: add tutorials, one-click "
            f"VPS templates, or a guided first-launch experience."
        )

    # ── Revenue concentration ───────────────────────────────────
    if not country_df.empty:
        top3_revenue = country_df.head(3)["revenue_sgd"].sum()
        total_revenue = country_df["revenue_sgd"].sum()
        top3_share = round(top3_revenue / total_revenue * 100, 1) if total_revenue else 0

        if top3_share > 80:
            top3_names = ", ".join(country_df.head(3)["country"].tolist())
            recs.append(
                f"**Revenue Concentration Risk** — Top 3 countries ({top3_names}) "
                f"account for {top3_share}% of revenue. Diversify acquisition into "
                f"high-potential but under-penetrated markets."
            )

        # Find high-signup, low-conversion countries
        high_signup_low_convert = country_df[
            (country_df["signups"] >= 50) & (country_df["paid_conversion"] < 1)
        ]
        if not high_signup_low_convert.empty:
            names = ", ".join(high_signup_low_convert["country"].head(3).tolist())
            recs.append(
                f"**Leaking Markets** — {names} have 50+ signups but <1% paid "
                f"conversion. Investigate product-market fit, localization, or "
                f"pricing adjustments for these regions."
            )

    # ── Points economy ──────────────────────────────────────────
    velocity = kpis.get("points_velocity", 0)
    if velocity < 30:
        recs.append(
            f"**Low Points Velocity** — Only {velocity}% of earned points are being "
            f"consumed. Large unspent balances may indicate inactive users or "
            f"insufficient VPS usage incentives. Consider expiry policies or "
            f"usage-based promotions."
        )
    elif velocity > 100:
        recs.append(
            f"**Points Deficit** — Users are spending more points ({velocity}%) than "
            f"earning. This is unsustainable long-term. Review pricing or increase "
            f"top-up incentives."
        )

    # ── ARPPU vs ARPU gap ──────────────────────────────────────
    if kpis.get("arppu", 0) > 0 and kpis.get("arpu", 0) > 0:
        gap = kpis["arppu"] / kpis["arpu"]
        if gap > 20:
            recs.append(
                f"**Whale Dependency** — ARPPU (${kpis['arppu']:.2f}) is {gap:.0f}x "
                f"higher than ARPU (${kpis['arpu']:.2f}), indicating heavy reliance "
                f"on a small paying base. Focus on broadening the paying user base "
                f"with lower-tier packages or freemium-to-paid nudges."
            )

    # ── Generic growth ──────────────────────────────────────────
    if kpis.get("conversion_rate", 0) < 3:
        recs.append(
            f"**Overall Conversion** — At {kpis['conversion_rate']}% (signup-to-paid), "
            f"the funnel underperforms SaaS benchmarks (3-5%). Prioritize reducing "
            f"friction at the weakest funnel step identified above."
        )

    if not recs:
        recs.append("No critical issues detected. Continue monitoring funnel health weekly.")

    return recs


# ============================================================
# Report Formatter: Professional PDF
# ============================================================

def _to_table(data: List[List[Any]], col_widths: List[float]) -> Table:
    """Build a consistently styled report table."""
    table = Table(data, colWidths=col_widths, repeatRows=1)
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f4e79")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, 0), 9),
                ("FONTSIZE", (0, 1), (-1, -1), 8),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#cccccc")),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f7f9fc")]),
                ("ALIGN", (1, 1), (-1, -1), "RIGHT"),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ]
        )
    )
    return table


def build_pdf_report(
    output_path: Path,
    kpis: Dict[str, Any],
    funnel: Dict[str, Any],
    country_df: pd.DataFrame,
    rev_data: Dict[str, Any],
    engagement: Dict[str, Any],
    recommendations: List[str],
    generated_at: str,
) -> None:
    """Render and write a professional PDF report."""
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "TitleStyle",
        parent=styles["Title"],
        fontName="Helvetica-Bold",
        fontSize=22,
        textColor=colors.HexColor("#16355c"),
        spaceAfter=10,
    )
    subtitle_style = ParagraphStyle(
        "SubTitleStyle",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=10,
        textColor=colors.HexColor("#4f4f4f"),
        spaceAfter=8,
    )
    section_style = ParagraphStyle(
        "SectionStyle",
        parent=styles["Heading2"],
        fontName="Helvetica-Bold",
        fontSize=13,
        textColor=colors.HexColor("#1f4e79"),
        spaceBefore=8,
        spaceAfter=6,
    )
    body_style = ParagraphStyle(
        "BodyStyle",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=9,
        leading=13,
    )
    bullet_style = ParagraphStyle(
        "BulletStyle",
        parent=body_style,
        leftIndent=12,
        bulletIndent=0,
        spaceAfter=3,
    )

    doc = SimpleDocTemplate(
        str(output_path),
        pagesize=A4,
        topMargin=1.4 * cm,
        bottomMargin=1.3 * cm,
        leftMargin=1.5 * cm,
        rightMargin=1.5 * cm,
        title="RS Analytics MBA App Performance Report",
        author="RS Analytics",
    )

    story = []
    story.append(Paragraph("RS Analytics App Performance Report", title_style))
    story.append(Paragraph("MBA Executive Review", section_style))
    story.append(
        Paragraph(
            f"Generated: {generated_at}<br/>"
            f"Period: {kpis['first_date']} to {kpis['last_date']} ({kpis['active_days']} active days)<br/>"
            f"Market Coverage: {kpis['country_count']} countries",
            subtitle_style,
        )
    )
    story.append(Spacer(1, 8))

    # Executive KPI table
    story.append(Paragraph("1. Executive Summary", section_style))
    summary_rows = [
        ["Metric", "Value"],
        ["Total Users", f"{kpis['total_users']:,}"],
        ["Verified Users", f"{kpis['verified_users']:,} ({kpis['verification_rate']}%)"],
        ["Paying Users", f"{kpis['paying_users']:,} ({kpis['conversion_rate']}%)"],
        ["Active VPS Users", f"{kpis['vps_users']:,} ({kpis['vps_adoption_rate']}%)"],
        ["Total Revenue (SGD)", f"${kpis['total_revenue_sgd']:,.2f}"],
        ["Average Transaction Value", f"${kpis['avg_transaction_sgd']:,.2f}"],
        ["ARPU", f"${kpis['arpu']:,.2f}"],
        ["ARPPU", f"${kpis['arppu']:,.2f}"],
        ["Points Velocity", f"{kpis['points_velocity']}%"],
    ]
    story.append(_to_table(summary_rows, [8.5 * cm, 6.5 * cm]))
    story.append(Spacer(1, 8))

    # Funnel
    story.append(Paragraph("2. Funnel Performance", section_style))
    funnel_rows = [
        ["Stage", "Users", "Conversion %", "Drop-off"],
        ["Sign Up", f"{funnel['signups']:,}", "100.0%", "-"],
        ["Mobile Verified", f"{funnel['verified']:,}", f"{funnel['signup_to_verified']:.1f}%", f"{funnel['signups'] - funnel['verified']:,}"],
        ["Created VPS", f"{funnel['created_vps']:,}", f"{funnel['verified_to_vps']:.1f}%", f"{funnel['verified'] - funnel['created_vps']:,}"],
        ["Paid Money", f"{funnel['paid']:,}", f"{funnel['vps_to_paid']:.1f}%", f"{funnel['created_vps'] - funnel['paid']:,}"],
        ["Overall (Sign Up -> Paid)", f"{funnel['paid']:,}", f"{funnel['overall_conversion']:.1f}%", f"{funnel['signups'] - funnel['paid']:,}"],
    ]
    story.append(_to_table(funnel_rows, [6.5 * cm, 3.0 * cm, 3.5 * cm, 2.0 * cm]))
    story.append(Spacer(1, 8))

    # Country performance
    story.append(Paragraph("3. Country Conversion and Monetization", section_style))
    if country_df.empty:
        story.append(Paragraph("No country-level data found.", body_style))
    else:
        top_country = country_df.sort_values("revenue_sgd", ascending=False).head(12)
        country_rows = [["Country", "Signups", "Paid %", "Revenue", "ARPPU"]]
        for _, row in top_country.iterrows():
            country_rows.append(
                [
                    str(row["country"]),
                    f"{int(row['signups']):,}",
                    f"{float(row['paid_conversion']):.1f}%",
                    f"${float(row['revenue_sgd']):,.0f}",
                    f"${float(row['arppu']):,.2f}",
                ]
            )
        story.append(_to_table(country_rows, [5.0 * cm, 2.7 * cm, 2.2 * cm, 3.0 * cm, 2.1 * cm]))
        story.append(Spacer(1, 4))

        # Country insights text
        paid_countries = int((country_df["paying_users"] > 0).sum())
        zero_revenue_signups = int(country_df[country_df["paying_users"] == 0]["signups"].sum())
        story.append(
            Paragraph(
                f"Countries with at least one payer: <b>{paid_countries}</b> of {len(country_df)}. "
                f"Signups from zero-revenue countries: <b>{zero_revenue_signups:,}</b>.",
                body_style,
            )
        )
    story.append(Spacer(1, 8))

    # Revenue section
    story.append(Paragraph("4. Revenue and Package Mix", section_style))
    payer_stats = rev_data["payer_stats"]
    if not payer_stats.empty:
        ps = payer_stats.iloc[0]
        total_payers = int(ps["total_payers"]) if pd.notna(ps["total_payers"]) else 0
        repeat_payers = int(ps["repeat_payers"]) if pd.notna(ps["repeat_payers"]) else 0
        repeat_pct = round(100 * repeat_payers / total_payers, 1) if total_payers else 0.0
        rev_rows = [
            ["Revenue Metric", "Value"],
            ["Total Payers", f"{total_payers:,}"],
            ["Repeat Payers (2+)", f"{repeat_payers:,} ({repeat_pct}%)"],
            ["One-time Payers", f"{int(ps['one_time_payers']):,}"],
            ["Avg Transaction", f"${float(ps['avg_per_txn']):,.2f}"],
        ]
        story.append(_to_table(rev_rows, [8.5 * cm, 6.5 * cm]))
        story.append(Spacer(1, 4))

    package_mix = rev_data["package_mix"].head(10)
    if not package_mix.empty:
        pkg_rows = [["Package", "Transactions", "Revenue", "Avg Ticket"]]
        for _, row in package_mix.iterrows():
            pkg_rows.append(
                [
                    str(row["package_code"]),
                    f"{int(row['transactions']):,}",
                    f"${float(row['revenue_sgd']):,.0f}",
                    f"${float(row['avg_amount']):,.2f}",
                ]
            )
        story.append(_to_table(pkg_rows, [4.5 * cm, 3.2 * cm, 3.8 * cm, 3.5 * cm]))
    story.append(Spacer(1, 8))

    # Engagement
    story.append(Paragraph("5. Engagement and Product Usage", section_style))
    dau = engagement["dau"]
    mau = engagement["mau"]
    avg_dau = float(dau["dau"].mean()) if not dau.empty else 0.0
    peak_dau = int(dau["dau"].max()) if not dau.empty else 0
    latest_mau = int(mau.iloc[-1]["mau"]) if not mau.empty else 0
    avg_mau = float(mau["mau"].mean()) if not mau.empty else 0.0
    dau_mau_ratio = round(100 * avg_dau / avg_mau, 1) if avg_mau else 0.0
    engage_rows = [
        ["Metric", "Value"],
        ["Average DAU", f"{avg_dau:,.0f}"],
        ["Peak DAU", f"{peak_dau:,}"],
        ["Latest MAU", f"{latest_mau:,}"],
        ["DAU/MAU Ratio", f"{dau_mau_ratio:.1f}%"],
    ]
    story.append(_to_table(engage_rows, [8.5 * cm, 6.5 * cm]))
    story.append(Spacer(1, 8))

    # Recommendations
    story.append(Paragraph("6. Strategic MBA Recommendations", section_style))
    for rec in recommendations:
        clean_text = (
            rec.replace("**", "")
            .replace("—", "-")
            .replace("->", "to")
        )
        story.append(Paragraph(clean_text, bullet_style, bulletText="•"))

    story.append(Spacer(1, 10))
    story.append(
        Paragraph(
            "Prepared by RS Analytics automated strategy layer.",
            ParagraphStyle(
                "FooterStyle",
                parent=styles["Italic"],
                alignment=1,
                textColor=colors.HexColor("#555555"),
                fontSize=8,
            ),
        )
    )

    doc.build(story)


# ============================================================
# Main
# ============================================================

def main() -> None:
    """
    Run all analysis sections, write output files.
    """
    parser = argparse.ArgumentParser(description="Generate MBA App Performance PDF report")
    parser.add_argument("--month", type=str, default="", help="Month in YYYY-MM format (e.g. 2026-01)")
    parser.add_argument("--start-date", type=str, default="", help="Start date YYYY-MM-DD")
    parser.add_argument("--end-date", type=str, default="", help="End date YYYY-MM-DD")
    args = parser.parse_args()

    start_date, end_date = _parse_period(
        month=args.month,
        start_date=args.start_date,
        end_date=args.end_date,
    )

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"Starting App Performance Report at {generated_at}")
    logger.info(f"Database: {DB_PATH}")
    logger.info(f"Output:   {OUTPUT_DIR}")
    logger.info(f"Period:   {start_date} to {end_date}")

    # Ensure output directory exists
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Connect to DuckDB (read-only — this is an analysis script)
    conn = duckdb.connect(str(DB_PATH), read_only=True)

    try:
        # ── Build all sections ──────────────────────────────────
        kpis          = build_executive_summary(conn, start_date=start_date, end_date=end_date)
        funnel        = build_funnel(conn, start_date=start_date, end_date=end_date)
        country_df    = build_country_analysis(conn, start_date=start_date, end_date=end_date)
        rev_data      = build_revenue_analysis(conn, start_date=start_date, end_date=end_date)
        engagement    = build_engagement_analysis(conn, start_date=start_date, end_date=end_date)
        recommendations = generate_recommendations(kpis, funnel, country_df)

        # ── Write PDF report only ───────────────────────────────
        report_path = OUTPUT_DIR / "app_performance_report.pdf"
        build_pdf_report(
            output_path=report_path,
            kpis=kpis,
            funnel=funnel,
            country_df=country_df,
            rev_data=rev_data,
            engagement=engagement,
            recommendations=recommendations,
            generated_at=generated_at,
        )
        logger.info(f"Wrote {report_path}")

        # Remove older non-PDF artifacts to keep output folder clean.
        for stale_name in [
            "app_performance_report.md",
            "summary_kpis.csv",
            "country_analysis.csv",
            "funnel_weekly.csv",
            "revenue_daily.csv",
        ]:
            stale_path = OUTPUT_DIR / stale_name
            if stale_path.exists():
                stale_path.unlink()
                logger.info(f"Removed legacy output {stale_path}")

        # ── Print summary to console ────────────────────────────
        print("\n" + "=" * 60)
        print("  APP PERFORMANCE REPORT — SUMMARY")
        print("=" * 60)
        print(f"  Users:     {kpis['total_users']:>8,}  ({kpis['country_count']} countries)")
        print(f"  Verified:  {kpis['verified_users']:>8,}  ({kpis['verification_rate']}%)")
        print(f"  Paying:    {kpis['paying_users']:>8,}  ({kpis['conversion_rate']}%)")
        print(f"  Revenue:   ${kpis['total_revenue_sgd']:>10,.2f}")
        print(f"  ARPU:      ${kpis['arpu']:>10,.2f}")
        print(f"  ARPPU:     ${kpis['arppu']:>10,.2f}")
        print(f"  Velocity:  {kpis['points_velocity']:>8}%")
        print("-" * 60)
        print(f"  Funnel:    {funnel['signups']:,} -> {funnel['verified']:,} -> "
              f"{funnel['created_vps']:,} -> {funnel['paid']:,}")
        print(f"  Overall:   {funnel['overall_conversion']}% signup-to-paid")
        print("-" * 60)
        print(f"  Output:    {OUTPUT_DIR}")
        print("  File:      app_performance_report.pdf")
        print("=" * 60 + "\n")

    finally:
        conn.close()

    logger.info("Report complete.")


if __name__ == "__main__":
    main()
