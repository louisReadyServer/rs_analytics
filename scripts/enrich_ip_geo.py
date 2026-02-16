"""
IP Geolocation Enrichment Script

Enriches core.dim_user rows that have a registration_ip but no
registration_country_code, by calling the free ip-api.com batch endpoint.

Usage:
    python scripts/enrich_ip_geo.py          # normal run
    python scripts/enrich_ip_geo.py --dry    # preview only, no writes

API: http://ip-api.com/batch  (free, max 100 IPs/request, 45 req/min)
"""

import sys
import time
import json
import logging
import argparse
from pathlib import Path

import duckdb
import requests
import pandas as pd

# ── Config ──────────────────────────────────────────────────────
BATCH_SIZE = 100          # ip-api.com limit per request
REQUEST_DELAY = 1.5       # seconds between batches (stay under 45 req/min)
DB_PATH = Path(__file__).resolve().parents[1] / "data" / "warehouse.duckdb"

# ── Logging ─────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("enrich_ip_geo")


def fetch_ip_batch(ips: list[str]) -> dict[str, dict]:
    """
    Call ip-api.com/batch for up to 100 IPs.

    Returns a dict mapping ip -> {countryCode, country, status}.
    Only includes IPs where the lookup succeeded.
    """
    # Build the payload — each item asks for ip, countryCode, country
    payload = [{"query": ip, "fields": "query,status,countryCode,country"} for ip in ips]

    try:
        resp = requests.post(
            "http://ip-api.com/batch",
            json=payload,
            timeout=30,
        )
        resp.raise_for_status()
        results = resp.json()
    except Exception as exc:
        logger.error(f"ip-api.com batch request failed: {exc}")
        return {}

    mapping: dict[str, dict] = {}
    for item in results:
        if item.get("status") == "success":
            mapping[item["query"]] = {
                "country_code": item.get("countryCode", ""),
                "country_name": item.get("country", ""),
            }
    return mapping


def main(dry_run: bool = False) -> None:
    logger.info(f"Connecting to DuckDB at {DB_PATH}")
    conn = duckdb.connect(str(DB_PATH), read_only=dry_run)

    # ── 1. Get distinct IPs that still need enrichment ──────────
    rows = conn.execute("""
        SELECT DISTINCT registration_ip
        FROM core.dim_user
        WHERE registration_ip IS NOT NULL
          AND registration_ip != ''
          AND (registration_country_code IS NULL OR registration_country_code = '')
    """).fetchall()

    unique_ips = [r[0] for r in rows]
    logger.info(f"Found {len(unique_ips)} unique IPs that need geo enrichment")

    if not unique_ips:
        logger.info("Nothing to enrich — all IPs already have country data.")
        conn.close()
        return

    # ── 2. Batch lookup ─────────────────────────────────────────
    all_results: dict[str, dict] = {}
    total_batches = (len(unique_ips) + BATCH_SIZE - 1) // BATCH_SIZE

    for batch_idx in range(total_batches):
        start = batch_idx * BATCH_SIZE
        end = start + BATCH_SIZE
        batch_ips = unique_ips[start:end]

        logger.info(f"Batch {batch_idx + 1}/{total_batches}: looking up {len(batch_ips)} IPs …")
        batch_results = fetch_ip_batch(batch_ips)
        all_results.update(batch_results)

        logger.info(f"  → resolved {len(batch_results)}/{len(batch_ips)} IPs")

        # Rate-limit between batches (skip delay on last batch)
        if batch_idx < total_batches - 1:
            time.sleep(REQUEST_DELAY)

    logger.info(f"Total resolved: {len(all_results)} / {len(unique_ips)} unique IPs")

    # ── 3. Show top countries preview ───────────────────────────
    country_counts: dict[str, int] = {}
    for ip in unique_ips:
        if ip in all_results:
            cc = all_results[ip]["country_name"]
            country_counts[cc] = country_counts.get(cc, 0) + 1

    for country, count in sorted(country_counts.items(), key=lambda x: -x[1])[:10]:
        logger.info(f"  {country}: {count} unique IPs")

    if dry_run:
        logger.info("[DRY-RUN] Skipping database update.")
        conn.close()
        return

    # ── 4. Update dim_user rows ─────────────────────────────────
    updated = 0
    for ip, geo in all_results.items():
        affected = conn.execute(
            """
            UPDATE core.dim_user
            SET registration_country_code = ?,
                registration_country_name = ?
            WHERE registration_ip = ?
              AND (registration_country_code IS NULL OR registration_country_code = '')
            """,
            [geo["country_code"], geo["country_name"], ip],
        ).fetchone()
        # DuckDB UPDATE doesn't return rowcount directly; count via affected rows
        updated += 1

    logger.info(f"Updated country data for IPs matching {updated} distinct IP addresses")

    # ── 5. Verify ───────────────────────────────────────────────
    verify = conn.execute("""
        SELECT registration_country_name, COUNT(*) AS users
        FROM core.dim_user
        WHERE registration_country_name IS NOT NULL
          AND registration_country_name != ''
        GROUP BY registration_country_name
        ORDER BY users DESC
        LIMIT 10
    """).fetchdf()
    logger.info(f"Verification — top countries in dim_user:\n{verify.to_string(index=False)}")

    remaining = conn.execute("""
        SELECT COUNT(*) FROM core.dim_user
        WHERE registration_country_code IS NULL OR registration_country_code = ''
    """).fetchone()[0]
    logger.info(f"Remaining un-enriched users: {remaining}")

    conn.close()
    logger.info("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Enrich dim_user IPs with country data")
    parser.add_argument("--dry", action="store_true", help="Preview only, no writes")
    args = parser.parse_args()
    main(dry_run=args.dry)
