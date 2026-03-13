"""
reporter.py — Generates a human-readable migration summary report.
"""

import csv
import json
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


def save_rejected_csv(rejected_rows: list[dict], output_dir: str = "output"):
    """Save rejected rows with error reasons to a CSV for review."""
    if not rejected_rows:
        logger.info("No rejected rows to save.")
        return

    Path(output_dir).mkdir(parents=True, exist_ok=True)
    filepath = Path(output_dir) / "rejected_rows.csv"

    fieldnames = list(rejected_rows[0].keys())
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rejected_rows)

    logger.info(f"Rejected rows saved → {filepath}")


def print_report(
    total_input: int,
    valid_count: int,
    rejected_rows: list[dict],
    db_inserted: int,
    db_skipped: int,
    api_result: dict,
    validation_checks: dict,
):
    """Print a formatted summary report to stdout."""

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    divider = "=" * 60

    print(f"\n{divider}")
    print(f"  CSV DATA PROCESSOR — MIGRATION REPORT")
    print(f"  Generated: {now}")
    print(divider)

    print(f"\n📥  INPUT")
    print(f"    Total rows in CSV     : {total_input}")
    print(f"    Valid rows            : {valid_count}")
    print(f"    Rejected rows         : {len(rejected_rows)}")

    if rejected_rows:
        print(f"\n⚠️   REJECTED ROWS (see output/rejected_rows.csv)")
        for row in rejected_rows:
            print(f"    Row {row['_row_number']:>3} | {row.get('email') or row.get('name') or 'N/A':<30} | {row['_errors']}")

    print(f"\n💾  DATABASE IMPORT")
    print(f"    Inserted              : {db_inserted}")
    print(f"    Skipped (duplicates)  : {db_skipped}")

    print(f"\n🌐  API IMPORT (dry run)")
    print(f"    Total sent            : {api_result['total']}")
    print(f"    Successful            : {api_result['success']}")
    print(f"    Failed                : {api_result['failed']}")

    print(f"\n✅  POST-IMPORT VALIDATION CHECKS")
    checks = validation_checks
    print(f"    Total records in DB   : {checks['total_records']}")
    print(f"    Duplicate emails      : {checks['duplicate_emails']}  {'✓ OK' if checks['duplicate_emails'] == 0 else '✗ FAIL'}")
    print(f"    Missing names         : {checks['missing_names']}  {'✓ OK' if checks['missing_names'] == 0 else '✗ FAIL'}")
    print(f"    Invalid plans         : {checks['invalid_plans']}  {'✓ OK' if checks['invalid_plans'] == 0 else '✗ FAIL'}")
    print(f"    Bad amounts (<=0)     : {checks['bad_amounts']}  {'✓ OK' if checks['bad_amounts'] == 0 else '✗ FAIL'}")

    print(f"\n📊  REVENUE BY PLAN")
    for entry in checks.get("revenue_by_plan", []):
        print(f"    {entry['plan']:<12} {entry['count']} customers   ₹{entry['revenue']:,.0f}")

    print(f"\n👥  STATUS BREAKDOWN")
    for entry in checks.get("status_breakdown", []):
        print(f"    {entry['status']:<12} {entry['count']} customers")

    print(f"\n{divider}\n")
