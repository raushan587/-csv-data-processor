"""
main.py — Entry point for the CSV Data Processor & API Importer pipeline.

Pipeline:
  1. Load raw CSV
  2. Clean & validate rows
  3. Import valid rows into SQLite
  4. POST records to REST API (dry run by default)
  5. Run SQL validation checks
  6. Generate summary report

Usage:
    python main.py                          # default sample data, dry run
    python main.py --file data/my.csv       # custom CSV
    python main.py --no-dry-run             # real API calls (set API URL in api_client.py)
"""

import argparse
import logging
import sys
from pathlib import Path

# Ensure src/ is importable
sys.path.insert(0, str(Path(__file__).parent))

from src.cleaner import load_csv, clean_rows
from src.db import get_connection, setup_schema, insert_records, run_validation_checks
from src.api_client import post_records
from src.reporter import save_rejected_csv, print_report

# ── Logging setup ──────────────────────────────────────────────────────────────
Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.FileHandler("logs/pipeline.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="CSV Data Processor & API Importer")
    parser.add_argument(
        "--file",
        default="data/sample/customers.csv",
        help="Path to input CSV file (default: data/sample/customers.csv)",
    )
    parser.add_argument(
        "--no-dry-run",
        action="store_true",
        help="Make real API calls instead of dry run",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    dry_run = not args.no_dry_run

    logger.info("=" * 50)
    logger.info("CSV Data Processor Pipeline — START")
    logger.info(f"Input file : {args.file}")
    logger.info(f"Mode       : {'DRY RUN' if dry_run else 'LIVE'}")
    logger.info("=" * 50)

    # Step 1 — Load
    raw_rows = load_csv(args.file)

    # Step 2 — Clean & Validate
    valid_rows, rejected_rows = clean_rows(raw_rows)

    # Step 3 — DB Import
    conn = get_connection()
    setup_schema(conn)
    db_inserted, db_skipped = insert_records(conn, valid_rows)

    # Step 4 — API Import
    api_result = post_records(valid_rows, dry_run=dry_run)

    # Step 5 — Validation Checks
    validation_checks = run_validation_checks(conn)
    conn.close()

    # Step 6 — Report
    save_rejected_csv(rejected_rows)
    print_report(
        total_input=len(raw_rows),
        valid_count=len(valid_rows),
        rejected_rows=rejected_rows,
        db_inserted=db_inserted,
        db_skipped=db_skipped,
        api_result=api_result,
        validation_checks=validation_checks,
    )

    logger.info("Pipeline complete.")


if __name__ == "__main__":
    main()
