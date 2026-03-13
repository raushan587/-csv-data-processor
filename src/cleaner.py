"""
cleaner.py — CSV ingestion, cleaning, and transformation pipeline
"""

import csv
import re
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

VALID_PLANS = {"basic", "pro", "enterprise", "premium"}
VALID_STATUSES = {"active", "inactive"}


def load_csv(filepath: str) -> list[dict]:
    """Load raw CSV file and return list of row dicts."""
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {filepath}")

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [row for row in reader]

    logger.info(f"Loaded {len(rows)} rows from {filepath}")
    return rows


def clean_rows(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    Clean and validate each row.
    Returns (valid_rows, rejected_rows).
    """
    valid = []
    rejected = []

    seen = set()  # for duplicate detection

    for i, row in enumerate(rows, start=2):  # start=2 accounts for header
        errors = []

        # --- Normalize fields ---
        row["name"] = row.get("name", "").strip().title()
        row["email"] = row.get("email", "").strip().lower()
        row["plan"] = row.get("plan", "").strip().lower()
        row["status"] = row.get("status", "").strip().lower()
        row["amount"] = row.get("amount", "").strip()
        row["created_at"] = row.get("created_at", "").strip()

        # --- Validate required fields ---
        if not row["name"]:
            errors.append("Missing name")

        if not row["email"]:
            errors.append("Missing email")
        elif not re.match(r"^[\w\.-]+@[\w\.-]+\.\w{2,}$", row["email"]):
            errors.append(f"Invalid email: {row['email']}")

        # --- Validate plan ---
        if row["plan"] not in VALID_PLANS:
            errors.append(f"Invalid plan: '{row['plan']}'")

        # --- Validate status ---
        if row["status"] not in VALID_STATUSES:
            errors.append(f"Invalid status: '{row['status']}'")

        # --- Validate amount ---
        if not row["amount"]:
            errors.append("Missing amount")
        else:
            try:
                row["amount"] = float(row["amount"])
            except ValueError:
                errors.append(f"Invalid amount: '{row['amount']}'")

        # --- Validate date ---
        if not row["created_at"]:
            errors.append("Missing created_at")
        else:
            try:
                datetime.strptime(row["created_at"], "%Y-%m-%d")
            except ValueError:
                errors.append(f"Invalid date format: '{row['created_at']}'")

        # --- Duplicate detection (by email) ---
        email_key = row["email"]
        if email_key and email_key in seen:
            errors.append(f"Duplicate email: {email_key}")
        elif email_key:
            seen.add(email_key)

        if errors:
            row["_errors"] = "; ".join(errors)
            row["_row_number"] = i
            rejected.append(row)
            logger.warning(f"Row {i} rejected — {row['_errors']}")
        else:
            valid.append(row)

    logger.info(f"Cleaning complete: {len(valid)} valid, {len(rejected)} rejected")
    return valid, rejected
