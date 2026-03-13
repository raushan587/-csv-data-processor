"""
db.py — SQLite database setup, import, and validation queries
"""

import sqlite3
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

DB_PATH = Path("output/customers.db")


def get_connection() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def setup_schema(conn: sqlite3.Connection):
    """Create the customers table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            id          INTEGER PRIMARY KEY,
            name        TEXT    NOT NULL,
            email       TEXT    NOT NULL UNIQUE,
            plan        TEXT    NOT NULL,
            amount      REAL    NOT NULL,
            status      TEXT    NOT NULL,
            created_at  TEXT    NOT NULL,
            imported_at TEXT    NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.commit()
    logger.info("Schema ready: customers table")


def insert_records(conn: sqlite3.Connection, rows: list[dict]) -> tuple[int, int]:
    """
    Insert valid rows into DB.
    Returns (inserted_count, skipped_count).
    """
    inserted = 0
    skipped = 0

    for row in rows:
        try:
            conn.execute("""
                INSERT INTO customers (id, name, email, plan, amount, status, created_at)
                VALUES (:id, :name, :email, :plan, :amount, :status, :created_at)
            """, row)
            inserted += 1
        except sqlite3.IntegrityError as e:
            logger.warning(f"Skipped row id={row.get('id')} — {e}")
            skipped += 1

    conn.commit()
    logger.info(f"Inserted {inserted} records, skipped {skipped}")
    return inserted, skipped


def run_validation_checks(conn: sqlite3.Connection) -> dict:
    """
    Run SQL-based post-import validation checks.
    Returns a dict of check_name -> result.
    """
    checks = {}

    # 1. Total record count
    checks["total_records"] = conn.execute(
        "SELECT COUNT(*) FROM customers"
    ).fetchone()[0]

    # 2. Duplicate emails (should be 0 after import)
    checks["duplicate_emails"] = conn.execute(
        "SELECT COUNT(*) FROM (SELECT email FROM customers GROUP BY email HAVING COUNT(*) > 1)"
    ).fetchone()[0]

    # 3. Records with NULL or empty name
    checks["missing_names"] = conn.execute(
        "SELECT COUNT(*) FROM customers WHERE name IS NULL OR name = ''"
    ).fetchone()[0]

    # 4. Invalid plan values
    checks["invalid_plans"] = conn.execute(
        "SELECT COUNT(*) FROM customers WHERE plan NOT IN ('basic', 'pro', 'enterprise', 'premium')"
    ).fetchone()[0]

    # 5. Negative or zero amounts
    checks["bad_amounts"] = conn.execute(
        "SELECT COUNT(*) FROM customers WHERE amount <= 0"
    ).fetchone()[0]

    # 6. Revenue by plan
    plan_revenue = conn.execute(
        "SELECT plan, COUNT(*) as count, SUM(amount) as revenue FROM customers GROUP BY plan ORDER BY revenue DESC"
    ).fetchall()
    checks["revenue_by_plan"] = [dict(r) for r in plan_revenue]

    # 7. Active vs inactive
    status_split = conn.execute(
        "SELECT status, COUNT(*) as count FROM customers GROUP BY status"
    ).fetchall()
    checks["status_breakdown"] = [dict(r) for r in status_split]

    return checks
