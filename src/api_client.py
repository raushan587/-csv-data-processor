"""
api_client.py — Simulates POSTing records to a REST API endpoint.
In production this would target a real SaaS onboarding API (e.g. Zenskar).
For demo purposes, it uses a local mock endpoint or logs dry-run payloads.
"""

import json
import logging
import time

logger = logging.getLogger(__name__)


def build_payload(row: dict) -> dict:
    """Transform a cleaned DB row into an API-ready JSON payload."""
    return {
        "externalId": str(row["id"]),
        "customerName": row["name"],
        "email": row["email"],
        "subscriptionPlan": row["plan"],
        "billingAmount": float(row["amount"]),
        "status": row["status"],
        "startDate": row["created_at"],
    }


def post_records(rows: list[dict], dry_run: bool = True) -> dict:
    """
    POST each record to the API.
    If dry_run=True, logs payloads without making real HTTP calls.

    Returns summary dict with success/failure counts.
    """
    success = 0
    failed = 0
    failed_ids = []

    for row in rows:
        payload = build_payload(row)

        if dry_run:
            logger.info(f"[DRY RUN] POST /api/v1/customers → {json.dumps(payload)}")
            success += 1
            time.sleep(0.01)  # simulate slight latency
        else:
            # Real HTTP call (requires requests + live endpoint)
            try:
                import requests
                response = requests.post(
                    "https://api.yourtarget.com/v1/customers",
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=10,
                )
                if response.status_code in (200, 201):
                    success += 1
                    logger.info(f"SUCCESS id={row['id']} → HTTP {response.status_code}")
                else:
                    failed += 1
                    failed_ids.append(row["id"])
                    logger.error(f"FAILED id={row['id']} → HTTP {response.status_code}: {response.text}")
            except Exception as e:
                failed += 1
                failed_ids.append(row["id"])
                logger.error(f"ERROR id={row['id']} → {e}")

    return {
        "total": len(rows),
        "success": success,
        "failed": failed,
        "failed_ids": failed_ids,
    }
