# CSV Data Processor & API Importer

A Python automation pipeline that ingests messy CSV datasets, cleans and validates records, imports them into a local SQLite database, and POSTs them to a REST API — designed to simulate real-world customer data migration workflows.

---

## Features

- **CSV Ingestion** — Load structured/unstructured CSV files from any source
- **Data Cleaning** — Normalize casing, strip whitespace, detect duplicates
- **Validation** — Flag missing fields, invalid emails, bad dates, unknown enum values
- **SQLite Import** — Store validated records with schema enforcement
- **REST API Integration** — POST records as JSON payloads (dry run or live)
- **SQL Validation Checks** — Cross-verify counts, duplicates, referential integrity post-import
- **Summary Report** — Human-readable migration report with per-row error details
- **Rejected Row Export** — Save rejected records to `output/rejected_rows.csv` for review

---

## Project Structure

```
csv_data_processor/
├── main.py                  # Pipeline entry point
├── requirements.txt
├── data/
│   └── sample/
│       └── customers.csv    # Sample messy dataset
├── src/
│   ├── cleaner.py           # CSV loading, cleaning, validation
│   ├── db.py                # SQLite schema, insert, SQL validation queries
│   ├── api_client.py        # REST API payload builder & HTTP client
│   └── reporter.py          # Summary report + rejected rows CSV export
├── logs/
│   └── pipeline.log         # Auto-generated run logs
└── output/
    ├── customers.db         # SQLite database (auto-created)
    └── rejected_rows.csv    # Rows that failed validation (auto-created)
```

---

## Setup

```bash
# Clone the repo
git clone https://github.com/raushan587/csv-data-processor.git
cd csv_data_processor

# Install dependencies
pip install -r requirements.txt
```

---

## Usage

```bash
# Run with sample data (dry run — no real API calls)
python main.py

# Run with your own CSV file
python main.py --file data/your_file.csv

# Run in live mode (real API calls — configure endpoint in src/api_client.py)
python main.py --no-dry-run
```

---

## Sample Output

```
============================================================
  CSV DATA PROCESSOR — MIGRATION REPORT
  Generated: 2025-07-01 14:32:01
============================================================

📥  INPUT
    Total rows in CSV     : 15
    Valid rows            : 9
    Rejected rows         : 6

⚠️   REJECTED ROWS (see output/rejected_rows.csv)
    Row   2 | anita@...    | Missing email
    Row   5 |              | Missing name
    ...

💾  DATABASE IMPORT
    Inserted              : 9
    Skipped (duplicates)  : 0

🌐  API IMPORT (dry run)
    Total sent            : 9
    Successful            : 9
    Failed                : 0

✅  POST-IMPORT VALIDATION CHECKS
    Total records in DB   : 9
    Duplicate emails      : 0  ✓ OK
    Missing names         : 0  ✓ OK
    Invalid plans         : 0  ✓ OK
    Bad amounts (<=0)     : 0  ✓ OK

📊  REVENUE BY PLAN
    enterprise   3 customers   ₹14,997
    pro          3 customers   ₹2,997
    ...
============================================================
```

---

## Data Validation Rules

| Field | Rule |
|-------|------|
| `name` | Required, non-empty |
| `email` | Required, valid format, unique |
| `plan` | Must be one of: `basic`, `pro`, `enterprise`, `premium` |
| `status` | Must be one of: `active`, `inactive` |
| `amount` | Required, must be a valid positive number |
| `created_at` | Required, must be `YYYY-MM-DD` format |

---

## Tech Stack

- **Python 3.11+**
- **SQLite** (via `sqlite3` stdlib)
- **REST APIs** (via `requests`)
- **CSV/JSON** processing (stdlib)
- **Git & GitHub**

---

## Use Case

This pipeline mirrors real-world **customer data migration** scenarios — e.g., migrating billing records from spreadsheets or legacy systems into a SaaS platform like Stripe, Chargebee, or Zenskar.
