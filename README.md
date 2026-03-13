# CSV Data Processor & API Importer

A Python pipeline that ingests messy CSV datasets, cleans and validates records, imports them into SQLite, and POSTs them to a REST API — simulating real-world customer data migration workflows.

## Features

- CSV Ingestion — Load structured/unstructured CSV files
- Data Cleaning — Normalize casing, strip whitespace, detect duplicates
- Validation — Flag missing fields, invalid emails, bad dates, unknown enum values
- SQLite Import — Store validated records with schema enforcement
- REST API Integration — POST records as JSON payloads (dry run or live)
- SQL Validation Checks — Cross-verify counts, duplicates, referential integrity
- Rejected Row Export — Save failed records to `output/rejected_rows.csv`

## Setup
```bash
git clone https://github.com/raushan587/-csv-data-processor.git
cd csv_data_processor
pip install -r requirements.txt
```

## Usage
```bash
python main.py                      # dry run with sample data
python main.py --file data/my.csv   # custom CSV file
python main.py --no-dry-run         # live API calls
```

## Tech Stack

- Python 3.11+
- SQLite (sqlite3 stdlib)
- REST APIs (requests)
- CSV/JSON processing
- Git & GitHub
