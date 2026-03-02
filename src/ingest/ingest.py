"""Ingestion module: load CSV files into `raw.raw_transactions`.

Features:
- Validate file exists and required columns
- Add `ingestion_timestamp`
- Avoid duplicate inserts (transaction_id + source_system)
- Support `dry_run` which writes a processed CSV instead of writing to Snowflake
"""
from pathlib import Path
from datetime import datetime
import logging
from typing import Dict, List
import csv

try:
    import pandas as pd  # type: ignore
    PANDAS_AVAILABLE = True
except Exception:
    pd = None
    PANDAS_AVAILABLE = False

from src.db.connection import execute_many, execute_query, create_run_id
from src.config.config import has_snowflake_credentials

LOG = logging.getLogger(__name__)

REQUIRED_COLUMNS = [
    "transaction_id",
    "transaction_date",
    "account_id",
    "amount",
    "transaction_type",
]

# `source_system` may be provided separately by the caller; if the CSV
# includes it we will use it, otherwise ingestion will fill with the
# supplied `source_system` argument.


class IngestionError(Exception):
    pass


def validate_columns(columns: List[str], required: List[str]):
    missing = [c for c in required if c not in columns]
    if missing:
        raise IngestionError(f"Missing required columns: {missing}")


def _build_insert_statement() -> str:
    return (
        "INSERT INTO raw.raw_transactions (transaction_id, transaction_date, account_id, amount, transaction_type, source_system, ingestion_timestamp) "
        "SELECT %(transaction_id)s, TO_DATE(%(transaction_date)s), %(account_id)s, %(amount)s, %(transaction_type)s, %(source_system)s, CURRENT_TIMESTAMP() "
        "WHERE NOT EXISTS (SELECT 1 FROM raw.raw_transactions r WHERE r.transaction_id = %(transaction_id)s AND r.source_system = %(source_system)s)"
    )


def ingest_file(
    path: str,
    source_system: str,
    dry_run: bool = False,
    batch_size: int = 1000,
    out_dir: str = "data/processed",
) -> Dict:
    """Ingest a CSV file into `raw.raw_transactions`.

    Returns a dict with metrics: file_name, total_rows, inserted_count, run_id
    """
    p = Path(path)
    if not p.exists():
        raise IngestionError(f"File not found: {path}")

    rows = []
    columns = []
    if PANDAS_AVAILABLE:
        df = pd.read_csv(p)
        columns = list(df.columns)
        validate_columns(columns, REQUIRED_COLUMNS)
        df = df.copy()
        if "source_system" not in df.columns:
            df["source_system"] = source_system
        else:
            df["source_system"] = df["source_system"].fillna(source_system)
        rows = df.to_dict(orient="records")
        total_rows = len(df)
    else:
        # lightweight CSV reader fallback
        with open(p, "r", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            columns = reader.fieldnames or []
            validate_columns(columns, REQUIRED_COLUMNS)
            for r in reader:
                if not r.get("source_system"):
                    r["source_system"] = source_system
                rows.append(r)
        total_rows = len(rows)
    run_id = create_run_id()
    start_time = datetime.utcnow().isoformat()

    # if writing mode but credentials missing, fall back to dry-run silently
    if not dry_run and not has_snowflake_credentials():
        LOG.warning("Snowflake credentials not found; switching to dry-run mode")
        dry_run = True

    # regardless of whether we are writing to Snowflake, we still produce a
    # processed CSV in ``out_dir`` when requested.  This makes downstream
    # stages (validation, reconciliation) simpler to write and also allows
    # offline inspection of what was ingested.  The file is written before any
    # database activity so the rest of the function may still raise if the
    # data fails to insert.
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    out_file = out_path / f"{p.stem}.processed.csv"
    with open(out_file, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=REQUIRED_COLUMNS + ["source_system"])
        writer.writeheader()
        for r in rows:
            out_row = {k: r.get(k, None) for k in REQUIRED_COLUMNS + ["source_system"]}
            writer.writerow(out_row)
    LOG.info("Wrote processed file to %s", out_file)

    if dry_run:
        LOG.info("Dry-run: skipping database inserts")
        return {"file_name": p.name, "total_rows": total_rows, "inserted_count": 0, "run_id": run_id}

    # Prepare param dicts for batch execution
    records = []
    for row in rows:
        val = row.get("amount")
        try:
            amount = float(val) if val not in (None, "", "nan") else None
        except Exception:
            amount = None

        rec = {
            "transaction_id": str(row.get("transaction_id")),
            "transaction_date": str(row.get("transaction_date")),
            "account_id": str(row.get("account_id")),
            "amount": amount,
            "transaction_type": str(row.get("transaction_type", "")),
            "source_system": str(row.get("source_system", source_system)),
        }
        records.append(rec)

    insert_sql = _build_insert_statement()

    # Execute in batches
    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        execute_many(insert_sql, batch)

    # Count inserted rows for this run (based on ingestion timestamp >= start_time)
    # Note: ingestion_timestamp uses CURRENT_TIMESTAMP() on insert, so this approximates inserted rows
    count_q = "SELECT COUNT(*) FROM raw.raw_transactions WHERE source_system = %(source_system)s AND ingestion_timestamp >= TO_TIMESTAMP_LTZ(%(start_time)s)"
    rows = execute_query(count_q, {"source_system": source_system, "start_time": start_time})
    inserted_count = int(rows[0][0]) if rows else 0

    LOG.info("Ingested file %s: total=%d inserted=%d", p.name, total_rows, inserted_count)

    return {"file_name": p.name, "total_rows": total_rows, "inserted_count": inserted_count, "run_id": run_id}


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Ingest CSV into raw.raw_transactions")
    parser.add_argument("path")
    parser.add_argument("--source", required=False, default="bank")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    res = ingest_file(args.path, args.source, dry_run=args.dry_run)
    print(res)
