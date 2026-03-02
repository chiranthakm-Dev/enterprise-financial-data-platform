"""Validation module for raw transactions.

Reads input from a CSV or from Snowflake (if configured) and applies
business rules producing two outputs: valid records and rejected records.

When run in dry-run mode the results are stored as CSVs under
`data/processed`/; otherwise they are inserted into staging tables.
"""
from pathlib import Path
import logging
from typing import Dict, List, Tuple
from datetime import datetime

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
    "source_system",
]


class ValidationError(Exception):
    pass


def _read_input(path: str) -> Tuple[List[Dict], List[str]]:
    """Read CSV file and return list of rows (as dicts) and column names."""
    p = Path(path)
    if not p.exists():
        raise ValidationError(f"Input file not found: {path}")

    rows = []
    columns = []
    if PANDAS_AVAILABLE:
        df = pd.read_csv(p)
        columns = list(df.columns)
        rows = df.to_dict(orient="records")
    else:
        import csv

        with open(p, "r", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            columns = reader.fieldnames or []
            for r in reader:
                rows.append(r)
    return rows, columns


def _validate_row(row: Dict, seen_ids: set) -> Tuple[bool, str]:
    # rule 1: required non-null
    for field in ["transaction_id", "transaction_date", "amount"]:
        if not row.get(field):
            return False, f"{field} is null or empty"

    # rule 2: unique transaction_id per source
    tid = row.get("transaction_id")
    source = row.get("source_system")
    key = (tid, source)
    if key in seen_ids:
        return False, "duplicate transaction_id for source"
    seen_ids.add(key)

    # rule 3: amount numeric
    try:
        float(row.get("amount"))
    except Exception:
        return False, "amount not numeric"

    # rule 4: transaction_type debit/credit
    ttype = row.get("transaction_type", "").lower()
    if ttype not in ("debit", "credit"):
        return False, "invalid transaction_type"

    # rule 5: date parseable
    try:
        datetime.fromisoformat(row.get("transaction_date"))
    except Exception:
        return False, "invalid transaction_date"

    # optional balance check skipped
    return True, ""


def validate_file(path: str, dry_run: bool = False, out_dir: str = "data/processed") -> Dict:
    """Process the given CSV and return metrics."""
    rows, columns = _read_input(path)
    validate_columns = REQUIRED_COLUMNS
    missing = [c for c in validate_columns if c not in columns]
    if missing:
        raise ValidationError(f"Missing columns: {missing}")

    valid = []
    rejected = []
    seen = set()
    for r in rows:
        ok, reason = _validate_row(r, seen)
        if ok:
            valid.append(r)
        else:
            rejected.append({**r, "reason": reason})

    run_id = create_run_id()
    total = len(rows)
    valid_count = len(valid)
    rejected_count = len(rejected)

    # automatically treat as dry-run when no Snowflake credentials exist
    if not dry_run and not has_snowflake_credentials():
        LOG.warning("Snowflake credentials missing; validation will run in dry-run mode")
        dry_run = True

    if dry_run:
        out_path = Path(out_dir)
        out_path.mkdir(parents=True, exist_ok=True)
        import csv

        # determine base name for outputs. when validating the output of the
        # ingestion step we usually feed in a file ending with
        # `<name>.processed.csv`.  In that case the `stem` property will be
        # "<name>.processed" which would lead to output files like
        # "<name>.processed.validated.csv".  That's awkward for downstream
        # stages (and for users inspecting the directory) so we strip any
        # trailing ".processed" from the stem before constructing filenames.
        stem = Path(path).stem
        if stem.endswith(".processed"):
            stem = stem[: -len(".processed")]

        # write valid
        valid_file = out_path / f"{stem}.validated.csv"
        with open(valid_file, "w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=REQUIRED_COLUMNS + ["reason"])
            writer.writeheader()
            for r in valid:
                writer.writerow({**r, "reason": ""})
        # write rejected
        rej_file = out_path / f"{stem}.rejected.csv"
        with open(rej_file, "w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=REQUIRED_COLUMNS + ["reason"])
            writer.writeheader()
            for r in rejected:
                writer.writerow(r)
        LOG.info("Dry-run validation: %d valid, %d rejected", valid_count, rejected_count)
    else:
        # Insert into staging via Snowflake
        insert_valid = (
            "INSERT INTO staging.validated_transactions (transaction_id, transaction_date, account_id, amount, transaction_type, source_system, ingestion_timestamp) "
            "SELECT %(transaction_id)s, TO_DATE(%(transaction_date)s), %(account_id)s, %(amount)s, %(transaction_type)s, %(source_system)s, CURRENT_TIMESTAMP()"
        )
        insert_rej = (
            "INSERT INTO staging.rejected_records (transaction_id, account_id, reason, raw_payload) "
            "VALUES (%(transaction_id)s, %(account_id)s, %(reason)s, PARSE_JSON(%(raw_payload)s))"
        )
        for r in valid:
            execute_many(insert_valid, [r])
        for r in rejected:
            execute_many(insert_rej, [ {**r, "raw_payload": str(r)} ])

    return {
        "run_id": run_id,
        "total": total,
        "valid": valid_count,
        "rejected": rejected_count,
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Validate transactions CSV")
    parser.add_argument("path")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    res = validate_file(args.path, dry_run=args.dry_run)
    print(res)
