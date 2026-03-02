"""Reconciliation logic between bank and ledger validated transactions.

When Snowflake credentials are available, reconciliation will read from
`staging.validated_transactions` for each source and write results to
analytics tables. In dry-run mode (or when credentials are missing) CSV files
under `data/processed` are used/produced instead.

The algorithm performs:
 1. exact match on transaction_id, amount, transaction_date
 2. tolerance-based match (amount difference <= tolerance)

Metrics are returned to the caller and, if not dry-run, are persisted in
`analytics.reconciliation_summary`.
"""
from pathlib import Path
import logging
from typing import Dict, List, Optional, Tuple

try:
    import pandas as pd  # type: ignore
    PANDAS_AVAILABLE = True
except Exception:
    pd = None
    PANDAS_AVAILABLE = False

from src.config.config import has_snowflake_credentials, get_config
from src.db.connection import execute_query, execute_many, create_run_id

LOG = logging.getLogger(__name__)


def _read_validated(source: str, dry_run: bool, path: Optional[str] = None) -> List[Dict]:
    """Return list of records for given source system.

    If dry_run or no credentials, path must point to a CSV file.
    Otherwise the staging table is queried directly.
    """
    if dry_run or not has_snowflake_credentials():
        if not path:
            raise ValueError("CSV path required when running in dry-run mode")
        records = []
        if PANDAS_AVAILABLE:
            df = pd.read_csv(path)
            records = df.to_dict(orient="records")
        else:
            import csv

            with open(path, encoding="utf-8") as fh:
                reader = csv.DictReader(fh)
                for r in reader:
                    records.append(r)
        return [r for r in records if r.get("source_system") == source]
    else:
        # query Snowflake
        sql = (
            "SELECT transaction_id, transaction_date, account_id, amount, transaction_type, source_system "
            "FROM staging.validated_transactions WHERE source_system = %(src)s"
        )
        rows = execute_query(sql, {"src": source})
        cols = ["transaction_id", "transaction_date", "account_id", "amount", "transaction_type", "source_system"]
        return [dict(zip(cols, row)) for row in rows]


def reconcile(
    bank_path: Optional[str] = None,
    ledger_path: Optional[str] = None,
    dry_run: bool = True,
    tolerance: float = 5.0,
    out_dir: str = "data/processed",
) -> Dict:
    """Perform reconciliation and return summary metrics."""
    # fetch data
    bank = _read_validated("bank", dry_run, bank_path)
    ledger = _read_validated("ledger", dry_run, ledger_path)

    total = len(bank) + len(ledger)
    matched: List[Dict] = []
    unmatched_bank = bank.copy()
    unmatched_ledger = ledger.copy()

    # exact matching
    for b in bank:
        for l in list(unmatched_ledger):
            if (
                b.get("transaction_id") == l.get("transaction_id")
                and float(b.get("amount", 0)) == float(l.get("amount", 0))
                and b.get("transaction_date") == l.get("transaction_date")
            ):
                matched.append({**b, **{"matched_with": l.get("transaction_id"), "match_type": "exact"}})
                unmatched_bank.remove(b)
                unmatched_ledger.remove(l)
                break

    # tolerance-based matching
    for b in list(unmatched_bank):
        for l in list(unmatched_ledger):
            try:
                diff = abs(float(b.get("amount", 0)) - float(l.get("amount", 0)))
            except Exception:
                continue
            if b.get("transaction_id") == l.get("transaction_id") and diff <= tolerance:
                matched.append({**b, **{"matched_with": l.get("transaction_id"), "match_type": "tolerance"}})
                unmatched_bank.remove(b)
                unmatched_ledger.remove(l)
                break

    matched_count = len(matched)
    unmatched_count = len(unmatched_bank) + len(unmatched_ledger)
    rate = matched_count / total * 100 if total > 0 else 0.0
    run_id = create_run_id()

    # output results
    if dry_run or not has_snowflake_credentials():
        out_path = Path(out_dir)
        out_path.mkdir(parents=True, exist_ok=True)
        import csv

        def write_csv(fname, rows, fields=None):
            with open(out_path / fname, "w", encoding="utf-8", newline="") as fh:
                if not fields and rows:
                    fields = list(rows[0].keys())
                writer = csv.DictWriter(fh, fieldnames=fields or [])
                writer.writeheader()
                for r in rows:
                    writer.writerow(r)

        write_csv("matched_transactions.csv", matched)
        write_csv("unmatched_bank.csv", unmatched_bank)
        write_csv("unmatched_ledger.csv", unmatched_ledger)
        write_csv("reconciliation_summary.csv", [{
            "run_id": run_id,
            "total_records": total,
            "matched_count": matched_count,
            "unmatched_count": unmatched_count,
            "match_rate_percentage": rate,
        }])
    else:
        # persist to analytics tables
        insert_matched = (
            "INSERT INTO analytics.matched_transactions (transaction_id, bank_amount, ledger_amount, match_type) "
            "VALUES (%(transaction_id)s, %(amount)s, %(amount)s, %(match_type)s)"
        )
        for r in matched:
            execute_many(insert_matched, [r])
        # unmatched tables
        insert_unmatched = (
            "INSERT INTO analytics.unmatched_bank (transaction_id, account_id, amount, transaction_date, source_system) VALUES (%(transaction_id)s, %(account_id)s, %(amount)s, %(transaction_date)s, %(source_system)s)"
        )
        for r in unmatched_bank:
            execute_many(insert_unmatched, [r])
        insert_unmatched2 = (
            "INSERT INTO analytics.unmatched_ledger (transaction_id, account_id, amount, transaction_date, source_system) VALUES (%(transaction_id)s, %(account_id)s, %(amount)s, %(transaction_date)s, %(source_system)s)"
        )
        for r in unmatched_ledger:
            execute_many(insert_unmatched2, [r])
        # summary
        insert_summary = (
            "INSERT INTO analytics.reconciliation_summary (run_id, start_time, end_time, total_records, matched_count, unmatched_count, match_rate) "
            "VALUES (%(run_id)s, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), %(total)s, %(matched)s, %(unmatched)s, %(rate)s)"
        )
        execute_many(insert_summary, [{"run_id": run_id, "total": total, "matched": matched_count, "unmatched": unmatched_count, "rate": rate}])

    return {
        "run_id": run_id,
        "total_records": total,
        "matched_count": matched_count,
        "unmatched_count": unmatched_count,
        "match_rate_percentage": rate,
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Reconcile bank and ledger validated files")
    parser.add_argument("--bank", required=False, help="path to bank validated CSV")
    parser.add_argument("--ledger", required=False, help="path to ledger validated CSV")
    parser.add_argument("--tolerance", type=float, default=get_config().tolerance_amount)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    res = reconcile(args.bank, args.ledger, dry_run=args.dry_run, tolerance=args.tolerance)
    print(res)
