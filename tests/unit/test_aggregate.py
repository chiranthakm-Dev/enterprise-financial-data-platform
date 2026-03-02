import os
import sys
import csv

# ensure src package import
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from src.aggregate.aggregate import aggregate


def create_csv(path, headers, rows):
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=headers)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


def test_aggregate_simple(tmp_path):
    val = tmp_path / "validated.csv"
    headers = ["transaction_id", "transaction_date", "account_id", "amount", "transaction_type", "source_system"]
    rows = [
        {"transaction_id": "1", "transaction_date": "2026-02-01", "account_id": "A", "amount": "100", "transaction_type": "debit", "source_system": "bank"},
        {"transaction_id": "2", "transaction_date": "2026-02-01", "account_id": "A", "amount": "50", "transaction_type": "credit", "source_system": "bank"},
    ]
    create_csv(val, headers, rows)
    res = aggregate([str(val)], dry_run=True, out_dir=str(tmp_path))
    assert res["daily_count"] >= 1
    assert (tmp_path / "daily_summary.csv").exists()
