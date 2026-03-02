import os
import sys
import csv
import pytest

# ensure src package import
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from src.reconcile.reconcile import reconcile


def create_csv(path, headers, rows):
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=headers)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


def test_reconcile_simple(tmp_path):
    bank = tmp_path / "bank_validated.csv"
    ledger = tmp_path / "ledger_validated.csv"
    headers = ["transaction_id", "transaction_date", "account_id", "amount", "transaction_type", "source_system"]
    create_csv(bank, headers, [{"transaction_id": "1", "transaction_date": "2026-02-01", "account_id": "A", "amount": "100", "transaction_type": "debit", "source_system": "bank"}])
    create_csv(ledger, headers, [{"transaction_id": "1", "transaction_date": "2026-02-01", "account_id": "A", "amount": "100", "transaction_type": "debit", "source_system": "ledger"}])
    res = reconcile(str(bank), str(ledger), dry_run=True, tolerance=5.0, out_dir=str(tmp_path))
    assert res["matched_count"] == 1
    assert res["unmatched_count"] == 0
    # check output files exist
    assert (tmp_path / "matched_transactions.csv").exists()
