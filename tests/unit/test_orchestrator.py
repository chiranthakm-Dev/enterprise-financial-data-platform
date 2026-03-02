import os
import sys
import csv
from pathlib import Path

# ensure src package import
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from src.main import run_pipeline


def create_csv(path, headers, rows):
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=headers)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


def test_pipeline_dry_run(tmp_path):
    # set up sample bank/ledger files
    bank = tmp_path / "bank.csv"
    ledger = tmp_path / "ledger.csv"
    headers = ["transaction_id", "transaction_date", "account_id", "amount", "transaction_type", "source_system"]
    create_csv(bank, headers, [{"transaction_id": "1", "transaction_date": "2026-02-01", "account_id": "A", "amount": "100", "transaction_type": "debit", "source_system": "bank"}])
    create_csv(ledger, headers, [{"transaction_id": "1", "transaction_date": "2026-02-01", "account_id": "A", "amount": "100", "transaction_type": "debit", "source_system": "ledger"}])

    # run full pipeline in dry-run with base_dir pointing at tmp_path
    run_pipeline(["all"], dry_run=True, bank_file=str(bank), ledger_file=str(ledger), base_dir=str(tmp_path))

    # verify outputs exist
    processed = tmp_path / "processed"
    assert processed.exists()
    assert (processed / "bank.validated.csv").exists()
    assert (processed / "ledger.validated.csv").exists()
    assert (processed / "matched_transactions.csv").exists()
    assert (processed / "daily_summary.csv").exists()
    # log file should exist in data/logs (workspace-relative)
    assert Path("data/logs/etl_run_logs.csv").exists()
