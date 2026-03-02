import os
import sys
import csv
import pytest

# ensure workspace root is on path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from src.ingest.ingest import ingest_file, IngestionError


def create_csv(path, headers, rows):
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=headers)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


def test_ingest_missing_file(tmp_path):
    with pytest.raises(IngestionError):
        ingest_file(str(tmp_path / "does_not_exist.csv"), source_system="bank", dry_run=True)


def test_ingest_missing_columns(tmp_path):
    file = tmp_path / "data.csv"
    # only some required columns
    create_csv(file, ["transaction_id", "amount"], [{"transaction_id": "x", "amount": "100"}])
    with pytest.raises(IngestionError):
        ingest_file(str(file), source_system="bank", dry_run=True)


def test_ingest_dry_run_writes(tmp_path):
    file = tmp_path / "data.csv"
    headers = ["transaction_id", "transaction_date", "account_id", "amount", "transaction_type"]
    rows = [
        {"transaction_id": "1", "transaction_date": "2026-02-01", "account_id": "A", "amount": "10", "transaction_type": "debit"},
        {"transaction_id": "2", "transaction_date": "2026-02-02", "account_id": "B", "amount": "20", "transaction_type": "credit"},
    ]
    create_csv(file, headers, rows)
    outdir = tmp_path / "out"
    metrics = ingest_file(str(file), source_system="bank", dry_run=True, out_dir=str(outdir))
    assert metrics["total_rows"] == 2
    assert metrics["inserted_count"] == 0
    # verify processed file created
    processed = outdir / "data.processed.csv"
    assert processed.exists()
    # check contents
    with open(processed, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        assert len(list(reader)) == 2



def test_ingest_fallback_no_creds(tmp_path, monkeypatch):
    # simulate missing credentials
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "")
    monkeypatch.setenv("SNOWFLAKE_USER", "")
    file = tmp_path / "data.csv"
    headers = ["transaction_id", "transaction_date", "account_id", "amount", "transaction_type"]
    rows = [{"transaction_id": "1", "transaction_date": "2026-02-01", "account_id": "A", "amount": "10", "transaction_type": "debit"}]
    create_csv(file, headers, rows)
    outdir = tmp_path / "out2"
    # call without dry_run; should auto-switch due to missing creds
    metrics = ingest_file(str(file), source_system="bank", dry_run=False, out_dir=str(outdir))
    assert metrics["total_rows"] == 1
    assert metrics["inserted_count"] == 0
    assert (outdir / "data.processed.csv").exists()
