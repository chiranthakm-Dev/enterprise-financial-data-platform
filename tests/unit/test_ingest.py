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


def test_ingest_non_dry_run_also_writes(tmp_path, monkeypatch):
    # with credentials present the function should still write the processed
    # CSV even when dry_run=False. We don't actually connect to a database in
    # unit tests, so we simply verify the file creation and that no exception
    # is raised.
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "acct")
    monkeypatch.setenv("SNOWFLAKE_USER", "user")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "pass")
    file = tmp_path / "data2.csv"
    headers = ["transaction_id", "transaction_date", "account_id", "amount", "transaction_type"]
    create_csv(file, headers, [{"transaction_id": "1", "transaction_date": "2026-02-01", "account_id": "A", "amount": "10", "transaction_type": "debit"}])
    outdir = tmp_path / "out3"
    # avoid any actual Snowflake calls during this unit test; the ingest module
    # imported these helpers at import time so we patch the names there.
    import src.ingest.ingest as ingest_mod
    monkeypatch.setattr(ingest_mod, "execute_many", lambda q, params: None)
    monkeypatch.setattr(ingest_mod, "execute_query", lambda q, params=None: [(0,)])
    metrics = ingest_file(str(file), source_system="bank", dry_run=False, out_dir=str(outdir))
    assert metrics["total_rows"] == 1
    # file should be created regardless of dry_run
    assert (outdir / "data2.processed.csv").exists()
