import os
import sys
import csv
import pytest

# make sure the src package is importable
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from src.validation.validate import validate_file, ValidationError
from src.config.config import get_config


def create_csv(path, headers, rows):
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=headers)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


def test_validate_missing_file(tmp_path):
    with pytest.raises(ValidationError):
        validate_file(str(tmp_path / "no.csv"), dry_run=True)


def test_validate_missing_columns(tmp_path):
    path = tmp_path / "data.csv"
    create_csv(path, ["transaction_id"], [{"transaction_id": "1"}])
    with pytest.raises(ValidationError):
        validate_file(str(path), dry_run=True)


def test_validate_rules(tmp_path):
    path = tmp_path / "data.csv"
    headers = ["transaction_id", "transaction_date", "account_id", "amount", "transaction_type", "source_system"]
    rows = [
        {"transaction_id": "1", "transaction_date": "2026-02-01", "account_id": "A", "amount": "10", "transaction_type": "debit", "source_system": "bank"},
        {"transaction_id": "1", "transaction_date": "2026-02-01", "account_id": "A", "amount": "10", "transaction_type": "debit", "source_system": "bank"},  # duplicate
        {"transaction_id": "2", "transaction_date": "invalid", "account_id": "B", "amount": "x", "transaction_type": "foo", "source_system": "bank"},
    ]
    create_csv(path, headers, rows)
    outdir = tmp_path / "out"
    res = validate_file(str(path), dry_run=True, out_dir=str(outdir))
    assert res["total"] == 3
    assert res["valid"] == 1
    assert res["rejected"] == 2
    # check output files exist
    assert (outdir / "data.validated.csv").exists()
    assert (outdir / "data.rejected.csv").exists()


def test_validate_processed_suffix(tmp_path):
    # when validating a file produced by ingest, the filename will usually
    # include ".processed" before the extension.  The validator should strip
    # that suffix when naming its outputs so that downstream stages can refer
    # to "bank.validated.csv" rather than "bank.processed.validated.csv".
    orig = tmp_path / "bank.processed.csv"
    headers = ["transaction_id", "transaction_date", "account_id", "amount", "transaction_type", "source_system"]
    create_csv(orig, headers, [{"transaction_id": "1", "transaction_date": "2026-02-01", "account_id": "A", "amount": "10", "transaction_type": "debit", "source_system": "bank"}])
    outdir = tmp_path / "out"
    res = validate_file(str(orig), dry_run=True, out_dir=str(outdir))
    # inputs processed file but validated output should drop the ".processed" part
    assert (outdir / "bank.validated.csv").exists()
    assert (outdir / "bank.rejected.csv").exists()
    assert res["total"] == 1


def test_validate_fallback_no_creds(tmp_path, monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "")
    monkeypatch.setenv("SNOWFLAKE_USER", "")
    path = tmp_path / "data.csv"
    headers = ["transaction_id", "transaction_date", "account_id", "amount", "transaction_type", "source_system"]
    create_csv(path, headers, [{"transaction_id": "1", "transaction_date": "2026-02-01", "account_id": "A", "amount": "10", "transaction_type": "debit", "source_system": "bank"}])
    outdir = tmp_path / "out"
    res = validate_file(str(path), dry_run=False, out_dir=str(outdir))
    assert res["total"] == 1
    assert res["valid"] == 1
    assert (outdir / "data.validated.csv").exists()


def test_config_dynamic(monkeypatch):
    # verify that get_config respects changes to environment variables
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "acct1")
    monkeypatch.setenv("SNOWFLAKE_USER", "user1")
    cfg1 = get_config()
    assert cfg1.snowflake_account == "acct1"
    assert cfg1.snowflake_user == "user1"

    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "acct2")
    cfg2 = get_config()
    assert cfg2.snowflake_account == "acct2"
    # other fields should update similarly
    assert cfg2.snowflake_user == "user1"
