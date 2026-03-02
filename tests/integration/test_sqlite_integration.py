"""Integration tests using SQLite as a stand-in for Snowflake.

These tests monkeypatch the Snowflake connection helpers to use a temporary
SQLite database.  SQL statements emitted by the application are translated
to a flat naming convention (schema.table -> schema_table) so that the same
code paths may execute without modification.

The goal is to exercise the full pipeline (ingest -> validate -> reconcile ->
aggregate -> logging) against a local store, providing confidence that the
SQL-based branches of the code work end-to-end in CI without requiring a real
Snowflake account.
"""

import os
import re
import sqlite3
from contextlib import contextmanager

import pytest

# ensure src package importable
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from src import main
from src.db import connection


def _translate_sql(sql: str) -> str:
    """Convert schema-qualified Snowflake names to sqlite-friendly names.

    E.g. ``raw.raw_transactions`` becomes ``raw_raw_transactions``.
    This is a very naive transformation suitable only for the limited set of
    object names used by the pipeline.
    """
    # convert schema.table (only known pipeline schemas) to schema_table
    out = re.sub(r"\b(raw|staging|analytics|logs)\.([a-zA-Z_]\w*)", r"\1_\2", sql)
    # convert Snowflake/psycopg2 parameter syntax %(name)s to sqlite named style
    out = re.sub(r"%\(([^)]+)\)s", r":\1", out)
    # strip TO_DATE wrapper which is not available in sqlite
    out = re.sub(r"TO_DATE\(([^)]+)\)", r"\1", out)
    # sqlite uses CURRENT_TIMESTAMP without parentheses
    out = out.replace("CURRENT_TIMESTAMP()", "CURRENT_TIMESTAMP")
    # drop TO_TIMESTAMP_LTZ calls which aren't available in sqlite
    out = re.sub(r"TO_TIMESTAMP_LTZ\(([^)]+)\)", r"\1", out)
    return out


def _setup_sqlite_db(db_path: str) -> sqlite3.Connection:
    """Create a sqlite database with the tables used by the pipeline."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    # create each table with simple types; dates/timestamps stored as TEXT
    cur.executescript(
        """
        CREATE TABLE raw_raw_transactions (
            transaction_id TEXT,
            transaction_date TEXT,
            account_id TEXT,
            amount REAL,
            transaction_type TEXT,
            source_system TEXT,
            ingestion_timestamp TEXT
        );
        CREATE TABLE staging_validated_transactions (
            transaction_id TEXT,
            transaction_date TEXT,
            account_id TEXT,
            amount REAL,
            transaction_type TEXT,
            source_system TEXT,
            ingestion_timestamp TEXT,
            validation_passed BOOLEAN
        );
        CREATE TABLE staging_rejected_records (
            transaction_id TEXT,
            account_id TEXT,
            reason TEXT,
            raw_payload TEXT,
            rejected_at TEXT
        );
        CREATE TABLE analytics_matched_transactions (
            transaction_id TEXT,
            bank_amount REAL,
            ledger_amount REAL,
            match_type TEXT,
            matched_at TEXT
        );
        CREATE TABLE analytics_unmatched_bank (
            transaction_id TEXT,
            account_id TEXT,
            amount REAL,
            transaction_date TEXT,
            source_system TEXT
        );
        CREATE TABLE analytics_unmatched_ledger (
            transaction_id TEXT,
            account_id TEXT,
            amount REAL,
            transaction_date TEXT,
            source_system TEXT
        );
        CREATE TABLE analytics_reconciliation_summary (
            run_id TEXT PRIMARY KEY,
            start_time TEXT,
            end_time TEXT,
            total_records INTEGER,
            matched_count INTEGER,
            unmatched_count INTEGER,
            match_rate REAL
        );
        CREATE TABLE logs_etl_run_logs (
            run_id TEXT PRIMARY KEY,
            start_time TEXT,
            end_time TEXT,
            total_ingested INTEGER,
            total_validated INTEGER,
            total_matched INTEGER,
            match_rate REAL,
            status TEXT
        );
        """
    )
    conn.commit()
    return conn


@pytest.fixture
def sqlite_monkeypatch(tmp_path, monkeypatch):
    # prepare sqlite database
    db_file = str(tmp_path / "test.db")
    conn = _setup_sqlite_db(db_file)

    # patch connection context manager to yield sqlite connection
    @contextmanager
    def fake_conn():
        yield conn

    monkeypatch.setattr(connection, "snowflake_connection", fake_conn)

    # patch execute_query and execute_many to translate SQL
    def exec_query(q, params=None):
        q2 = _translate_sql(q)
        cur = conn.cursor()
        if params:
            cur.execute(q2, params)
        else:
            cur.execute(q2)
        try:
            return cur.fetchall()
        except sqlite3.ProgrammingError:
            return []

    def exec_many(q, seq):
        q2 = _translate_sql(q)
        cur = conn.cursor()
        for params in seq:
            cur.execute(q2, params)
        conn.commit()

    monkeypatch.setattr(connection, "execute_query", exec_query)
    monkeypatch.setattr(connection, "execute_many", exec_many)
    # ignore view creation since sqlite syntax differs
    monkeypatch.setattr(connection, "run_sql_file", lambda path: None)

    # ensure credentials appear present so modules don't flip to dry-run
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "x")
    monkeypatch.setenv("SNOWFLAKE_USER", "x")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "x")
    yield conn
    conn.close()


def test_pipeline_with_sqlite(sqlite_monkeypatch, tmp_path):
    # prepare sample input files
    bank = tmp_path / "bank.csv"
    ledger = tmp_path / "ledger.csv"
    headers = [
        "transaction_id",
        "transaction_date",
        "account_id",
        "amount",
        "transaction_type",
        "source_system",
    ]
    import csv

    with open(bank, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=headers)
        writer.writeheader()
        writer.writerow({
            "transaction_id": "1",
            "transaction_date": "2026-02-01",
            "account_id": "A",
            "amount": "100",
            "transaction_type": "debit",
            "source_system": "bank",
        })
    with open(ledger, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=headers)
        writer.writeheader()
        writer.writerow({
            "transaction_id": "1",
            "transaction_date": "2026-02-01",
            "account_id": "A",
            "amount": "100",
            "transaction_type": "debit",
            "source_system": "ledger",
        })

    # run full pipeline against sqlite
    main.run_pipeline(
        ["all"],
        dry_run=False,
        bank_file=str(bank),
        ledger_file=str(ledger),
        base_dir=str(tmp_path),
    )

    # inspect sqlite tables for expected rows
    conn = sqlite_monkeypatch
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM raw_raw_transactions")
    assert cur.fetchone()[0] == 2
    cur.execute("SELECT COUNT(*) FROM staging_validated_transactions")
    assert cur.fetchone()[0] == 2
    cur.execute("SELECT COUNT(*) FROM analytics_reconciliation_summary")
    assert cur.fetchone()[0] == 1
    cur.execute("SELECT COUNT(*) FROM logs_etl_run_logs")
    assert cur.fetchone()[0] == 1
