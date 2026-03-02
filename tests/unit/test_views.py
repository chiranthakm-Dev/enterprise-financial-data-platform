import os
import sys

# ensure src package importable
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))


def _read_sql(name: str) -> str:
    path = os.path.join(os.path.dirname(__file__), "..", "..", "sql", "views", name)
    with open(path, "r", encoding="utf-8") as fh:
        return fh.read()


def test_view_files_exist():
    names = [
        "v_financial_summary.sql",
        "v_reconciliation_summary.sql",
        "v_variance_summary.sql",
    ]
    base = os.path.join(os.path.dirname(__file__), "..", "..", "sql", "views")
    for n in names:
        assert os.path.exists(os.path.join(base, n)), f"{n} missing"


def test_view_definitions_contain_create():
    sql = _read_sql("v_financial_summary.sql")
    assert "CREATE OR REPLACE VIEW" in sql.upper()
    assert "V_FINANCIAL_SUMMARY" in sql.upper()
    sql = _read_sql("v_reconciliation_summary.sql")
    assert "V_RECONCILIATION_SUMMARY" in sql.upper()
    sql = _read_sql("v_variance_summary.sql")
    assert "V_VARIANCE_SUMMARY" in sql.upper()


# these tests are a sanity check rather than executing against a database.