"""Snowflake connection helpers using `snowflake-connector-python`.

This module exposes simple helpers for connecting and executing parameterized
queries. It intentionally avoids embedding credentials; use environment vars
loaded via `src.config.config.get_config()`.
"""
from contextlib import contextmanager
import uuid
import logging
from typing import Any, Iterable, List, Optional

from src.config.config import get_config

LOG = logging.getLogger(__name__)


@contextmanager
def snowflake_connection():
    cfg = get_config()
    if not cfg.snowflake_account or not cfg.snowflake_user:
        raise RuntimeError("Snowflake credentials are not configured in environment")
    # Import connector lazily so modules that only run local/dry-run code do not require the package
    try:
        import snowflake.connector
    except Exception as exc:
        raise RuntimeError("snowflake-connector-python is required to open a Snowflake connection") from exc

    conn = snowflake.connector.connect(
        user=cfg.snowflake_user,
        password=cfg.snowflake_password,
        account=cfg.snowflake_account,
        warehouse=cfg.snowflake_warehouse,
        role=cfg.snowflake_role,
        database=cfg.snowflake_database,
    )
    try:
        yield conn
    finally:
        try:
            conn.close()
        except Exception:
            LOG.exception("Error closing Snowflake connection")


def execute_query(query: str, params: Optional[dict] = None) -> List[tuple]:
    """Execute a single query with optional parameter dictionary and return rows."""
    with snowflake_connection() as conn:
        cur = conn.cursor()
        try:
            if params:
                cur.execute(query, params)
            else:
                cur.execute(query)
            try:
                rows = cur.fetchall()
            except Exception:
                rows = []
            return rows
        finally:
            cur.close()


def execute_many(query: str, params_seq: Iterable[dict]):
    """Execute a parameterized statement multiple times in a transaction."""
    with snowflake_connection() as conn:
        cur = conn.cursor()
        try:
            for params in params_seq:
                cur.execute(query, params)
            conn.commit()
        finally:
            cur.close()


def run_sql_file(path: str):
    """Run a .sql file (multiple statements)."""
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    with snowflake_connection() as conn:
        cur = conn.cursor()
        try:
            for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
                cur.execute(stmt)
            conn.commit()
        finally:
            cur.close()


def create_run_id() -> str:
    return str(uuid.uuid4())
