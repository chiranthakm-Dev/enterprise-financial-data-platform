"""Structured ETL logging and run metadata storage.

Provides both console logging and persistence of run-level metrics.
In dry-run mode or when Snowflake credentials are absent, logs are written
to a local CSV under `data/logs/etl_run_logs.csv`.
When Snowflake is available, the logger inserts records into
`logs.etl_run_logs` table.
"""
import csv
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

from src.config.config import has_snowflake_credentials
from src.db.connection import execute_many

LOG = logging.getLogger(__name__)


class ETLLogger:
    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run or not has_snowflake_credentials()
        if self.dry_run:
            self.log_file = Path("data/logs")
            self.log_file.mkdir(parents=True, exist_ok=True)
            self.log_csv = self.log_file / "etl_run_logs.csv"
            if not self.log_csv.exists():
                with open(self.log_csv, "w", newline="", encoding="utf-8") as fh:
                    writer = csv.writer(fh)
                    writer.writerow([
                        "run_id",
                        "start_time",
                        "end_time",
                        "total_ingested",
                        "total_validated",
                        "total_matched",
                        "match_rate",
                        "status",
                    ])

    def log_run(self, metrics: Dict[str, Optional[float]]):
        """Record a run's metrics.

        Required keys: run_id, start_time, end_time, total_ingested,
        total_validated, total_matched, match_rate, status
        """
        if self.dry_run:
            with open(self.log_csv, "a", newline="", encoding="utf-8") as fh:
                writer = csv.writer(fh)
                writer.writerow([
                    metrics.get("run_id"),
                    metrics.get("start_time"),
                    metrics.get("end_time"),
                    metrics.get("total_ingested"),
                    metrics.get("total_validated"),
                    metrics.get("total_matched"),
                    metrics.get("match_rate"),
                    metrics.get("status"),
                ])
            LOG.info("Logged ETL run to %s", self.log_csv)
        else:
            insert = (
                "INSERT INTO logs.etl_run_logs (run_id, start_time, end_time, total_ingested, total_validated, total_matched, match_rate, status) "
                "VALUES (%(run_id)s, %(start_time)s, %(end_time)s, %(total_ingested)s, %(total_validated)s, %(total_matched)s, %(match_rate)s, %(status)s)"
            )
            execute_many(insert, [metrics])
            LOG.info("Logged ETL run to Snowflake table logs.etl_run_logs")
