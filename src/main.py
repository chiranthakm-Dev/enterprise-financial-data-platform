"""CLI entrypoint for the ETL pipeline (scaffold)."""
import argparse
from pathlib import Path
from datetime import datetime
from src.db.connection import create_run_id
from src.config.config import has_snowflake_credentials


def run_pipeline(
    stages,
    dry_run=False,
    bank_file=None,
    ledger_file=None,
    budget_file=None,
    base_dir: str = "data",
):
    print(f"Running stages: {stages} (dry_run={dry_run})")
    from src.ingest.ingest import ingest_file
    from src.validation.validate import validate_file
    from src.reconcile.reconcile import reconcile
    from src.aggregate.aggregate import aggregate
    from src.db.connection import run_sql_file
    from src.logging.etl_logger import ETLLogger

    # working directory for processed files
    processed = Path(base_dir) / "processed"
    processed.mkdir(exist_ok=True, parents=True)

    logger = ETLLogger(dry_run=dry_run)
    run_metrics = {
        "run_id": create_run_id(),
        "start_time": datetime.utcnow().isoformat(),
        "end_time": None,
        "total_ingested": 0,
        "total_validated": 0,
        "total_matched": 0,
        "match_rate": 0.0,
        "status": "RUNNING",
    }

    # track intermediate paths
    bank_validated = None
    ledger_validated = None

    if "ingest" in stages or "all" in stages:
        if bank_file:
            print("Ingesting bank file")
            ingest_file(bank_file, source_system="bank", dry_run=dry_run, out_dir=str(processed))
        if ledger_file:
            print("Ingesting ledger file")
            ingest_file(ledger_file, source_system="ledger", dry_run=dry_run, out_dir=str(processed))
        if budget_file:
            print("Ingesting budget file")
            ingest_file(budget_file, source_system="budget", dry_run=dry_run, out_dir=str(processed))

    if "validate" in stages or "all" in stages:
        if bank_file:
            bank_validated = str(processed / f"{Path(bank_file).stem}.validated.csv")
            validate_file(str(processed / f"{Path(bank_file).stem}.processed.csv"), dry_run=dry_run, out_dir=str(processed))
        if ledger_file:
            ledger_validated = str(processed / f"{Path(ledger_file).stem}.validated.csv")
            validate_file(str(processed / f"{Path(ledger_file).stem}.processed.csv"), dry_run=dry_run, out_dir=str(processed))

    if "reconcile" in stages or "all" in stages:
        print("Reconciling")
        recon_res = reconcile(bank_validated, ledger_validated, dry_run=dry_run, out_dir=str(processed))
        print(recon_res)
        run_metrics["total_matched"] = recon_res.get("matched_count")
        run_metrics["match_rate"] = recon_res.get("match_rate_percentage")

    if "aggregate" in stages or "all" in stages:
        print("Aggregating")
        paths = [p for p in (bank_validated, ledger_validated) if p]
        agg_res = aggregate(paths, budget_file, dry_run=dry_run, out_dir=str(processed))
        print(agg_res)
            # no metrics tracked yet

    if "report" in stages or "all" in stages:
        # only attempt to install/update views when we're actually talking to
        # Snowflake.  In dry-run mode (or when credentials are missing) the
        # views have no real effect and the underlying helper will fail
        # attempting to open a connection.  Skip the step in that case.
        if dry_run or not has_snowflake_credentials():
            print("Skipping report/view creation (dry-run or no credentials)")
        else:
            print("Creating reporting views")
            # run SQL view definitions
            run_sql_file("sql/views/v_financial_summary.sql")
            run_sql_file("sql/views/v_reconciliation_summary.sql")
            run_sql_file("sql/views/v_variance_summary.sql")

    # finalize run metrics
    run_metrics["end_time"] = datetime.utcnow().isoformat()
    run_metrics["status"] = "SUCCESS"
    logger.log_run(run_metrics)



def main():
    parser = argparse.ArgumentParser(description="Enterprise Financial Analytics Engine CLI")
    parser.add_argument("--stages", nargs="*", default=["all"], help="Stages to run: ingest,validate,reconcile,aggregate,report")
    parser.add_argument("--bank-file", help="Path to bank CSV for ingestion")
    parser.add_argument("--ledger-file", help="Path to ledger CSV for ingestion")
    parser.add_argument("--budget-file", help="Path to budget CSV for ingestion")
    parser.add_argument("--dry-run", action="store_true", help="Do not write to DB; run validations only")
    args = parser.parse_args()
    run_pipeline(
        args.stages,
        dry_run=args.dry_run,
        bank_file=args.bank_file,
        ledger_file=args.ledger_file,
        budget_file=args.budget_file,
    )


if __name__ == "__main__":
    main()
