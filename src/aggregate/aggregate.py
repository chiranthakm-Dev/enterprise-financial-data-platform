"""Aggregation module to produce financial summaries.

Works in dry-run mode (CSV I/O) or writes to Snowflake analytics tables when
credentials are available.
"""
from pathlib import Path
import logging
from typing import Dict, List, Optional

try:
    import pandas as pd  # type: ignore
    PANDAS_AVAILABLE = True
except Exception:
    pd = None
    PANDAS_AVAILABLE = False

from src.config.config import has_snowflake_credentials
from src.db.connection import execute_many

LOG = logging.getLogger(__name__)


def _read_validated(path: str) -> List[Dict]:
    records = []
    if PANDAS_AVAILABLE:
        df = pd.read_csv(path)
        records = df.to_dict(orient="records")
    else:
        import csv

        with open(path, encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for r in reader:
                records.append(r)
    return records


def aggregate(
    validated_paths: Optional[List[str]] = None,
    budget_path: Optional[str] = None,
    dry_run: bool = True,
    out_dir: str = "data/processed",
) -> Dict:
    """Run aggregation and return metrics."""
    if dry_run or not has_snowflake_credentials():
        if not validated_paths:
            raise ValueError("validated_paths required in dry-run mode")
        # concatenate all validated inputs
        data = []
        for p in validated_paths:
            data.extend(_read_validated(p))
    else:
        # fetch all validated from staging
        sql = (
            "SELECT transaction_date, account_id, amount, transaction_type FROM staging.validated_transactions"
        )
        rows = execute_query(sql)
        data = [
            {"transaction_date": r[0], "account_id": r[1], "amount": r[2], "transaction_type": r[3]}
            for r in rows
        ]

    # convert to dataframe for convenience
    if PANDAS_AVAILABLE:
        df = pd.DataFrame(data)
        df["transaction_date"] = pd.to_datetime(df["transaction_date"])
        # daily totals per account
        daily = df.groupby([df.transaction_date.dt.date, "account_id"])["amount"].sum().reset_index()
        daily.columns = ["transaction_date", "account_id", "daily_total"]
        # monthly totals per account
        monthly = df.groupby([df.transaction_date.dt.to_period("M"), "account_id"])["amount"].sum().reset_index()
        monthly["transaction_month"] = monthly["transaction_date"].astype(str)
        monthly = monthly.drop(columns=["transaction_date"])
        # debit vs credit
        debit_credit = df.groupby(["account_id", "transaction_type"])["amount"].sum().reset_index()
        # account summary
        account_summary = df.groupby("account_id")["amount"].sum().reset_index()
        account_summary.columns = ["account_id", "total_amount"]

        variance = None
        if budget_path:
            bud = pd.read_csv(budget_path)
            if "account_id" in bud.columns and "budget_amount" in bud.columns:
                merged = account_summary.merge(bud, on="account_id", how="left")
                merged["variance_amount"] = merged["total_amount"] - merged["budget_amount"]
                merged["variance_percentage"] = merged["variance_amount"] / merged["budget_amount"].replace(0, pd.NA)
                variance = merged
    else:
        # fallback using simple loops: only daily and monthly sums
        daily = {}
        monthly = {}
        debit_credit = {}
        account_summary = {}
        for r in data:
            date = r.get("transaction_date")
            acct = r.get("account_id")
            amt = float(r.get("amount", 0))
            ttype = r.get("transaction_type")
            daily_key = (date, acct)
            daily[daily_key] = daily.get(daily_key, 0) + amt
            mon = date[:7]  # assume yyyy-mm-dd
            monthly_key = (mon, acct)
            monthly[monthly_key] = monthly.get(monthly_key, 0) + amt
            dc_key = (acct, ttype)
            debit_credit[dc_key] = debit_credit.get(dc_key, 0) + amt
            account_summary[acct] = account_summary.get(acct, 0) + amt
        variance = None
        if budget_path:
            # basic variance: join on account_id from budget CSV
            import csv
            bud = {}
            with open(budget_path, encoding="utf-8") as fh:
                reader = csv.DictReader(fh)
                for r in reader:
                    bud[r.get("account_id")] = float(r.get("budget_amount", 0))
            variance = []
            for acct, total in account_summary.items():
                bamt = bud.get(acct, 0)
                varamt = total - bamt
                varpct = varamt / bamt if bamt != 0 else None
                variance.append({"account_id": acct, "actual_amount": total, "budget_amount": bamt, "variance_amount": varamt, "variance_percentage": varpct})
        # convert dicts to list of dicts for writing
        daily = [{"transaction_date": k[0], "account_id": k[1], "daily_total": v} for k, v in daily.items()]
        monthly = [{"transaction_month": k[0], "account_id": k[1], "monthly_total": v} for k, v in monthly.items()]
        debit_credit = [{"account_id": k[0], "transaction_type": k[1], "amount": v} for k, v in debit_credit.items()]
        account_summary = [{"account_id": k, "total_amount": v} for k, v in account_summary.items()]

    # output results
    if dry_run or not has_snowflake_credentials():
        out_path = Path(out_dir)
        out_path.mkdir(parents=True, exist_ok=True)
        import csv

        def write(name, rows):
            if not rows:
                return
            with open(out_path / name, "w", encoding="utf-8", newline="") as fh:
                writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
                writer.writeheader()
                for r in rows:
                    writer.writerow(r)

        write("daily_summary.csv", daily if isinstance(daily, list) else daily.to_dict(orient="records"))
        write("monthly_summary.csv", monthly if isinstance(monthly, list) else monthly.to_dict(orient="records"))
        if variance is not None:
            write("variance_analysis.csv", variance if isinstance(variance, list) else variance.to_dict(orient="records"))
    else:
        # TODO: write to Snowflake analytics tables when schema defined
        LOG.warning("Snowflake write for aggregation not yet implemented")

    return {
        "daily_count": len(daily) if isinstance(daily, list) else len(daily.index),
        "monthly_count": len(monthly) if isinstance(monthly, list) else len(monthly.index),
        "variance_rows": len(variance) if variance is not None and isinstance(variance, list) else (len(variance.index) if variance is not None else 0),
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Aggregate validated transactions")
    parser.add_argument("--validated", help="path to validated transactions CSV")
    parser.add_argument("--budget", help="path to budget CSV (optional)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    res = aggregate(args.validated, args.budget, dry_run=args.dry_run)
    print(res)
