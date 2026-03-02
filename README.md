# Enterprise Financial Analytics Engine

Minimal scaffold for the Enterprise Financial Analytics Engine project.

Overview
- Modular Python ETL pipeline for ingestion, validation, reconciliation, aggregation, and reporting against Snowflake.

Structure
- `src/` - Python source code
- `sql/` - DDL and view scripts
- `data/` - sample datasets
- `tests/` - unit and integration tests
- `docs/` - documentation and runbook

Getting started
1. Copy `.env.example` to `.env` and fill Snowflake creds.
2. Create a virtualenv and install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

3. Run the CLI (scaffold):

```bash
python -m src.main --help
```

Security
- Do not commit `.env` or any secrets.
