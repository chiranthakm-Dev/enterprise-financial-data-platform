-- Idempotent table creation
CREATE TABLE IF NOT EXISTS raw.raw_transactions (
    transaction_id VARCHAR,
    transaction_date DATE,
    account_id VARCHAR,
    amount NUMBER,
    transaction_type VARCHAR,
    source_system VARCHAR,
    ingestion_timestamp TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (transaction_id, source_system)
);

CREATE TABLE IF NOT EXISTS staging.validated_transactions (
    transaction_id VARCHAR,
    transaction_date DATE,
    account_id VARCHAR,
    amount NUMBER,
    transaction_type VARCHAR,
    source_system VARCHAR,
    ingestion_timestamp TIMESTAMP_LTZ,
    validation_passed BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS staging.rejected_records (
    transaction_id VARCHAR,
    account_id VARCHAR,
    reason VARCHAR,
    raw_payload VARIANT,
    rejected_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS analytics.matched_transactions (
    transaction_id VARCHAR,
    bank_amount NUMBER,
    ledger_amount NUMBER,
    match_type VARCHAR,
    matched_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS analytics.unmatched_bank (
    transaction_id VARCHAR,
    account_id VARCHAR,
    amount NUMBER,
    transaction_date DATE,
    source_system VARCHAR
);

CREATE TABLE IF NOT EXISTS analytics.unmatched_ledger (
    transaction_id VARCHAR,
    account_id VARCHAR,
    amount NUMBER,
    transaction_date DATE,
    source_system VARCHAR
);

CREATE TABLE IF NOT EXISTS analytics.reconciliation_summary (
    run_id VARCHAR PRIMARY KEY,
    start_time TIMESTAMP_LTZ,
    end_time TIMESTAMP_LTZ,
    total_records NUMBER,
    matched_count NUMBER,
    unmatched_count NUMBER,
    match_rate FLOAT
);

CREATE TABLE IF NOT EXISTS logs.etl_run_logs (
    run_id VARCHAR PRIMARY KEY,
    start_time TIMESTAMP_LTZ,
    end_time TIMESTAMP_LTZ,
    total_ingested NUMBER,
    total_validated NUMBER,
    total_matched NUMBER,
    match_rate FLOAT,
    status VARCHAR
);
