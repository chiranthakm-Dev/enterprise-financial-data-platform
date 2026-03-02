-- View combining daily and monthly summaries into a single financial summary.
CREATE OR REPLACE VIEW analytics.v_financial_summary AS
SELECT ds.transaction_date,
       ds.account_id,
       ds.daily_total,
       ms.monthly_total
FROM analytics.daily_summary ds
LEFT JOIN analytics.monthly_summary ms
  ON ds.account_id = ms.account_id
  AND TO_CHAR(ds.transaction_date, 'YYYY-MM') = ms.transaction_month;
