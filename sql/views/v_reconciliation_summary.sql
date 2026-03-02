-- View exposing reconciliation summary metrics
CREATE OR REPLACE VIEW analytics.v_reconciliation_summary AS
SELECT run_id,
       start_time,
       end_time,
       total_records,
       matched_count,
       unmatched_count,
       match_rate
FROM analytics.reconciliation_summary;
