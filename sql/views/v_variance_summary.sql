-- View showing variance analysis results
CREATE OR REPLACE VIEW analytics.v_variance_summary AS
SELECT account_id,
       actual_amount,
       budget_amount,
       variance_amount,
       variance_percentage
FROM analytics.variance_analysis;
