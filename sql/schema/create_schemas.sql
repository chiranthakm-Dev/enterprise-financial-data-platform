-- Idempotent schema creation for Snowflake
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS logs;

-- Example grants (replace role names as appropriate)
-- GRANT USAGE ON DATABASE <your_database> TO ROLE etl_service_role;
-- GRANT USAGE ON SCHEMA raw TO ROLE etl_service_role;
-- GRANT USAGE ON SCHEMA staging TO ROLE etl_service_role;
-- GRANT USAGE ON SCHEMA analytics TO ROLE analyst_role;
-- REVOKE USAGE ON SCHEMA raw FROM ROLE analyst_role; -- ensure analysts cannot access raw
