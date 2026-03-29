-- ============================================================================
-- GOLD SCHEMA INITIALIZATION - Vélib Data Engineering Project
-- ============================================================================
-- Description: Creates the gold schema managed by dbt.
--              dbt will create / replace all tables and views within it.
-- Run: automatically on first PostgreSQL init via /docker-entrypoint-initdb.d/
--      If the DB already exists, run manually:
--        docker exec -i velib_postgres psql -U velib -d velib_dw < sql/03_init_gold_schema.sql
-- ============================================================================

-- Create schemas
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS gold_staging;   -- staging views created by dbt

-- Grant full access to the application user
GRANT USAGE ON SCHEMA gold TO velib;
GRANT CREATE ON SCHEMA gold TO velib;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA gold TO velib;

GRANT USAGE ON SCHEMA gold_staging TO velib;
GRANT CREATE ON SCHEMA gold_staging TO velib;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA gold_staging TO velib;

-- Ensure future objects are also accessible
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT ALL ON TABLES TO velib;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT ALL ON SEQUENCES TO velib;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold_staging GRANT ALL ON TABLES TO velib;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold_staging GRANT ALL ON SEQUENCES TO velib;

-- Descriptive comments
COMMENT ON SCHEMA gold IS 'Gold layer — business-ready analytical models created by dbt';
COMMENT ON SCHEMA gold_staging IS 'Staging views created by dbt (lightweight views on Silver)';

-- Summary
SELECT
    schema_name,
    'created' AS status
FROM information_schema.schemata
WHERE schema_name IN ('gold', 'gold_staging');
