-- Grant CONNECT privilege on the database
GRANT CONNECT ON DATABASE postgres TO postgres;

-- Grant read permissions on the source schema
GRANT USAGE ON SCHEMA public TO postgres;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO postgres;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO postgres;

-- Create destination schema (if not exists) and assign ownership
CREATE SCHEMA IF NOT EXISTS dbt_cloud AUTHORIZATION postgres;
-- OR if you created without AUTHORIZATION, you can do:
-- ALTER SCHEMA dbt_cloud OWNER TO postgres;

-- Grant write permissions on the destination schema
GRANT USAGE, CREATE ON SCHEMA dbt_cloud TO postgres;
GRANT INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA dbt_cloud TO postgres;
ALTER DEFAULT PRIVILEGES IN SCHEMA dbt_cloud GRANT INSERT, UPDATE, DELETE, TRUNCATE ON TABLES TO postgres;
