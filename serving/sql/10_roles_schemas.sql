-- Roles, schemas and grants (idempotent). Passwords are set by bootstrap.py from the environment, never here.
-- Principle: raw = contract-checked landing, staging/marts = dbt output, serving = what people query.
DO $$
DECLARE r text;
BEGIN
  FOREACH r IN ARRAY ARRAY['ai_dbt','ai_analyst','ai_finops','ai_steward'] LOOP
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) THEN EXECUTE format('CREATE ROLE %I LOGIN', r); END IF;
  END LOOP;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ai_dept_head') THEN CREATE ROLE ai_dept_head NOLOGIN; END IF;
END $$;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging AUTHORIZATION ai_dbt;
CREATE SCHEMA IF NOT EXISTS marts   AUTHORIZATION ai_dbt;
CREATE SCHEMA IF NOT EXISTS analytics AUTHORIZATION ai_dbt;
CREATE SCHEMA IF NOT EXISTS serving;

-- Nobody gets anything by default
REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA raw FROM PUBLIC;

-- raw: dbt builds from it, the steward may see clear data (incl. user_id); analysts and finance may not
GRANT USAGE ON SCHEMA raw TO ai_dbt, ai_steward;
GRANT SELECT ON ALL TABLES IN SCHEMA raw TO ai_dbt, ai_steward;
ALTER DEFAULT PRIVILEGES IN SCHEMA raw GRANT SELECT ON TABLES TO ai_dbt, ai_steward;  -- applies to tables the loader recreates

-- serving: analysts / finance / department heads only ever touch views in this schema
GRANT USAGE ON SCHEMA serving TO ai_analyst, ai_finops, ai_steward, ai_dept_head;

-- Resource limits: the free-tier analogue of a Snowflake resource monitor / warehouse size
ALTER ROLE ai_analyst  SET statement_timeout = '30s';
ALTER ROLE ai_analyst  CONNECTION LIMIT 5;
ALTER ROLE ai_finops   SET statement_timeout = '30s';
ALTER ROLE ai_finops   CONNECTION LIMIT 3;
ALTER ROLE ai_steward  SET statement_timeout = '120s';
ALTER ROLE ai_steward  CONNECTION LIMIT 3;
ALTER ROLE ai_dept_head SET statement_timeout = '15s';
ALTER ROLE ai_dbt      SET statement_timeout = '10min';
