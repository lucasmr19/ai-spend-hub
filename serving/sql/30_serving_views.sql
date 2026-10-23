-- Serving views (run AFTER dbt, because dbt recreates the mart tables). Idempotent.
-- Access model:
--   ai_finops, ai_analyst, ai_steward : all departments
--   ai_dept_head members              : only the departments mapped to their role in serving.dept_access
-- NB: inside a SECURITY DEFINER function current_user is the owner, so the login identity is session_user.
-- Views are owned by the admin role and are security_barrier, so the filter cannot be bypassed by
-- user-defined functions. Row-level security policies were not used because dbt drops/recreates tables
-- (policies would be lost on every run); see docs/adr/0003.
CREATE TABLE IF NOT EXISTS serving.dept_access (role_name text NOT NULL, department_id text NOT NULL, PRIMARY KEY (role_name, department_id));

CREATE OR REPLACE FUNCTION serving.can_see_department(dept text) RETURNS boolean
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = serving, pg_catalog AS $$
  SELECT pg_has_role(session_user, 'ai_finops', 'member') OR pg_has_role(session_user, 'ai_analyst', 'member')
      OR pg_has_role(session_user, 'ai_steward', 'member')
      OR EXISTS (SELECT 1 FROM serving.dept_access a WHERE a.role_name = session_user AND a.department_id = dept)
$$;
REVOKE ALL ON FUNCTION serving.can_see_department(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION serving.can_see_department(text) TO ai_analyst, ai_finops, ai_steward, ai_dept_head;

CREATE OR REPLACE VIEW serving.cost_by_department_month WITH (security_barrier = true) AS
  SELECT * FROM marts.mart_cost_by_department_month WHERE serving.can_see_department(department_id);
CREATE OR REPLACE VIEW serving.chargeback WITH (security_barrier = true) AS
  SELECT * FROM marts.mart_chargeback WHERE serving.can_see_department(department_id);
CREATE OR REPLACE VIEW serving.slo_compliance WITH (security_barrier = true) AS
  SELECT * FROM marts.mart_slo_compliance WHERE serving.can_see_department(department_id);
CREATE OR REPLACE VIEW serving.inference WITH (security_barrier = true) AS
  SELECT * FROM marts.fct_inference WHERE serving.can_see_department(department_id);
-- not department-scoped (company-wide, no personal data)
CREATE OR REPLACE VIEW serving.model_efficiency AS SELECT * FROM marts.mart_model_efficiency;
CREATE OR REPLACE VIEW serving.cache_savings    AS SELECT * FROM marts.mart_cache_savings;
CREATE OR REPLACE VIEW serving.data_reliability AS SELECT * FROM marts.mart_data_reliability;

GRANT SELECT ON ALL TABLES IN SCHEMA serving TO ai_analyst, ai_finops, ai_steward, ai_dept_head;
REVOKE ALL ON serving.dept_access FROM PUBLIC, ai_analyst, ai_finops, ai_dept_head;

-- Steward only: user_id in clear, straight from raw, for investigations (audit it in production)
CREATE OR REPLACE VIEW serving.inference_with_user AS
  SELECT event_id, event_ts, app_id, department_id, user_id, status, cost_units FROM raw.fact_inference;
REVOKE ALL ON serving.inference_with_user FROM PUBLIC, ai_analyst, ai_finops, ai_dept_head;
GRANT SELECT ON serving.inference_with_user TO ai_steward;
GRANT USAGE ON SCHEMA marts TO ai_analyst, ai_finops, ai_steward, ai_dept_head;  -- views run as owner, but keeps catalog lookups clean
REVOKE ALL ON ALL TABLES IN SCHEMA marts FROM ai_analyst, ai_finops, ai_dept_head;
