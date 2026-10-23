"""Provision roles, schemas, department roles and serving views in PostgreSQL.

    SERVING_DSN=postgresql://admin:...@host/db python -m serving.bootstrap roles [--dev]
    ... dbt build --target postgres ...
    SERVING_DSN=... python -m serving.bootstrap views

Passwords come from env vars AI_DBT_PASSWORD, AI_ANALYST_PASSWORD, AI_FINOPS_PASSWORD, AI_STEWARD_PASSWORD,
AI_DEPT_HEAD_PASSWORD. ``--dev`` uses 'local-dev-only' for roles without one (local demo only).
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

import psycopg
from psycopg import sql

SQL_DIR = Path(__file__).parent / "sql"
ROLES = ["ai_dbt", "ai_analyst", "ai_finops", "ai_steward"]


def dept_role(department_id: str) -> str:
    return "ai_dept_" + re.sub(r"[^a-z0-9]+", "_", department_id.lower())


def _set_password(cur, role: str, env_var: str, dev: bool) -> None:
    pw = os.environ.get(env_var) or ("local-dev-only" if dev else None)
    if pw is None:
        raise SystemExit(f"{env_var} is not set (use --dev only for a local demo)")
    cur.execute(sql.SQL("ALTER ROLE {} PASSWORD {}").format(sql.Identifier(role), sql.Literal(pw)))


def bootstrap_roles(dsn: str, *, dev: bool = False) -> list[str]:
    with psycopg.connect(dsn, autocommit=True) as pg, pg.cursor() as cur:
        cur.execute((SQL_DIR / "10_roles_schemas.sql").read_text())
        for role in ROLES:
            _set_password(cur, role, role.upper() + "_PASSWORD", dev)
        cur.execute("CREATE TABLE IF NOT EXISTS serving.dept_access (role_name text NOT NULL, department_id text NOT NULL, "
                    "PRIMARY KEY (role_name, department_id))")
        cur.execute("TRUNCATE serving.dept_access")
        cur.execute("SELECT department_id FROM raw.dim_departments ORDER BY 1")
        made = []
        for (dept,) in cur.fetchall():
            role = dept_role(dept)
            cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,))
            if not cur.fetchone():
                cur.execute(sql.SQL("CREATE ROLE {} LOGIN IN ROLE ai_dept_head").format(sql.Identifier(role)))
            _set_password(cur, role, "AI_DEPT_HEAD_PASSWORD", dev)
            cur.execute("INSERT INTO serving.dept_access VALUES (%s, %s)", (role, dept))
            made.append(role)
        # raw schema may predate the default privileges: grant again on the tables that exist now
        cur.execute("GRANT SELECT ON ALL TABLES IN SCHEMA raw TO ai_dbt, ai_steward")
        return made


def bootstrap_views(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as pg, pg.cursor() as cur:
        cur.execute((SQL_DIR / "30_serving_views.sql").read_text())


if __name__ == "__main__":
    if len(sys.argv) < 2 or "SERVING_DSN" not in os.environ:
        raise SystemExit(__doc__)
    if sys.argv[1] == "roles":
        print("\n".join(bootstrap_roles(os.environ["SERVING_DSN"], dev="--dev" in sys.argv)))
    elif sys.argv[1] == "views":
        bootstrap_views(os.environ["SERVING_DSN"])
    else:
        raise SystemExit(__doc__)
