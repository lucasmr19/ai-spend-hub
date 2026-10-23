"""DDL for the raw schema, generated from the contract (so the contract is the only place types live)."""
from __future__ import annotations

from .contract import pg_type


def raw_table_ddl(name: str, table: dict, schema: str = "raw") -> str:
    cols = ",\n  ".join(f'"{c["name"]}" {pg_type(c["type"])}' for c in table["columns"])
    return f'CREATE TABLE "{schema}"."{name}" (\n  {cols}\n);'


def raw_schema_ddl(contract: dict, schema: str = "raw") -> list[str]:
    stmts = [f'CREATE SCHEMA IF NOT EXISTS "{schema}";']
    for name, table in contract["tables"].items():
        stmts.append(f'DROP TABLE IF EXISTS "{schema}"."{name}";')
        stmts.append(raw_table_ddl(name, table, schema))
    return stmts
