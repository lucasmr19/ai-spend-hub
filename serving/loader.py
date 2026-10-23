"""Load a Gold export (Parquet + manifest.json) into PostgreSQL ``raw`` schema.

    python -m serving.loader <export_dir>            # env SERVING_DSN, e.g. postgresql://user:pw@host/db

Guarantees: manifest matches the contract (else nothing is touched); the whole load is ONE transaction
(a failed check rolls back, the previous data stays); after COPY every row count and control sum equals
the manifest. Re-running with the same export is idempotent (tables are replaced, not appended).
"""
from __future__ import annotations

import csv
import io
import json
import os
import sys
from decimal import Decimal
from pathlib import Path

import duckdb
import psycopg

from .contract import ContractError, check_manifest, load_contract, pg_type
from .ddl import raw_table_ddl


class LoadError(Exception):
    pass


def _rows(con: duckdb.DuckDBPyConnection, parquet: Path, columns: list[str]):
    cols = ", ".join(f'"{c}"' for c in columns)
    return con.execute(f"SELECT {cols} FROM read_parquet(?)", [str(parquet)]).fetchall()


def _csv(rows) -> io.StringIO:
    buf = io.StringIO()
    w = csv.writer(buf)
    for r in rows:
        w.writerow(["\\N" if v is None else v for v in r])
    buf.seek(0)
    return buf


def _prepare_table(cur, schema: str, name: str, table: dict) -> None:
    """Create the table, or TRUNCATE it when it exists with exactly the contract's columns.

    TRUNCATE (not DROP) keeps grants and the views built on top of it; a structural change needs a
    deliberate migration, never an implicit DROP CASCADE.
    """
    cur.execute("SELECT column_name, data_type, numeric_precision, numeric_scale FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position", (schema, name))
    existing = cur.fetchall()
    if not existing:
        cur.execute(raw_table_ddl(name, table, schema))
        return
    want = [c["name"] for c in table["columns"]]
    if [r[0] for r in existing] != want:
        raise LoadError(f"{name}: existing table columns differ from the contract; migrate it explicitly")
    for (col, dtype, prec, scale), c in zip(existing, table["columns"], strict=True):
        if dtype == "numeric" and f"numeric({prec},{scale})" != pg_type(c["type"]):
            raise LoadError(f"{name}.{col}: numeric precision differs from the contract; migrate it explicitly")
    cur.execute(f'TRUNCATE "{schema}"."{name}"')


def load_export(export_dir: str | Path, dsn: str, *, schema: str = "raw") -> dict[str, int]:
    export_dir = Path(export_dir)
    manifest = json.loads((export_dir / "manifest.json").read_text())
    contract = load_contract()
    check_manifest(manifest, contract)  # ContractError before touching the database

    con = duckdb.connect()
    loaded: dict[str, int] = {}
    with psycopg.connect(dsn) as pg:  # one transaction: commit on success, rollback on any exception
        with pg.cursor() as cur:
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
            for name, table in contract["tables"].items():
                _prepare_table(cur, schema, name, table)
            for name, t in manifest["tables"].items():
                cols = [c["name"] for c in t["columns"]]
                rows = _rows(con, export_dir / t["file"], cols)
                if len(rows) != t["rows"]:
                    raise LoadError(f"{name}: parquet has {len(rows)} rows, manifest says {t['rows']}")
                collist = ", ".join(f'"{c}"' for c in cols)
                with cur.copy(f"COPY \"{schema}\".\"{name}\" ({collist}) FROM STDIN WITH (FORMAT csv, NULL '\\N')") as cp:
                    cp.write(_csv(rows).read())
                cur.execute(f'SELECT count(*) FROM "{schema}"."{name}"')
                n = cur.fetchone()[0]
                if n != t["rows"]:
                    raise LoadError(f"{name}: loaded {n} rows, expected {t['rows']}")
                for col, expected in t["sums"].items():
                    cur.execute(f'SELECT sum("{col}") FROM "{schema}"."{name}"')
                    got = cur.fetchone()[0]
                    if Decimal(str(got if got is not None else 0)) != Decimal(expected if expected is not None else "0"):
                        raise LoadError(f"{name}.{col}: control sum {got} != manifest {expected}")
                loaded[name] = n
    return loaded


def main(argv: list[str]) -> int:
    if len(argv) != 1 or "SERVING_DSN" not in os.environ:
        print("usage: SERVING_DSN=postgresql://... python -m serving.loader <export_dir>", file=sys.stderr)
        return 2
    try:
        loaded = load_export(argv[0], os.environ["SERVING_DSN"])
    except (ContractError, LoadError) as e:
        print(f"LOAD REFUSED: {e}", file=sys.stderr)
        return 1
    for k, v in loaded.items():
        print(f"{k:26} {v:>9,}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
