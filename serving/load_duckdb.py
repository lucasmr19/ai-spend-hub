"""Load a Gold export into a DuckDB file (schema ``raw``) for dev / CI without any server.

    python -m serving.load_duckdb <export_dir> <db_file>

Same contract check and same control sums as the PostgreSQL loader.
"""
from __future__ import annotations

import json
import sys
from decimal import Decimal
from pathlib import Path

import duckdb

from .contract import check_manifest, load_contract
from .loader import LoadError


def load_export_duckdb(export_dir: str | Path, db_file: str | Path) -> dict[str, int]:
    export_dir = Path(export_dir)
    manifest = json.loads((export_dir / "manifest.json").read_text())
    check_manifest(manifest, load_contract())
    con = duckdb.connect(str(db_file))
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    out = {}
    for name, t in manifest["tables"].items():
        con.execute(f'CREATE OR REPLACE TABLE raw."{name}" AS SELECT * FROM read_parquet(?)', [str(export_dir / t["file"])])
        n = con.execute(f'SELECT count(*) FROM raw."{name}"').fetchone()[0]
        if n != t["rows"]:
            raise LoadError(f"{name}: loaded {n}, manifest {t['rows']}")
        for col, expected in t["sums"].items():
            got = con.execute(f'SELECT sum("{col}") FROM raw."{name}"').fetchone()[0]
            if Decimal(str(got or 0)) != Decimal(expected or "0"):
                raise LoadError(f"{name}.{col}: {got} != {expected}")
        out[name] = n
    con.close()
    return out


if __name__ == "__main__":
    for k, v in load_export_duckdb(sys.argv[1], sys.argv[2]).items():
        print(f"{k:26} {v:>9,}")
