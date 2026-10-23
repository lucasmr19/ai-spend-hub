"""Gold -> serving contract: tables, columns, types and control-sum columns.

The contract file is the single source of truth for the raw tables created in the warehouse.
The loader refuses an export whose manifest does not match it.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

CONTRACT_PATH = Path(__file__).parent / "contract" / "gold_export.v1.json"


class ContractError(Exception):
    pass


def load_contract(path: Path = CONTRACT_PATH) -> dict:
    return json.loads(Path(path).read_text())


def from_manifest(manifest: dict) -> dict:
    """Build a contract from a manifest (used once to create / deliberately change the contract)."""
    return {
        "version": 1,
        "tables": {
            name: {"columns": t["columns"], "sums": sorted(t["sums"])} for name, t in sorted(manifest["tables"].items())
        },
    }


def pg_type(spark_type: str) -> str:
    m = re.fullmatch(r"decimal\((\d+),(\d+)\)", spark_type)
    if m:
        return f"numeric({m.group(1)},{m.group(2)})"
    try:
        return {"string": "text", "bigint": "bigint", "int": "integer", "double": "double precision",
                "boolean": "boolean", "date": "date", "timestamp": "timestamptz"}[spark_type]
    except KeyError:
        raise ContractError(f"unsupported type {spark_type!r}") from None


def check_manifest(manifest: dict, contract: dict) -> None:
    """Raise ContractError listing every difference between an export manifest and the contract."""
    problems: list[str] = []
    exp, got = contract["tables"], manifest["tables"]
    for name in sorted(set(exp) - set(got)):
        problems.append(f"missing table {name}")
    for name in sorted(set(got) - set(exp)):
        problems.append(f"unexpected table {name}")
    for name in sorted(set(exp) & set(got)):
        ec = {c["name"]: c["type"] for c in exp[name]["columns"]}
        gc = {c["name"]: c["type"] for c in got[name]["columns"]}
        for c in sorted(set(ec) - set(gc)):
            problems.append(f"{name}: missing column {c}")
        for c in sorted(set(gc) - set(ec)):
            problems.append(f"{name}: unexpected column {c}")
        for c in sorted(set(ec) & set(gc)):
            if ec[c] != gc[c]:
                problems.append(f"{name}.{c}: type {gc[c]} != contract {ec[c]}")
        if sorted(got[name]["sums"]) != sorted(exp[name]["sums"]):
            problems.append(f"{name}: control-sum columns differ")
    if problems:
        raise ContractError("; ".join(problems))
