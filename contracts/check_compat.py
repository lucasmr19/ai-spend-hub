"""Schema compatibility checker (CI gate).

Usage:
    python -m contracts.check_compat OLD.json NEW.json [--migration MIGRATION.json]

Rules
-----
COMPATIBLE: new optional field, widened enum, relaxed constraint.
BREAKING:   removed field, new required field, type change, narrowed enum.
A BREAKING change is accepted only if a migration file documents how every removed
field and every new required field is mapped (rename / replaced / default / dropped).
Exit code 1 otherwise.
"""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Change:
    kind: str  # "compatible" | "breaking"
    field: str
    detail: str


def _types(spec: dict) -> set[str]:
    t = spec.get("type")
    if t is None:
        return {"enum"} if "enum" in spec else {"const"} if "const" in spec else set()
    return set(t) if isinstance(t, list) else {t}


def diff_schemas(old: dict, new: dict) -> list[Change]:
    changes: list[Change] = []
    old_p, new_p = old["properties"], new["properties"]
    old_req, new_req = set(old.get("required", [])), set(new.get("required", []))

    for name in sorted(set(old_p) - set(new_p)):
        changes.append(Change("breaking", name, "field removed"))
    for name in sorted(set(new_p) - set(old_p)):
        if name in new_req:
            changes.append(Change("breaking", name, "new REQUIRED field"))
        else:
            changes.append(Change("compatible", name, "new optional field"))
    for name in sorted(set(old_p) & set(new_p)):
        o, n = old_p[name], new_p[name]
        if _types(o) != _types(n) and name != "schema_version":
            changes.append(Change("breaking", name, f"type {sorted(_types(o))} -> {sorted(_types(n))}"))
        if "enum" in o and "enum" in n:
            removed, added = set(o["enum"]) - set(n["enum"]), set(n["enum"]) - set(o["enum"])
            if removed:
                changes.append(Change("breaking", name, f"enum values removed: {sorted(removed)}"))
            if added:
                changes.append(Change("compatible", name, f"enum values added: {sorted(added)}"))
        if name in new_req and name not in old_req and name in old_p:
            changes.append(Change("breaking", name, "became required"))
    return changes


def unmapped(changes: list[Change], migration: dict) -> list[Change]:
    """Breaking changes that the migration file does not account for."""
    renames = migration.get("renames", {})
    replaced = migration.get("replaced", {})
    defaults = migration.get("defaults", {})
    dropped = set(migration.get("dropped", []))
    new_names = set(renames.values()) | {v["by"] for v in replaced.values()} | set(defaults)
    old_names = set(renames) | set(replaced) | dropped
    out = []
    for c in changes:
        if c.kind != "breaking":
            continue
        if c.detail == "field removed" and c.field in old_names:
            continue
        if c.detail in ("new REQUIRED field", "became required") and c.field in new_names:
            continue
        out.append(c)
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("old")
    ap.add_argument("new")
    ap.add_argument("--migration")
    args = ap.parse_args(argv)

    old = json.loads(Path(args.old).read_text())
    new = json.loads(Path(args.new).read_text())
    changes = diff_schemas(old, new)
    for c in changes:
        print(f"[{c.kind.upper():10}] {c.field}: {c.detail}")

    breaking = [c for c in changes if c.kind == "breaking"]
    if not breaking:
        print("OK: backward compatible")
        return 0
    if not args.migration:
        print(f"FAIL: {len(breaking)} breaking change(s) and no migration file given")
        return 1
    left = unmapped(changes, json.loads(Path(args.migration).read_text()))
    if left:
        print(f"FAIL: {len(left)} breaking change(s) not covered by the migration file:")
        for c in left:
            print(f"   - {c.field}: {c.detail}")
        return 1
    print("OK: breaking (MAJOR) change fully covered by migration file")
    return 0


if __name__ == "__main__":
    sys.exit(main())
