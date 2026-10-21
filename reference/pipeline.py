"""Reference pipeline: landing files -> validated, deduplicated, canonical events -> KPIs.

It reads ONLY the landed files and the dimension CSVs (never generator internals), so
it proves the landed data is sufficient to recover the ground truth. The Databricks
pipeline (week 2) must produce the same numbers; this is its executable specification.

    python -m reference.pipeline out/synthetic
"""
from __future__ import annotations

import csv
import gzip
import json
import re
import sys
from collections import defaultdict
from datetime import date
from decimal import Decimal
from pathlib import Path

from contracts.migrate import to_canonical
from contracts.validate import check_record

UNIT = Decimal(10) ** 8


def _read_csv(path: Path) -> list[dict]:
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def _arrival(path: Path) -> date:
    return date.fromisoformat(re.search(r"dt=(\d{4}-\d{2}-\d{2})", str(path)).group(1))


def _open(path: Path):
    return gzip.open(path, "rt") if path.suffix == ".gz" else open(path)


def run(root: Path) -> dict:
    dims = root / "dimensions"
    models = {r["model_id"] for r in _read_csv(dims / "models.csv")}
    apps = _read_csv(dims / "apps.csv")
    app_dept = {r["app_id"]: r["department_id"] for r in apps}
    app_rtype = {r["app_id"]: r["default_request_type"] for r in apps}
    prices: dict[str, list[tuple[date, int, int]]] = defaultdict(list)
    for r in _read_csv(dims / "model_prices.csv"):
        prices[r["model_id"]].append((
            date.fromisoformat(r["valid_from"]),
            int(Decimal(r["price_in_eur_per_mtok"]) * 100),
            int(Decimal(r["price_out_eur_per_mtok"]) * 100),
        ))
    for rows in prices.values():
        rows.sort()

    def price(model: str, day: date) -> tuple[int, int]:
        best = None
        for valid_from, pin, pout in prices[model]:
            if valid_from <= day:
                best = (pin, pout)
        return best

    seen: set[str] = set()
    invalid: dict[str, int] = defaultdict(int)
    dup = lines = 0
    cost_dept: dict = defaultdict(int)
    cost_model: dict = defaultdict(int)
    cache_units: dict = defaultdict(int)
    slo_total: dict = defaultdict(int)
    slo_failed: dict = defaultdict(int)
    by_version = {1: 0, 2: 0}

    files = sorted((root / "landing" / "inference_events").rglob("*.jsonl*"), key=lambda p: _arrival(p))
    for path in files:  # arrival order -> first copy wins
        arrival = _arrival(path)
        with _open(path) as f:
            for line in f:
                lines += 1
                rec = json.loads(line)
                if rec.get("schema_version") in by_version:
                    by_version[rec["schema_version"]] += 1
                reason = check_record(rec, arrival, models, app_dept)
                if reason:
                    invalid[reason] += 1
                    continue
                if rec["event_id"] in seen:
                    dup += 1
                    continue
                seen.add(rec["event_id"])
                ev = to_canonical(rec, app_rtype)
                day = date.fromisoformat(ev["event_ts"][:10])
                month = ev["event_ts"][:7]
                pin, pout = price(ev["model_id"], day)
                units = ev["tokens_in"] * pin + ev["tokens_out"] * pout
                slo_total[(ev["app_id"], month)] += 1
                if ev["status"] != "ok":
                    slo_failed[(ev["app_id"], month)] += 1
                    continue
                if ev["cache_status"] == "hit":
                    cache_units[month] += units
                else:
                    cost_dept[(ev["department_id"], month)] += units
                    cost_model[(ev["model_id"], month)] += units

    def eur(u: int) -> str:
        return f"{Decimal(u) / UNIT:.8f}"

    months = sorted({m for (_, m) in cost_dept})
    return {
        "dq": {
            "lines_read": lines,
            "lines_by_schema_version": {"v1": by_version[1], "v2": by_version[2]},
            "unique_valid_events": len(seen),
            "duplicate_copies": dup,
            "invalid_by_code": dict(sorted(invalid.items())),
        },
        "cost_by_department_month_eur": {
            d: {m: eur(cost_dept[(d, m)]) for m in months} for d in sorted({k[0] for k in cost_dept})
        },
        "cost_by_model_month_eur": {
            md: {m: eur(cost_model[(md, m)]) for m in months} for md in sorted({k[0] for k in cost_model})
        },
        "cache_savings_by_month_eur": {m: eur(cache_units[m]) for m in months},
        "slo_by_app_month": {
            a: {
                m: {"total": slo_total[(a, m)], "failed": slo_failed[(a, m)]}
                for m in months
            }
            for a in sorted({k[0] for k in slo_total})
        },
    }


if __name__ == "__main__":
    print(json.dumps(run(Path(sys.argv[1])), indent=2))
