"""Ground-truth accumulator: the exact answers the pipelines must reproduce.

All money is kept as integer units of 1e-8 EUR (tokens * centi-EUR-per-Mtok) so the
truth is exact and independent of floating point.
"""
from __future__ import annotations

import math
from collections import defaultdict
from decimal import Decimal

import numpy as np

from . import scenarios as sc

UNIT = Decimal(10) ** 8


def eur(units: int) -> str:
    return f"{Decimal(units) / UNIT:.8f}"


def _round_2sig(x: float) -> float:
    if x <= 0:
        return 0.0
    digits = -int(math.floor(math.log10(x))) + 1
    return round(x, digits)


class Truth:
    def __init__(self) -> None:
        self.cost_dept: dict = defaultdict(int)
        self.cost_model: dict = defaultdict(int)
        self.cost_app: dict = defaultdict(int)
        self.cache_units: dict = defaultdict(int)
        self.cache_hits: dict = defaultdict(int)
        self.requests: dict = defaultdict(int)
        self.slo_total: dict = defaultdict(int)
        self.slo_failed: dict = defaultdict(int)
        self.inv_n: dict = defaultdict(int)
        self.inv_cost: dict = defaultdict(int)
        self.inv_lat: dict = defaultdict(list)
        self.very_late_cost: dict = defaultdict(int)
        self.very_late_events = 0
        self.late_events = 0
        self.unique_valid = 0
        self.duplicate_copies = 0
        self.invalid: dict = defaultdict(int)
        self.lines = {1: 0, 2: 0}
        self.files = 0

    # -- accumulation -------------------------------------------------------------------------
    def add(self, *, month: str, dept: str, app: str, model: str, status: str, cache: bool,
            tin: int, tout: int, lat: int, pin: int, pout: int, lag: int) -> None:
        self.unique_valid += 1
        self.requests[(model, month)] += 1
        self.slo_total[(app, month)] += 1
        if status != "ok":
            self.slo_failed[(app, month)] += 1
        units = tin * pin + tout * pout
        if lag > sc.WATERMARK_DAYS:
            self.very_late_events += 1
        elif lag >= 1:
            self.late_events += 1
        if status == "ok" and cache:
            self.cache_units[month] += units
            self.cache_hits[month] += 1
        elif status == "ok":
            self.cost_dept[(dept, month)] += units
            self.cost_model[(model, month)] += units
            self.cost_app[(app, month)] += units
            self.inv_n[model] += 1
            self.inv_cost[model] += units
            self.inv_lat[model].append(lat)
            if lag > sc.WATERMARK_DAYS:
                self.very_late_cost[(dept, month)] += units

    # -- output ---------------------------------------------------------------------------------
    def budgets(self) -> dict[str, float]:
        out = {}
        for (dept, month), u in self.cost_dept.items():
            if month == "2026-07":
                out[dept] = _round_2sig(float(Decimal(u) / UNIT) * sc.BUDGET_HEADROOM)
        return out

    def finalize(self, meta: dict, apps: list, depts: list[str]) -> dict:
        budgets = self.budgets()
        months = sorted({m for (_, m) in self.cost_dept})
        cost_by_dept = {d: {m: eur(self.cost_dept[(d, m)]) for m in months} for d in depts}
        status = {
            d: {
                m: {
                    "cost_eur": eur(self.cost_dept[(d, m)]),
                    "budget_eur": f"{budgets.get(d, 0):.2f}",
                    "over_budget": float(Decimal(self.cost_dept[(d, m)]) / UNIT) > budgets.get(d, 0),
                }
                for m in months
            }
            for d in depts
        }
        models = sorted(self.inv_n)
        model_perf = {}
        for m in models:
            lat = np.array(self.inv_lat[m])
            model_perf[m] = {
                "invocations": self.inv_n[m],
                "cost_eur": eur(self.inv_cost[m]),
                "cost_per_1k_invocations_eur": f"{Decimal(self.inv_cost[m]) / UNIT / self.inv_n[m] * 1000:.6f}",
                "latency_p50_ms": float(np.percentile(lat, 50)),
                "latency_p95_ms": float(np.percentile(lat, 95)),
            }
        slo = {}
        for a in apps:
            slo[a.app_id] = {}
            for m in months:
                tot, fail = self.slo_total[(a.app_id, m)], self.slo_failed[(a.app_id, m)]
                avail = 100.0 * (1 - fail / tot) if tot else None
                slo[a.app_id][m] = {
                    "total": tot,
                    "failed": fail,
                    "availability_pct": round(avail, 4) if avail is not None else None,
                    "target_pct": a.slo_target,
                    "breached": bool(avail is not None and avail < a.slo_target),
                }
        cache = {
            m: {"hits": self.cache_hits[m], "savings_eur": eur(self.cache_units[m])} for m in months
        }
        return {
            "meta": meta,
            "dq": {
                "lines_emitted": self.lines[1] + self.lines[2],
                "lines_by_schema_version": {"v1": self.lines[1], "v2": self.lines[2]},
                "unique_valid_events": self.unique_valid,
                "duplicate_copies": self.duplicate_copies,
                "invalid_by_code": dict(sorted(self.invalid.items())),
                "late_events_1_to_7d": self.late_events,
                "very_late_events_gt_7d": self.very_late_events,
            },
            "cost_by_department_month_eur": cost_by_dept,
            "budget_status_by_department_month": status,
            "cost_by_model_month_eur": {
                m: {mo: eur(self.cost_model[(m, mo)]) for mo in months} for m in models
            },
            "cost_by_app_month_eur": {
                a.app_id: {mo: eur(self.cost_app[(a.app_id, mo)]) for mo in months} for a in apps
            },
            "model_performance_overall": model_perf,
            "cache_by_month": cache,
            "slo_by_app_month": slo,
            "very_late_cost_by_department_month_eur": {
                d: {m: eur(self.very_late_cost[(d, m)]) for m in months} for d in depts
            },
            "budgets_eur_per_month": budgets,
        }
