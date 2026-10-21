"""Static dimensions of the fictional client (Halcyon Retail Group, fictional)."""
from __future__ import annotations

import csv
from bisect import bisect_right
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import numpy as np

DEPARTMENTS = [
    ("customer-support", "Customer Support"),
    ("legal", "Legal & Compliance"),
    ("hr", "People & HR"),
    ("finance", "Finance"),
    ("engineering", "Engineering"),
    ("marketing", "Marketing"),
]

REQUEST_TYPES = ["qa_rag", "extraction", "drafting", "code", "summarization", "analysis"]

# model_id -> (family, base latency ms, extra ms per output token)
MODELS = {
    "aria-s": ("aria", 350, 1.5),
    "aria-m": ("aria", 700, 3.0),
    "aria-l": ("aria", 1500, 6.0),
    "vega-r": ("vega", 4200, 12.0),
}

# (model_id, valid_from inclusive, valid_to exclusive | None, EUR per 1M tokens in, out)
PRICE_HISTORY = [
    ("aria-s", date(2026, 1, 1), None, 0.15, 0.60),
    ("aria-m", date(2026, 1, 1), date(2026, 8, 15), 0.80, 3.20),
    ("aria-m", date(2026, 8, 15), None, 0.60, 2.40),  # price cut mid-period
    ("aria-l", date(2026, 1, 1), None, 3.00, 15.00),
    ("vega-r", date(2026, 1, 1), None, 5.00, 25.00),
]

CACHED_FRACTION_NOTE = "cache hits are served by the gateway: provider cost is 0"


@dataclass(frozen=True)
class App:
    app_id: str
    department_id: str
    request_type: str
    share: float  # share of daily volume
    model_mix: dict
    tin_mu: float  # lognormal params for input tokens
    tin_sigma: float
    tout_mu: float
    tout_sigma: float
    cache_rate: float
    base_error: float
    slo_target: float  # monthly availability target, percent
    v2_from: date | None = None  # emitter migrates to contract v2 on this date
    cache_enabled: bool = field(default=True)


APPS = [
    App("support-copilot", "customer-support", "qa_rag", 0.20, {"aria-m": 0.7, "aria-s": 0.3},
        7.4, 0.5, 5.7, 0.5, 0.30, 0.0006, 99.9, date(2026, 8, 20)),
    App("faq-bot", "customer-support", "qa_rag", 0.18, {"aria-s": 1.0},
        6.2, 0.5, 5.0, 0.5, 0.55, 0.0008, 99.5, date(2026, 8, 22)),
    App("contract-review", "legal", "analysis", 0.04, {"aria-l": 0.8, "vega-r": 0.2},
        9.0, 0.4, 6.5, 0.4, 0.0, 0.003, 99.5, date(2026, 8, 25), cache_enabled=False),
    App("policy-qa", "legal", "qa_rag", 0.05, {"aria-m": 1.0},
        7.6, 0.5, 5.8, 0.5, 0.20, 0.002, 99.5, date(2026, 8, 27)),
    App("hr-assistant", "hr", "qa_rag", 0.06, {"aria-s": 0.8, "aria-m": 0.2},
        6.8, 0.5, 5.5, 0.5, 0.25, 0.002, 99.0, None),  # legacy: stays on v1
    App("cv-screener", "hr", "extraction", 0.03, {"aria-m": 1.0},
        7.9, 0.4, 5.0, 0.4, 0.02, 0.004, 99.0, date(2026, 9, 1)),
    App("invoice-extractor", "finance", "extraction", 0.12, {"aria-s": 1.0},
        7.0, 0.4, 5.3, 0.4, 0.10, 0.001, 99.5, None),  # legacy: stays on v1
    App("forecast-narrator", "finance", "analysis", 0.02, {"aria-l": 1.0},
        8.2, 0.4, 6.6, 0.4, 0.0, 0.003, 99.0, date(2026, 9, 3), cache_enabled=False),
    App("code-assistant", "engineering", "code", 0.17, {"aria-l": 0.55, "vega-r": 0.15, "aria-m": 0.30},
        8.0, 0.5, 6.3, 0.5, 0.05, 0.004, 99.5, date(2026, 8, 20)),
    App("incident-summarizer", "engineering", "summarization", 0.04, {"aria-m": 1.0},
        8.3, 0.4, 5.9, 0.4, 0.05, 0.002, 99.5, date(2026, 8, 21)),
    App("campaign-writer", "marketing", "drafting", 0.05, {"aria-m": 0.7, "aria-l": 0.3},
        6.8, 0.5, 6.4, 0.5, 0.03, 0.003, 99.0, date(2026, 8, 24)),
    App("seo-helper", "marketing", "drafting", 0.04, {"aria-s": 1.0},
        6.0, 0.5, 5.8, 0.5, 0.15, 0.002, 99.0, date(2026, 8, 28)),
]

APP_BY_ID = {a.app_id: a for a in APPS}
assert abs(sum(a.share for a in APPS) - 1.0) < 1e-9

USERS_PER_DEPT = 60


def user_pool(dept_id: str) -> list[str]:
    idx = [d for d, _ in DEPARTMENTS].index(dept_id)
    return [f"u-{((idx + 1) * 10_000 + i) * 2654435761 % 2**32:08x}" for i in range(USERS_PER_DEPT)]


# --- prices ---------------------------------------------------------------------------------
def _centi(x: float) -> int:
    return int(round(x * 100))


_PRICES: dict[str, list[tuple[date, int, int]]] = {}
for _m, _from, _to, _pin, _pout in PRICE_HISTORY:
    _PRICES.setdefault(_m, []).append((_from, _centi(_pin), _centi(_pout)))
for _rows in _PRICES.values():
    _rows.sort()
_PRICE_KEYS = {m: [r[0] for r in rows] for m, rows in _PRICES.items()}


def price_on(model_id: str, day: date) -> tuple[int, int]:
    """Prices in centi-EUR per 1M tokens (integers) valid on ``day``.

    cost_units = tokens * price_centi, where 1 unit = 1e-8 EUR (exact integer arithmetic).
    """
    rows = _PRICES[model_id]
    i = bisect_right(_PRICE_KEYS[model_id], day) - 1
    if i < 0:
        raise KeyError(f"no price for {model_id} on {day}")
    return rows[i][1], rows[i][2]


# --- quality evaluations (synthetic) -----------------------------------------------------------
_BASE_Q = {"aria-s": 0.70, "aria-m": 0.81, "aria-l": 0.89, "vega-r": 0.92}
_TASK_ADJ = {
    "qa_rag": {"aria-s": 0.06, "aria-m": 0.03, "aria-l": 0.0, "vega-r": 0.0},
    "extraction": {"aria-s": 0.14, "aria-m": 0.08, "aria-l": 0.02, "vega-r": 0.0},
    "drafting": {"aria-s": 0.03, "aria-m": 0.02, "aria-l": 0.0, "vega-r": -0.01},
    "code": {"aria-s": -0.14, "aria-m": -0.06, "aria-l": 0.0, "vega-r": 0.03},
    "summarization": {"aria-s": 0.05, "aria-m": 0.04, "aria-l": 0.0, "vega-r": 0.0},
    "analysis": {"aria-s": -0.12, "aria-m": -0.05, "aria-l": 0.0, "vega-r": 0.04},
}


def write_static(out: Path, seed: int) -> None:
    d = out / "dimensions"
    d.mkdir(parents=True, exist_ok=True)

    def w(name: str, header: list[str], rows: list[list]) -> None:
        with open(d / name, "w", newline="") as f:
            cw = csv.writer(f)
            cw.writerow(header)
            cw.writerows(rows)

    w("departments.csv", ["department_id", "department_name"], [list(x) for x in DEPARTMENTS])
    w("apps.csv", ["app_id", "department_id", "default_request_type", "cache_enabled"],
      [[a.app_id, a.department_id, a.request_type, str(a.cache_enabled).lower()] for a in APPS])
    w("models.csv", ["model_id", "family"], [[m, v[0]] for m, v in MODELS.items()])
    w("model_prices.csv",
      ["model_id", "valid_from", "valid_to", "price_in_eur_per_mtok", "price_out_eur_per_mtok"],
      [[m, f.isoformat(), t.isoformat() if t else "", f"{pi:.2f}", f"{po:.2f}"]
       for m, f, t, pi, po in PRICE_HISTORY])
    w("slo_targets.csv", ["app_id", "target_availability_pct"],
      [[a.app_id, f"{a.slo_target:.1f}"] for a in APPS])

    rng = np.random.default_rng(seed + 1)  # independent stream: quality table
    rows = []
    for rt in REQUEST_TYPES:
        for m in MODELS:
            score = _BASE_Q[m] + _TASK_ADJ[rt][m] + rng.normal(0, 0.008)
            rows.append([m, rt, f"{min(max(score, 0), 1):.3f}", 400, "2026-06-15"])
    w("model_quality.csv", ["model_id", "request_type", "quality_score", "eval_samples", "evaluated_on"], rows)
