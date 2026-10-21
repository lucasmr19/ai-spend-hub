"""Publication gate (FR-08 / AC-08): Gold is only refreshed when the data is trustworthy.

Thresholds are proposals (see docs/kpi-definitions.md) to be agreed with the client.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class GateFailed(Exception):
    pass


def evaluate_gate(classified: DataFrame, silver: DataFrame, *, min_valid_rate_pct: float = 97.0,
                  window_days: int = 7, max_staleness_days: int | None = None,
                  as_of=None) -> list[dict]:
    """Return the gate results. ``classified`` needs: arrival_date, disposition, reason, schema_version."""
    results: list[dict] = []

    latest = classified.agg(F.max("arrival_date")).first()[0]
    win = classified.filter(F.col("arrival_date") > F.date_sub(F.lit(latest), window_days))
    n, bad = win.count(), win.filter("disposition = 'quarantine'").count()
    rate = 100.0 * (1 - bad / n) if n else 0.0
    results.append({"id": "G1", "name": f"valid rate over last {window_days} arrival days >= {min_valid_rate_pct}%",
                    "ok": rate >= min_valid_rate_pct, "detail": f"{rate:.2f}% ({bad} invalid of {n})"})

    dups = silver.groupBy("event_id").count().filter("count > 1").count()
    results.append({"id": "G2", "name": "no duplicate event_id in Silver", "ok": dups == 0, "detail": f"{dups} duplicated"})

    unsupported = classified.filter("reason = 'E005'").count()
    results.append({"id": "G3", "name": "no unsupported schema_version", "ok": unsupported == 0,
                    "detail": f"{unsupported} lines"})

    if max_staleness_days is not None and as_of is not None:
        age = classified.agg(F.datediff(F.lit(as_of), F.max("arrival_date"))).first()[0]
        results.append({"id": "G4", "name": f"freshness <= {max_staleness_days} day(s)",
                        "ok": age <= max_staleness_days, "detail": f"{age} day(s) since last arrival"})
    return results


def enforce_gate(results: list[dict]) -> None:
    failed = [r for r in results if not r["ok"]]
    if failed:
        raise GateFailed("; ".join(f"{r['id']} {r['name']} -> {r['detail']}" for r in failed))
