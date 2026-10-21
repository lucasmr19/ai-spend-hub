"""Generate the synthetic dataset: landing files, dimensions, ground truth.

    python -m generator.generate --preset default --out out/synthetic

Output layout::

    out/synthetic/
      landing/inference_events/dt=YYYY-MM-DD/events-000.jsonl[.gz]   (dt = ARRIVAL date)
      (--layout flat: landing/inference_events/dt=YYYY-MM-DD_events-000.jsonl[.gz])
      dimensions/*.csv
      ground_truth.json
      manifest.json
"""
from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import uuid
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

import numpy as np

from . import dimensions as dm
from . import scenarios as sc
from .truth import Truth

_HOUR_W = np.array([0.02] * 6 + [0.1, 0.4, 0.9, 1.2, 1.3, 1.3, 1.0, 1.0, 1.2, 1.2, 1.1, 0.8,
                                 0.4, 0.2, 0.1, 0.06, 0.04, 0.03])
HOUR_P = _HOUR_W / _HOUR_W.sum()
_ZIPF = 1.0 / np.arange(1, dm.USERS_PER_DEPT + 1)
USER_P = _ZIPF / _ZIPF.sum()
ERR_CODES = ["upstream_5xx", "rate_limited", "context_length"]
DEPT_IDS = [d for d, _ in dm.DEPARTMENTS]
INVALID_KINDS = ["E001", "E002", "E003", "E004", "E101", "E102", "E103"]


def _ts(day: date, ms_in_hour: int, hour: int) -> str:
    m, s, ms = ms_in_hour // 60000, (ms_in_hour // 1000) % 60, ms_in_hour % 1000
    return f"{day.isoformat()}T{hour:02d}:{m:02d}:{s:02d}.{ms:03d}Z"


def _render(ev: dict, version: int) -> dict:
    base = {
        "schema_version": version,
        "event_id": ev["event_id"],
        "event_ts": ev["event_ts"],
        "app_id": ev["app_id"],
        "department_id": ev["department_id"],
        "model_id": ev["model_id"],
        "user_id": ev["user_id"],
    }
    if version == 1:
        base.update(
            tokens_in=ev["tokens_in"], tokens_out=ev["tokens_out"], latency_ms=ev["latency"],
            cache_hit=ev["cache"], status=ev["status"], error_code=ev["error_code"],
        )
    else:
        base.update(
            request_type=ev["request_type"], tokens_in=ev["tokens_in"], tokens_out=ev["tokens_out"],
            latency_total_ms=ev["latency"], ttft_ms=ev["ttft"],
            cache_status=ev["cache_status"], status=ev["status"], error_code=ev["error_code"],
        )
    return base


def _corrupt(rec: dict, code: str, day: date) -> dict:
    rec = dict(rec)
    if code == "E001":
        rec.pop("event_id")
    elif code == "E002":
        rec["tokens_in"] = -rec["tokens_in"]
    elif code == "E003":
        rec["tokens_out"] = str(rec["tokens_out"])
    elif code == "E004":
        rec.pop("latency_ms" if rec["schema_version"] == 1 else "latency_total_ms")
    elif code == "E101":
        rec["model_id"] = "aria-xl"
    elif code == "E102":
        i = DEPT_IDS.index(rec["department_id"])
        rec["department_id"] = DEPT_IDS[(i + 1) % len(DEPT_IDS)]
    elif code == "E103":
        rec["event_ts"] = (day + timedelta(days=3)).isoformat() + rec["event_ts"][10:]
    return rec


def _dump(rec: dict) -> str:
    return json.dumps(rec, separators=(",", ":"))


def run(out: Path, *, events_per_day: int, seed: int = 42, gzip_files: bool = False,
        start: date = sc.PERIOD_START, end: date = sc.PERIOD_END, preset: str = "custom",
        layout: str = "partitioned") -> dict:
    rng = np.random.default_rng(seed)
    out.mkdir(parents=True, exist_ok=True)
    dm.write_static(out, seed)
    truth = Truth()
    pending: dict[date, list[str]] = defaultdict(list)
    price_cache: dict[tuple[str, date], tuple[int, int]] = {}
    pools = {d: dm.user_pool(d) for d in DEPT_IDS}
    land = out / "landing" / "inference_events"
    manifest_files: list[dict] = []

    def price(model: str, day: date) -> tuple[int, int]:
        k = (model, day)
        if k not in price_cache:
            price_cache[k] = dm.price_on(model, day)
        return price_cache[k]

    def emit_app_day(app: dm.App, day: date, day_idx: int) -> None:
        factor = 1.0 if day.weekday() < 5 else sc.WEEKEND_FACTOR
        factor *= 1 + sc.DAILY_GROWTH * day_idx
        vol_mult = tok_mult = 1.0
        if app.app_id == sc.RUNAWAY["app_id"] and day in sc.RUNAWAY["days"]:
            vol_mult, tok_mult = sc.RUNAWAY["volume_mult"], sc.RUNAWAY["tokens_mult"]
        n = int(rng.poisson(events_per_day * app.share * factor * vol_mult))
        if n == 0:
            return
        hours = rng.choice(24, size=n, p=HOUR_P)
        ms = rng.integers(0, 3_600_000, size=n)
        mix_models = list(app.model_mix)
        models = rng.choice(mix_models, size=n, p=[app.model_mix[m] for m in mix_models])
        tin = np.maximum(1, rng.lognormal(app.tin_mu, app.tin_sigma, n) * tok_mult).astype(np.int64)
        tout = np.maximum(1, rng.lognormal(app.tout_mu, app.tout_sigma, n) * tok_mult).astype(np.int64)
        cache = (rng.random(n) < app.cache_rate) if app.cache_enabled else np.zeros(n, dtype=bool)
        err_p = np.full(n, app.base_error)
        o = sc.OUTAGE
        if app.app_id == o["app_id"] and day == o["day"]:
            err_p[(hours >= o["start_h"]) & (hours < o["end_h"])] = o["error_rate"]
        failed = (rng.random(n) < err_p) & ~cache
        timeout = failed & (rng.random(n) < 0.5)
        base_ms = np.array([dm.MODELS[m][1] for m in models], dtype=float)
        per_tok = np.array([dm.MODELS[m][2] for m in models], dtype=float)
        lat = base_ms * rng.lognormal(0, 0.35, n) + tout * per_tok * rng.lognormal(0, 0.2, n)
        lat = np.where(cache, 20 + rng.gamma(2, 10, n), lat)
        lat = np.where(failed, rng.uniform(100, 1500, n), lat)
        lat = np.where(timeout, 30_000, lat).astype(np.int64)
        ttft_frac = rng.uniform(0.15, 0.35, n)
        users = rng.choice(pools[app.department_id], size=n, p=USER_P)
        raw_ids = rng.bytes(16 * n)
        err_pick = rng.integers(0, len(ERR_CODES), n)
        u_lag, late_d, vlate_d = rng.random(n), rng.integers(1, 6, n), rng.integers(8, 21, n)
        u_inv, inv_kind = rng.random(n), rng.integers(0, len(INVALID_KINDS), n)
        u_dup, dup_mode = rng.random(n), rng.random(n)

        v2 = app.v2_from is not None and day >= app.v2_from
        version = 2 if v2 else 1
        month = day.isoformat()[:7]
        b = sc.COLLECTOR_BURST

        for i in range(n):
            h = int(hours[i])
            if day == b["day"] and b["start_h"] <= h < b["end_h"]:
                lag = b["lag_days"]
            elif u_lag[i] < sc.P_VERY_LATE:
                lag = int(vlate_d[i])
            elif u_lag[i] < sc.P_VERY_LATE + sc.P_LATE:
                lag = int(late_d[i])
            else:
                lag = 0

            is_fail, is_to = bool(failed[i]), bool(timeout[i])
            status = "timeout" if is_to else "error" if is_fail else "ok"
            is_cache = bool(cache[i])
            ev = {
                "event_id": str(uuid.UUID(bytes=raw_ids[16 * i:16 * i + 16], version=4)),
                "event_ts": _ts(day, int(ms[i]), h),
                "app_id": app.app_id,
                "department_id": app.department_id,
                "model_id": str(models[i]),
                "user_id": str(users[i]),
                "request_type": app.request_type,
                "tokens_in": int(tin[i]),
                "tokens_out": 0 if is_fail else int(tout[i]),
                "latency": int(lat[i]),
                "ttft": None if is_fail else int(lat[i] if is_cache else lat[i] * ttft_frac[i]),
                "cache": is_cache,
                "cache_status": "hit" if is_cache else ("miss" if app.cache_enabled else "bypass"),
                "status": status,
                "error_code": ("gateway_timeout" if is_to else ERR_CODES[int(err_pick[i])]) if is_fail else None,
            }
            rec = _render(ev, version)

            if lag == 0 and u_inv[i] < sc.P_INVALID:
                code = INVALID_KINDS[int(inv_kind[i])]
                truth.invalid[code] += 1
                truth.lines[version] += 1
                pending[day].append(_dump(_corrupt(rec, code, day)))
                continue

            pin, pout = price(ev["model_id"], day)
            truth.add(month=month, dept=app.department_id, app=app.app_id, model=ev["model_id"],
                      status=status, cache=is_cache, tin=ev["tokens_in"], tout=ev["tokens_out"],
                      lat=ev["latency"], pin=pin, pout=pout, lag=lag)
            line = _dump(rec)
            arrival = day + timedelta(days=lag)
            pending[arrival].append(line)
            truth.lines[version] += 1
            if u_dup[i] < sc.P_DUPLICATE:
                truth.duplicate_copies += 1
                truth.lines[version] += 1
                pending[arrival if dup_mode[i] < 0.6 else arrival + timedelta(days=1)].append(line)

    def flush(d: date) -> None:
        lines = pending.pop(d, None)
        if not lines:
            return
        order = rng.permutation(len(lines))
        ext = ".jsonl.gz" if gzip_files else ".jsonl"
        if layout == "flat":  # one directory, arrival date in the file name (easy multi-file UI upload)
            ddir, name = land, f"dt={d.isoformat()}_events-000{ext}"
        else:
            ddir, name = land / f"dt={d.isoformat()}", f"events-000{ext}"
        ddir.mkdir(parents=True, exist_ok=True)
        payload = ("\n".join(lines[j] for j in order) + "\n").encode()
        path = ddir / name
        if gzip_files:
            with gzip.GzipFile(path, "wb", mtime=0) as f:  # mtime=0 -> reproducible bytes
                f.write(payload)
        else:
            path.write_bytes(payload)
        truth.files += 1
        manifest_files.append({"path": str(path.relative_to(out)), "lines": len(lines),
                               "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})

    d, last = start, end + timedelta(days=sc.MAX_LAG_DAYS + 1)
    while d <= last:
        if d <= end:
            for app in dm.APPS:
                emit_app_day(app, d, (d - start).days)
        flush(d)
        d += timedelta(days=1)

    meta = {"seed": seed, "preset": preset, "events_per_weekday": events_per_day,
            "period": [start.isoformat(), end.isoformat()], "gzip": gzip_files,
            "currency": "EUR", "money_precision": "8 decimals (exact integer arithmetic)",
            "fictional": True}
    gt = truth.finalize(meta, dm.APPS, DEPT_IDS)

    # budgets dimension (needs the July baseline)
    months = sorted(next(iter(gt["cost_by_department_month_eur"].values())))
    with open(out / "dimensions" / "budgets.csv", "w", newline="") as f:
        cw = csv.writer(f)
        cw.writerow(["department_id", "month", "budget_eur"])
        for dept, b in gt["budgets_eur_per_month"].items():
            for m in months:
                cw.writerow([dept, m, f"{b:.2f}"])

    (out / "ground_truth.json").write_text(json.dumps(gt, indent=2))
    (out / "manifest.json").write_text(json.dumps(
        {"meta": meta, "files": manifest_files, "file_count": len(manifest_files)}, indent=2))
    return gt


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--preset", choices=list(sc.PRESETS), default="default")
    ap.add_argument("--events-per-day", type=int, help="override the preset")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", type=Path, default=Path("out/synthetic"))
    ap.add_argument("--gzip", action="store_true", help="write .jsonl.gz files")
    ap.add_argument("--layout", choices=["partitioned", "flat"], default="partitioned",
                    help="partitioned: dt=YYYY-MM-DD/events-000.jsonl; flat: dt=YYYY-MM-DD_events-000.jsonl in one folder")
    args = ap.parse_args(argv)
    epd = args.events_per_day or sc.PRESETS[args.preset]
    gt = run(args.out, events_per_day=epd, seed=args.seed, gzip_files=args.gzip, preset=args.preset,
             layout=args.layout)
    dq = gt["dq"]
    print(f"wrote {args.out}: {dq['lines_emitted']:,} lines, {dq['unique_valid_events']:,} unique valid events")
    print(f"  duplicates={dq['duplicate_copies']:,}  invalid={sum(dq['invalid_by_code'].values()):,}  "
          f"late={dq['late_events_1_to_7d']:,}  very_late={dq['very_late_events_gt_7d']:,}")


if __name__ == "__main__":
    main()
