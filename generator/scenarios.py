"""Deliberate failure / business scenarios injected into the synthetic stream.

Every scenario is deterministic and documented so that pipelines, tests and the
dashboard can be checked against known answers (see ground_truth.json).
"""
from __future__ import annotations

from datetime import date

PERIOD_START = date(2026, 7, 1)
PERIOD_END = date(2026, 9, 30)

# --- data-quality scenarios ----------------------------------------------------------------
P_DUPLICATE = 0.015        # share of valid events emitted twice (60% same batch, 40% replayed next day)
P_LATE = 0.030             # arrive 1..5 days late
P_VERY_LATE = 0.002        # arrive 8..20 days late (beyond the 7-day watermark)
P_INVALID = 0.008          # corrupted at the source (only among on-time, non-duplicated events)
WATERMARK_DAYS = 7
MAX_LAG_DAYS = 21          # 20 days very late + 1 day replay

# Collector outage: events of this window are all delivered the next day in one burst.
COLLECTOR_BURST = {"day": date(2026, 8, 12), "start_h": 9, "end_h": 15, "lag_days": 1}

# --- business scenarios ------------------------------------------------------------------------
# Legal / contract-review: upstream incident -> app breaches its monthly availability SLO.
OUTAGE = {"app_id": "contract-review", "day": date(2026, 8, 27), "start_h": 13, "end_h": 19,
          "error_rate": 0.45}

# Marketing / campaign-writer: runaway agent loop -> budget overrun.
RUNAWAY = {"app_id": "campaign-writer",
           "days": {date(2026, 9, 9), date(2026, 9, 10), date(2026, 9, 11)},
           "volume_mult": 5.0, "tokens_mult": 4.0}

# Budgets: approved monthly budget = July actual (clean baseline month) + this headroom.
BUDGET_HEADROOM = 1.15
DAILY_GROWTH = 0.0005      # volume grows 0.05% per day
WEEKEND_FACTOR = 0.25

PRESETS = {"small": 800, "default": 5000, "large": 25000}  # weekday events/day
