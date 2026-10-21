"""Reference (oracle) record validator.

Pure-Python implementation of the Silver validation rules. The Databricks pipeline
(week 2) must reproduce exactly the same verdicts; tests compare both.

Reason codes
------------
E000 schema_invalid          any other schema violation
E001 missing_event_id        required field event_id absent
E002 negative_tokens         tokens_in / tokens_out < 0
E003 wrong_type_tokens       tokens_in / tokens_out not an integer
E004 missing_latency         latency field absent
E005 unsupported_version     schema_version not in {1, 2}
E101 unknown_model           model_id not in dimensions
E102 app_department_mismatch app_id does not belong to department_id
E103 future_timestamp        event date later than the arrival (landing) date
"""
from __future__ import annotations

import json
from datetime import date
from functools import lru_cache
from pathlib import Path

from jsonschema import Draft202012Validator

CONTRACTS_DIR = Path(__file__).parent
SUPPORTED_VERSIONS = (1, 2)

REASONS = {
    "E000": "schema_invalid",
    "E001": "missing_event_id",
    "E002": "negative_tokens",
    "E003": "wrong_type_tokens",
    "E004": "missing_latency",
    "E005": "unsupported_version",
    "E101": "unknown_model",
    "E102": "app_department_mismatch",
    "E103": "future_timestamp",
}

_LATENCY_FIELDS = {"latency_ms", "latency_total_ms"}
_TOKEN_FIELDS = {"tokens_in", "tokens_out"}


@lru_cache(maxsize=None)
def load_schema(version: int) -> dict:
    path = CONTRACTS_DIR / f"inference_event.v{version}.schema.json"
    return json.loads(path.read_text())


@lru_cache(maxsize=None)
def _validator(version: int) -> Draft202012Validator:
    return Draft202012Validator(load_schema(version))


def _schema_reason(rec: dict, version: int) -> str | None:
    errors = sorted(_validator(version).iter_errors(rec), key=lambda e: list(map(str, e.path)))
    if not errors:
        return None
    for err in errors:
        field = err.path[-1] if err.path else None
        if err.validator == "required":
            missing = err.message.split("'")[1]
            if missing == "event_id":
                return "E001"
            if missing in _LATENCY_FIELDS:
                return "E004"
        elif field in _TOKEN_FIELDS and err.validator == "minimum":
            return "E002"
        elif field in _TOKEN_FIELDS and err.validator == "type":
            return "E003"
    return "E000"


def check_record(
    rec: dict,
    arrival_date: date,
    valid_models: set[str],
    app_department: dict[str, str],
) -> str | None:
    """Return None if the record is valid, otherwise the first failing reason code."""
    version = rec.get("schema_version")
    if version not in SUPPORTED_VERSIONS:
        return "E005"
    reason = _schema_reason(rec, version)
    if reason:
        return reason
    if rec["model_id"] not in valid_models:
        return "E101"
    if app_department.get(rec["app_id"]) != rec["department_id"]:
        return "E102"
    if date.fromisoformat(rec["event_ts"][:10]) > arrival_date:
        return "E103"
    return None
