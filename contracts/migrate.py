"""Normalise v1/v2 events into the canonical Silver shape (v2 + source_schema_version)."""
from __future__ import annotations


def to_canonical(rec: dict, app_default_request_type: dict[str, str]) -> dict:
    """Return the canonical event. Assumes ``rec`` already passed validation."""
    version = rec["schema_version"]
    if version == 2:
        return {
            "event_id": rec["event_id"],
            "event_ts": rec["event_ts"],
            "app_id": rec["app_id"],
            "department_id": rec["department_id"],
            "model_id": rec["model_id"],
            "user_id": rec["user_id"],
            "request_type": rec["request_type"],
            "tokens_in": rec["tokens_in"],
            "tokens_out": rec["tokens_out"],
            "latency_total_ms": rec["latency_total_ms"],
            "ttft_ms": rec.get("ttft_ms"),
            "cache_status": rec["cache_status"],
            "status": rec["status"],
            "error_code": rec.get("error_code"),
            "source_schema_version": 2,
        }
    return {
        "event_id": rec["event_id"],
        "event_ts": rec["event_ts"],
        "app_id": rec["app_id"],
        "department_id": rec["department_id"],
        "model_id": rec["model_id"],
        "user_id": rec["user_id"],
        "request_type": app_default_request_type[rec["app_id"]],
        "tokens_in": rec["tokens_in"],
        "tokens_out": rec["tokens_out"],
        "latency_total_ms": rec["latency_ms"],
        "ttft_ms": None,
        "cache_status": "hit" if rec["cache_hit"] else "miss",
        "status": rec["status"],
        "error_code": rec.get("error_code"),
        "source_schema_version": 1,
    }
