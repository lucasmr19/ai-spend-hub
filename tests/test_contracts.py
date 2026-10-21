import copy
import json
from datetime import date
from pathlib import Path

from jsonschema import Draft202012Validator

from contracts import check_compat as cc
from contracts.migrate import to_canonical
from contracts.validate import check_record, load_schema

ROOT = Path(__file__).resolve().parents[1] / "contracts"
V1, V2 = load_schema(1), load_schema(2)
MIG = json.loads((ROOT / "migrations" / "v1_to_v2.json").read_text())
MODELS = {"aria-s", "aria-m"}
APPS = {"faq-bot": "customer-support"}
ARRIVAL = date(2026, 8, 1)

EV1 = {
    "schema_version": 1, "event_id": "123e4567-e89b-42d3-a456-426614174000",
    "event_ts": "2026-08-01T10:00:00.000Z", "app_id": "faq-bot",
    "department_id": "customer-support", "model_id": "aria-s", "user_id": "u-0000abcd",
    "tokens_in": 10, "tokens_out": 5, "latency_ms": 120, "cache_hit": False,
    "status": "ok", "error_code": None,
}


def _check(rec):
    return check_record(rec, ARRIVAL, MODELS, APPS)


def test_schemas_are_valid_json_schema():
    Draft202012Validator.check_schema(V1)
    Draft202012Validator.check_schema(V2)


def test_v1_to_v2_is_breaking_but_covered_by_migration():
    changes = cc.diff_schemas(V1, V2)
    assert any(c.kind == "breaking" for c in changes)
    assert cc.unmapped(changes, MIG) == []
    assert cc.main([str(ROOT / "inference_event.v1.schema.json"),
                    str(ROOT / "inference_event.v2.schema.json")]) == 1
    assert cc.main([str(ROOT / "inference_event.v1.schema.json"),
                    str(ROOT / "inference_event.v2.schema.json"),
                    "--migration", str(ROOT / "migrations" / "v1_to_v2.json")]) == 0


def test_adding_optional_field_is_compatible():
    new = copy.deepcopy(V1)
    new["properties"]["region"] = {"type": "string"}
    changes = cc.diff_schemas(V1, new)
    assert [c.kind for c in changes] == ["compatible"]


def test_new_required_field_and_type_change_are_breaking():
    new = copy.deepcopy(V1)
    new["properties"]["region"] = {"type": "string"}
    new["required"].append("region")
    new["properties"]["tokens_in"] = {"type": "string"}
    kinds = {(c.field, c.kind) for c in cc.diff_schemas(V1, new)}
    assert ("region", "breaking") in kinds and ("tokens_in", "breaking") in kinds


def test_unmapped_breaking_change_is_reported():
    new = copy.deepcopy(V1)
    del new["properties"]["user_id"]
    new["required"].remove("user_id")
    assert [c.field for c in cc.unmapped(cc.diff_schemas(V1, new), MIG)] == ["user_id"]


def test_valid_record_passes():
    assert _check(EV1) is None


def test_each_reason_code():
    def mut(**kw):
        r = dict(EV1)
        r.update(kw)
        return r

    no_id = {k: v for k, v in EV1.items() if k != "event_id"}
    no_lat = {k: v for k, v in EV1.items() if k != "latency_ms"}
    assert _check(no_id) == "E001"
    assert _check(mut(tokens_in=-1)) == "E002"
    assert _check(mut(tokens_out="5")) == "E003"
    assert _check(no_lat) == "E004"
    assert _check(mut(schema_version=3)) == "E005"
    assert _check(mut(model_id="aria-xl")) == "E101"
    assert _check(mut(department_id="legal")) == "E102"
    assert _check(mut(event_ts="2026-08-04T10:00:00.000Z")) == "E103"
    assert _check(mut(pii_email="a@b.c")) == "E000"  # unknown fields are rejected


def test_v1_to_canonical_migration_matches_v2_shape():
    canon = to_canonical(EV1, {"faq-bot": "qa_rag"})
    assert canon["latency_total_ms"] == 120
    assert canon["cache_status"] == "miss"
    assert canon["request_type"] == "qa_rag"
    assert canon["ttft_ms"] is None and canon["source_schema_version"] == 1
    as_v2 = {k: v for k, v in canon.items() if k != "source_schema_version"}
    as_v2["schema_version"] = 2
    assert Draft202012Validator(V2).is_valid(as_v2)
