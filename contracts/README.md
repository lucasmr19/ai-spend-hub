# Data contracts — `inference_event`

Producer: AI gateway · Consumers: Databricks Bronze/Silver · Format: JSON Lines, one event per line.

| Version | File | Change |
|---|---|---|
| v1 | `inference_event.v1.schema.json` | Initial |
| v2 | `inference_event.v2.schema.json` | **MAJOR**: `latency_ms` → `latency_total_ms`; `cache_hit` (bool) → `cache_status` (hit/miss/bypass); new required `request_type`; new optional `ttft_ms` |

## Rules

1. **Backward compatible (MINOR):** add an optional field, widen an enum, relax a constraint.
2. **Breaking (MAJOR):** remove/rename a field, add a required field, change a type, narrow an enum.
   Requires a new schema file **and** a migration file in `migrations/` that maps every removed and every new
   required field. CI runs `python -m contracts.check_compat OLD NEW --migration FILE`.
3. Producers may migrate app by app; **consumers accept all live versions** and normalise to the canonical
   Silver schema (`contracts/migrate.py`).
4. Schemas use `additionalProperties: false`: an unexpected field (for example a leaked email) is rejected
   and quarantined — data minimisation is enforced at the boundary.
5. The contract carries **no prompt/response content and no direct identifiers**; `user_id` is a pseudonymous key.

## Validation reason codes (`validate.py`)

| Code | Meaning |
|---|---|
| E000 | other schema violation (including unknown fields) |
| E001 | missing `event_id` |
| E002 | negative token count |
| E003 | token count not an integer |
| E004 | missing latency |
| E005 | unsupported `schema_version` |
| E101 | unknown `model_id` |
| E102 | `app_id` does not belong to `department_id` |
| E103 | event date later than landing (arrival) date |

`validate.py` and `reference/pipeline.py` are the executable specification the Spark pipeline must match.
