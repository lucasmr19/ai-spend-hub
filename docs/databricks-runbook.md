# Databricks runbook (Week 2) — Free Edition, €0

> **Verification status.** The Spark logic behind these notebooks (validation, dedupe, watermark, pricing,
> Gold SQL, reconciliation, gate, run log) is tested locally against `ground_truth.json`. The notebooks
> themselves — Auto Loader, Delta `MERGE`, Unity Catalog volumes, serverless — have **not yet been run on a
> real Free Edition workspace**. Section 7 lists what to report back if a step behaves differently.

## 1. What runs where

| Step | Notebook | Does | Idempotent because |
|---|---|---|---|
| 0 | `00_setup` | Creates schema + volume `landing`, checks the uploaded files | `IF NOT EXISTS` |
| 1 | `01_bronze` | Auto Loader (`availableNow`) → `bronze_inference_events` (raw line, file, arrival date) | checkpoint |
| 2 | `02_silver` | Classifies each line → `silver_inference_events`, `silver_late_quarantine`, `silver_quarantine`, `silver_line_audit` | `MERGE … WHEN NOT MATCHED`, audit overwritten |
| 3 | `03_gold` | **Gate**, then `fact_inference` + 6 Gold tables | gate fails before any write; tables overwritten |
| 4 | `04_reconcile` | Checks Gold against `ground_truth.json` → `ops_reconciliation` | append-only log |
| 5 | `05_backfill_late` | Applies events later than 7 days, rebuilds Gold | `MERGE … WHEN NOT MATCHED` |

Every task also writes a row to `ops_run_log` (status, duration, rows in/out, gate results, error).
Tables live in `<catalog>.<schema>` — defaults `workspace` / `ai_spend` (check your catalog name in *Catalog*).

## 2. Get the data into the workspace

1. On your laptop (≈19 MB, ≈10 s):
   ```bash
   python -m generator --preset default --gzip --layout flat --out out/synthetic
   ```
   `flat` puts all event files in one folder with the arrival date in the file name, which makes the browser
   upload a simple multi-select. (`--layout partitioned` is the same data in `dt=…/` folders.)
2. In the workspace run `00_setup` once (it creates the volume; the final check will fail until files exist — expected).
3. *Catalog → your catalog → `ai_spend` → `landing` volume → Upload to this volume*:
   - `out/synthetic/landing/inference_events/*` → into a folder `inference_events/`
   - `out/synthetic/dimensions/*.csv` → into `dimensions/`
   - `out/synthetic/ground_truth.json` → volume root
   Alternative (unverified on Free Edition): Databricks CLI `databricks fs cp -r out/synthetic dbfs:/Volumes/<catalog>/ai_spend/landing --overwrite`.
4. Re-run `00_setup`: it must print `OK: landing area is ready`.

## 3. Get the code into the workspace

Preferred: push this repository to GitHub (public) and create a **Git folder** (*Workspace → Create → Git folder*).
The notebooks find the shared code at `../src` relative to their own folder; if your layout differs, set the `repo_src`
widget to the absolute workspace path of `databricks/src`.

## 4. Run order and expected numbers (default preset, seed 42)

Run `01 → 02 → 03 → 04 (phase = pre_backfill) → 05 → 04 (phase = complete)` on serverless compute, **one at a time**.

| After | Expect |
|---|---|
| `01_bronze` | 379,489 rows in `bronze_inference_events` |
| `02_silver` | `silver` 370,227 · `late_quarantine` 800 · `quarantine` 2,889 · duplicates 5,573 (in the audit table) |
| `03_gold` | gate G1–G3 all `ok`; 7 Gold tables written |
| `04` pre_backfill | 8 / 8 checks OK (costs equal the truth minus the 800 very-late events) |
| `05_backfill_late` | `applied` = 800 |
| `04` complete | 15 / 15 checks OK — every number equals `ground_truth.json` |

Business answers to look for in Gold: Marketing is the only department over budget (September); `contract-review`
is the only app below its SLO (August); `gold_cost_daily.cache_savings_eur` gives the cache savings.

## 5. Failure demo (AC-08, publication gate)

1. Create a poisoned file and upload it to `inference_events/`:
   ```bash
   yes 'not json at all' | head -5000 > dt=2026-10-25_bad.jsonl
   ```
2. Run `01`, `02`, `03`. Task `03_gold` **fails** with `G1 valid rate over last 7 arrival days …`; Gold keeps its previous content; `ops_run_log` shows the failure.
3. Recovery (operator procedure): remove the bad rows from Bronze and re-run Silver and Gold:
   ```sql
   DELETE FROM <catalog>.ai_spend.bronze_inference_events WHERE source_file LIKE '%_bad.jsonl';
   DELETE FROM <catalog>.ai_spend.silver_quarantine WHERE source_file LIKE '%_bad.jsonl';
   ```
   then `02_silver` and `03_gold`. (Delete the file from the volume too, otherwise Auto Loader will not re-read it but it stays as noise.)

## 6. Optional: schedule it as a Job / bundle

`databricks/databricks.yml` + `resources/pipeline_job.yml` define the same graph (`bronze → silver → gold → reconcile`
and a separate backfill job) on serverless compute. Set the workspace host, authenticate with `databricks auth login`
(never put a token in the file), then `databricks bundle validate && databricks bundle deploy`. If bundles or
serverless jobs are not available in Free Edition, create the job in the UI with the same tasks — do not pay for anything.

## 7. Free Edition guardrails and what to report back

- Serverless only, daily quota: if you see `RESOURCE_EXHAUSTED`, stop and continue after the reset. Nothing is billed.
- Run **one** notebook/job at a time. The default dataset (≈380 k lines) is deliberately small.
- Non-commercial use only; keep it a personal project.
- Please report: the exact error text of any failing cell; the catalog name; whether `cloudFiles` / `MERGE` / jobs / bundles worked.
  The most likely adjustments are paths and widget defaults, not logic.

## 8. Evidence to capture (for the portfolio)

Job/run graph or the notebook run sequence · `ops_run_log` and `ops_reconciliation` tables · Gold tables preview ·
Unity Catalog lineage of `fact_inference` · the failed gate run from section 5.
