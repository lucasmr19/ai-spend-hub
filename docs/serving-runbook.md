# Serving runbook (Week 3) — Gold → PostgreSQL → dbt marts, €0

Verified in the authoring sandbox on PostgreSQL 16.15 (native), DuckDB 2.0, dbt-core 1.12 with the small preset.
Not verified: `docker-compose.yml` (no Docker daemon in the sandbox), GitHub Actions run of the new CI job.

## 1. Produce the export
**A. From Databricks (the real path).** After `05_backfill_late` and `04_reconcile (complete)`, run notebook `06_export_gold`.
It writes 13 Parquet files + `manifest.json` to `/Volumes/<catalog>/ai_spend/landing/export`. Download the folder to
`out/export/` (Catalog → volume → `export`, or `databricks fs cp -r dbfs:/Volumes/.../export out/export`; the CLI path is unverified).
**B. Locally (no workspace).** `make data && make export` uses the same Spark code on a local session and writes `out/export/`.

## 2. DuckDB path (no server) — quickest demo
```bash
pip install -e ".[dev,spark,serving]"
make serving-demo     # loads out/export into out/ai.duckdb and runs `dbt build` (27 models/tests)
```
## 3. PostgreSQL path (governed)
```bash
docker compose up -d            # or any PostgreSQL 16 you own; only dev credentials in the file
export SERVING_DSN=postgresql://ai_admin:local-dev-only@localhost:5432/ai_spend
python -m serving.loader out/export                 # contract + manifest checked, one transaction
python -m serving.bootstrap roles --dev             # roles, schemas, one role per department
export PII_SALT=<long random string>  PGPASSWORD=local-dev-only DBT_TARGET=postgres PGUSER=ai_dbt
cp dbt/profiles.yml.example dbt/profiles.yml && (cd dbt && dbt build --profiles-dir .)
python -m serving.bootstrap views                   # serving.* views (after dbt, which recreates tables)
```
Outside a local demo drop `--dev` and set `AI_DBT_PASSWORD`, `AI_ANALYST_PASSWORD`, `AI_FINOPS_PASSWORD`,
`AI_STEWARD_PASSWORD`, `AI_DEPT_HEAD_PASSWORD`; never commit them.

## 4. Who can see what
| Role | raw | marts tables | `serving.*` views | user_id in clear |
|---|---|---|---|---|
| ai_loader/admin | write | — | — | yes |
| ai_dbt | read | owner | — | reads raw, publishes only `user_hash` |
| ai_analyst | no | no | all departments | no |
| ai_finops | no | no | all departments (chargeback) | no |
| ai_steward | read | no | all + `inference_with_user` | **yes** |
| ai_dept_<name> | no | no | own department only | no |
Limits: analyst 30 s / 5 connections, finops 30 s / 3, department heads 15 s.

## 5. Failure drills
1. **Contract drift:** change a column type in the manifest → `LOAD REFUSED: … type … != contract`, database untouched.
2. **Wrong totals:** edit a sum in the manifest → load aborts on `control sum`, previous data still queryable (tested).
3. **Re-run:** loading the same export twice gives identical results (TRUNCATE + COPY in one transaction).
4. **Salt rotation:** change `PII_SALT`, re-run dbt: every `user_hash` changes; joins across old and new snapshots break — announce it.

## 6. Troubleshooting
- `DependentObjectsStillExist` on load: someone changed a raw column. Migrate deliberately; the loader never uses DROP CASCADE.
- dbt `permission denied for schema raw`: run `bootstrap roles` again after the loader created the tables.
- Analyst sees "permission denied" on `marts.*`: intended; query `serving.*`.

## 7. Evidence to capture
`dbt build` output (27 PASS), `dbt docs generate` lineage graph, `\dp serving.*` output, a denied `SELECT` as `ai_analyst`,
a department head seeing only their rows, the marts reconciled against `ground_truth.json` (`pytest tests/test_serving.py -v`).
