# ADR 018: dbt reads Silver by four-part Lakehouse name; Gold co-located in europe-west3

## Status

Accepted.

## Context

B1+B2 (ADR-016/017) moved Silver to Iceberg tables in the BigLake Lakehouse
catalog. dbt still read Silver through `dbt_external_tables`
(`stage_external_sources`): external tables over `gs://…/silver/…/*.parquet`
globs with autodetect. Against Iceberg those globs match nothing or pick up
Iceberg's internal files, so Gold breaks — this was ADR-016's accepted red
window.

Two facts shaped the fix:

1. **BigQuery reads a Lakehouse-catalog table live by four-part name**,
   `project.catalog.namespace.table` — no external table, no pinned metadata
   file, fresh on every query (spike 0B). A dbt source with
   `database: "<project>.<catalog>"` renders
   `` `project.catalog`.`namespace`.`table` `` and BigQuery accepts it
   (spike 0C). This is **not** "BigQuery catalog federation", which is the
   reverse direction (BigQuery tables exposed to Spark/Trino).
2. **Location mismatch.** The catalog and bucket are in `europe-west3`, but
   every dbt-created dataset (`_stg`, `_int`, `_dwh`, `_mrt`,
   `_dbt_test__audit`, `source`, `seed`) was in multi-region `EU`, because
   `.env` had `BQ_LOCATION=EU` and `profiles.yml` fell back to `'EU'`. A
   BigQuery query runs in one location, so `stg_*` could not read
   `europe-west3` Silver into `EU` datasets.

## Decision

- `sources.yaml` points both source groups at the catalog:
  `database: "{{ target.project }}.ai_compute_arbitrage_monitor_catalog"`,
  `schema: silver_sources` / `silver_seeds`. The dot inside `database` is
  deliberate.
- The project comes from `target.project` (already resolved by the profile)
  rather than a second `env_var` read. The catalog id is **hardcoded**: there
  is one project and one catalog, so it does not vary per deployment. If a
  per-environment catalog appears, switch to
  `{{ env_var('BIGLAKE_CATALOG_ID') }}` fed from Terraform's
  `lakehouse_catalog_id` output.
- Gold moves to `europe-west3`: the `EU` datasets are dropped and rebuilt by
  `dbt run --full-refresh`. `profiles.yml` reads `env_var('BQ_LOCATION')`
  with **no default** — the silent `'EU'` fallback caused the mismatch, and
  per ADR-013 a missing GCP identity value should fail loudly.
- `stage_external_sources`, the `t_ext_table` DAG task, the
  `run-operation` plumbing in `transform_strategy.py` / `transform_adapter.py`
  / `pipeline_config.py`, and the `dbt_external_tables` package are removed.
  The DAG is `ingest >> refine >> dbt_run >> dbt_test`.

## Considered options

- **Iceberg external tables pinned to a metadata JSON** — rejected: every
  commit writes a new metadata file, so a pin goes stale (spike 0B).
- **Move the catalog to `EU`** instead of Gold to `europe-west3` — rejected:
  reverses ADR-016's infrastructure and needs a new bucket plus a re-run of
  B1+B2.
- **Default `BQ_LOCATION` to `'europe-west3'`** — rejected: a second silent
  copy of the region that can drift.

## Consequences

- The Aug 24 – Sep 8 2026 Gold history is **knowingly lost**: Iceberg Silver
  starts at the first Iceberg snapshot and the legacy Parquet was not
  backfilled (test data). The legacy Parquet still exists in GCS; its
  retirement is tracked in #31.
- The catalog id now lives in three places: `infra/terraform/shared/terraform.tfvars`
  (owner), `config/settings.yaml`, `sources.yaml`.
- Correction to ADR-016: the catalog id is `ai_compute_arbitrage_monitor_catalog`
  (underscores), not `ai-compute-arbitrage-monitor-catalog`.
- `BQ_LOCATION` must now be set wherever dbt runs. The local path gets it
  from `.env`; the (undeployed) Cloud Run job sets no env vars at all —
  tracked in #30.
