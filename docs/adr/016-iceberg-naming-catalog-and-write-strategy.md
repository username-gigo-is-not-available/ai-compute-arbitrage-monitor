# ADR 016: Iceberg naming, catalog identity, cutover window, and per-source write strategy

## Status

Accepted. Amended — read these before relying on the Decision below:

- **Catalog id** is `ai_compute_arbitrage_monitor_catalog` (underscores),
  per ADR-018's correction; the hyphenated id below was never created.
- **Bronze sources** are true append (ADR-017), partitioned by
  `hour(ingested_at)`; `compute_offers` by `hour(snapshot_at)` (ADR-019).
- **Bronze seeds** overwrite by **identity partition on `valid_from_text`**
  (`OverwriteByPartition(column="valid_from_text")`), not by hour partition:
  the overwrite filter is scoped to the incoming `valid_from_text` values
  (commit `9d1d314`).
- **Silver `exchange_rates`** overwrites by its effective `timestamp`
  partition with a full Bronze read, not append (ADR-019).
- `GCPStorageConfig.directory_path` was removed and `scripts/purge_data.py`
  now purges Iceberg namespaces (#31, ADR-014 amendment).

## Context

B1+B2 migrates Bronze and Silver from Parquet to Iceberg. Four things were
settled with the operator before implementation, each with evidence:

1. **Nested namespaces are unusable in BigLake.** Tested through the real
   Iceberg REST protocol (pyiceberg), not the gcloud wrapper: a catalog
   accepts `create_namespace(('bronze', 'sources'))`, but every operation
   that places the namespace into a URL path — `create_table`, `list_tables`,
   `drop_namespace` — rejects it with `400 Invalid namespace name:
   bronze\u001fsources`. The nested child never registers
   (`list_namespaces(('bronze',))` returns `[]`). A literal-dot single-part
   name is not a workaround: Spark parses identifiers on dots, so
   `FROM bronze.sources.compute_offers` resolves to the two-part form and
   hits the same 400. The operator's stated preference — keep
   `stage.dataset_type` — therefore survives only as a flattened string.
2. **The `ai-compute-arbitrage-monitor-lake` name is already taken** in
   `infra/terraform/envs/gcp/terraform.tfvars` (`gcs_bucket_name`, consumed
   at `orchestration.tf:12`) and refers to a bucket that does not exist;
   only `ai-compute-arbitrage-monitor-bucket` does.
3. **dbt reads Silver as Parquet globs today**
   (`src/transform/models/staging/sources.yaml`), and B1+B2 is forbidden to
   touch that file. Once refine writes Iceberg-only, every DAG's dbt step
   fails until task 2 rewrites it.
4. **Sources are not one thing.** `compute_offers` is an event log
   (`@hourly`, 24 distinct snapshots/day); the four `electricity_tariff_*`
   seeds are reference data that barely changes, where a daily re-scrape of
   an unchanged tariff is the *same* fact, not a new one.

## Decision

- **Namespaces:** four flat namespaces — `bronze_sources`,
  `bronze_seeds`, `silver_sources`, `silver_seeds`. Table name = dataset
  name (e.g. `bronze_sources.compute_offers`,
  `silver_seeds.electricity_tariff_tiers`). This is `stage.dataset_type`
  with `_` instead of `/`: the same two segments as today's
  `bronze/sources/…` paths, minus the (unavailable) hierarchy.
- **Catalog:** id `ai-compute-arbitrage-monitor-catalog`, warehouse pointing
  at the existing `ai-compute-arbitrage-monitor-bucket`. The catalog is
  provisioned by Terraform (`google_biglake_iceberg_catalog` exists in the
  pinned `google` 7.18.0) together with the `billing_project` +
  `user_project_override = true` provider fix from ADR-015. The `-lake`
  suffix is avoided to prevent conflation with the tfvars name.
- **Cutover:** option (a) — B1+B2 lands Iceberg-only, task 2 follows
  immediately; no dual-write transition code.
- **Write strategy is per-source**, not one rule for all:

  | Source | Bronze | Silver | Why |
  |---|---|---|---|
  | `compute_offers` | overwrite hour partition | append | Each hourly poll is a distinct snapshot; all history preserved |
  | `exchange_rates` | overwrite hour partition | append | Each daily rate is a distinct record |
  | `electricity_tariff_*` (all 4) | overwrite hour partition | overwrite `valid_from` partition | Same `valid_from` re-scraped daily = replace; new `valid_from` = new partition (SCD2 for free) |

  Tariff rows are deduplicated in Spark on the natural key before writing.
  Tariffs are reference data; offers are an event log — they must not share
  a write strategy.
- **Spike teardown:** the three leftover catalogs
  (`spike_iceberg_15ac3e23`, `_391781bb`, `_5040e109`) and their buckets are
  deleted only **after B1+B2 validates successfully**.

## Consequences

- Namespace strings propagate into `config/settings.yaml` (iceberg block),
  Spark catalog config keys, the `CREATE NAMESPACE` provisioning step, and
  BigQuery federation identifiers. Flattened names make them safe in
  Spark SQL identifiers and REST paths; they are expensive to change later.
- **Red window between B1+B2 and task 2**: any DAG running dbt in that gap
  fails, because `sources.yaml` globs Parquet that no longer exists.
  Accepted because the pipeline is local-first and production DAGs are not
  triggered in the gap; task 2 must follow immediately.
- `GCPStorageConfig.directory_path` (stage→`bronze/seeds/…` path layout) is
  superseded for Iceberg but still used by `scripts/purge_data.py` and the
  remaining Parquet/GCS surfaces — it cannot simply be deleted in B1+B2.
- Bronze overwrite-scoping is by hour partition for *all* sources, so a
  retry within the hour replaces rather than appends; retry-safety remains
  deferred per ADR-015, and no daily partition collapse occurs.
- Tariff Silver is partitioned by `valid_from`, matching the SCD2 validity
  endpoints recorded in the CONTEXT.md glossary (source-parsed dates, not
  ingest timestamps).
