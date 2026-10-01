# B1+B2 — Migrate Bronze + Silver to Iceberg

## Goal

Replace the plain-Parquet bronze/silver storage with Iceberg tables using
the canonical lakehouse pattern:

- **Bronze** = append-only raw event log. Every ingest attempt is recorded
  as-is. Bronze is the immutable source of truth you can always rebuild from.
- **Silver** = incrementally built from Bronze. Each run processes only new
  Bronze records and appends them to Silver. Cleanliness is enforced here
  via the existing `transform_steps` (dedup etc.), not in Bronze.

This partially supersedes ADR-016 (Bronze sources write strategy changes
from overwrite-hour-partition to true append). ADR-015 retry-dedup remains
deferred — Bronze may contain duplicate raw files from retries, but Silver
stays clean because `deduplicate_compute_offers` already deduplicates on
`["offer_id", "offer_type", "ingested_at"]` and the Silver watermark
excludes already-processed `ingested_at` values.

> **Superseded by ADR-019.** `ingested_at` is `now()` at fetch, so the
> reasoning above does not hold for retries. The tables below reflect
> ADR-019: `compute_offers` is keyed on `snapshot_at`; `exchange_rates`
> overwrites by its effective `timestamp`.

## Files in scope

- `src/ingest/base.py` — `Ingestor.store()`
- `src/refine/base.py` — `Pipeline.read()`, `Pipeline.save()`
- `src/refine/init.py` — `initialize_spark()` catalog config
- `src/config/` — new `IcebergConfig` pydantic class + `ConfigLoader.get_iceberg()`
- `config/settings.yaml` — new `iceberg:` block

## Requirements

### Bronze (`Ingestor.store()`)

Rewrite using pyiceberg against the Lakehouse REST catalog.

| Source | Write strategy | Partition |
|---|---|---|
| `compute_offers` | append | `hour(snapshot_at)` |
| `exchange_rates` | append | `hour(ingested_at)` |
| `electricity_tariff_*` seeds | overwrite `valid_from` partition | `valid_from` |

Table identifier: `bronze_sources.<dataset_name>` or
`bronze_seeds.<dataset_name>` per ADR-016 namespace convention.

Seeds use overwrite because re-scraping the same tariff data on the same
day is the same fact — not a new event. Sources are event logs where every
hourly snapshot is distinct.

### Silver (`Pipeline.read()`, `Pipeline.save()`)

| Source | Read | Write strategy | Partition |
|---|---|---|---|
| `compute_offers` | Bronze where `snapshot_at > max(snapshot_at in Silver)` | append | `hour(snapshot_at)` |
| `exchange_rates` | full Bronze read | overwrite `timestamp` partition | `timestamp` |
| `electricity_tariff_*` seeds | full Bronze read | overwrite `valid_from` partition | `valid_from` |

The watermark (`max(ingested_at)` from Silver) is derived at the start of
`Pipeline.run()` via a Spark query on the Silver table. `None` on first run
= read all of Bronze.

The watermark column (`ingested_at` for sources, `valid_from` for seeds)
is a property subclasses can override. The base class handles the rest.

Table identifier: `silver_sources.<dataset_name>` or
`silver_seeds.<dataset_name>` per ADR-016.

### `initialize_spark()` (`src/refine/init.py`)

Add the Iceberg REST catalog config block for both execution branches
(findings from 0D spike):

**Both LOCAL and GCP:**
```
spark.sql.catalog.<catalog_id>.type = rest
spark.sql.catalog.<catalog_id>.uri = https://biglake.googleapis.com/iceberg/v1/restcatalog
spark.sql.catalog.<catalog_id>.warehouse = bl://projects/<project>/catalogs/<catalog_id>
spark.sql.catalog.<catalog_id>.header.x-goog-user-project = <project>
spark.sql.catalog.<catalog_id>.io-impl = org.apache.iceberg.gcp.gcs.GCSFileIO
```

**LOCAL branch:** replace the Hadoop `USER_CREDENTIALS` path with
impersonated ADC token (see `spike_0d_local.py`'s `mint_sa_token()`).
The Hadoop GCS connector (`fs.gs.impl`) is not needed — `GCSFileIO`
handles GCS directly (proven in 0D).

**GCP branch:**
```
spark.sql.catalog.<catalog_id>.rest.auth.type = org.apache.iceberg.gcp.auth.GoogleAuthManager
```

### Config (`src/config/`, `config/settings.yaml`)

New `IcebergConfig` pydantic class loaded via `ConfigLoader.get_iceberg()`:

```yaml
iceberg:
  catalog_id: "ai-compute-arbitrage-monitor-catalog"
  project_id: "graphic-mission-505412-j7"
  warehouse: "bl://projects/graphic-mission-505412-j7/catalogs/ai-compute-arbitrage-monitor-catalog"
```

The REST endpoint URL is a constant (not config). Follow the existing
pydantic/ConfigLoader pattern — no new config style.

## Hard constraints

- Do NOT touch any dbt model, `schema.yml`, or `sources.yaml` — that is
  task 2, deliberately separate so this task's blast radius stays inside
  ingest/refine.
- Do NOT change `offer_type`/dedup key logic (`deduplicate_compute_offers`,
  ADR-008) — settled and orthogonal to storage format.
- Do NOT start on B3 (Vast.ai bisection workaround) as part of this task.

## Acceptance criteria

- A full ingest → refine cycle for `compute_offers` and one seed dataset
  (`electricity_tariff_tiers`) runs end to end and produces Iceberg tables
  in both Bronze and Silver.
- Running the ingest + refine cycle twice produces the same Silver row
  count both times (watermark excludes already-processed records for
  sources; overwrite is idempotent for seeds).
- Existing `transform_steps` (`strip_non_ascii`, `trim_whitespace`,
  `deduplicate_compute_offers`, etc.) still run unchanged on top of the
  new read/write path.
