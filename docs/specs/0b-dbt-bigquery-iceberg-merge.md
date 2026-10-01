# SPIKE 0B — dbt-bigquery MERGE support + catalog choice for tariff dims

Throwaway spike. Scratch model deleted, scratch dataset/bucket/connection torn down.
Evidence lives in `.spratch/_0b_*.log|json` (untracked) and `src/transform/logs/dbt.log`.

## Verdict

The tariff dims stay **BigQuery-authored**, materialised as **BigQuery-managed Iceberg**
(`catalog_name: managed_iceberg`) — option (a) of the ticket. Option (b) — dims living in
the Lakehouse runtime catalog — is impossible for dbt: BigQuery refuses DML on such tables,
so dbt's MERGE (and therefore the SCD2 build) cannot write them.

## The two checks

**MERGE check — PASS.** `incremental_strategy='merge'` + `unique_key='id'` against a
`managed_iceberg` table: second run reported `MERGE (3.0 rows, 95.0 Bytes processed)`, row
count stayed 3, `distinct id` stayed 3, the mutated row came back as `bravo-merged` @
`11:30:00`, and a new data file appeared under `storage_uri` (in-place update, not append).
Format was still Iceberg afterwards.

**Read check — PASS, but not the form that unlocks retirement.** BigQuery read a
pyiceberg-created table living in the Lakehouse runtime catalog: 5 rows,
`alpha…echo` / `100…500`. The only working mechanism is a **pinned Iceberg external table**
(`ICEBERG=gs://…/metadata/00001-….metadata.json@projects/<p>/locations/europe-west3/connections/<c>`).
Pinning cannot replace `stage_external_sources`: pyiceberg writes a **new numbered metadata
file per commit** (`00000-….metadata.json`, `00001-….metadata.json`) inside a random
per-table directory, so the pointer goes stale after every Spark write. The live
(no-pinning) route is not available via `bq` — `bq mk --external_source` is
"AWS Glue databases" only, and Google's Lakehouse docs are not machine-fetchable
(404/empty to non-browser clients). Status: **unresolved, needs a browser**.

**Expected failure, confirmed — BigQuery cannot write a Lakehouse-catalog table:**

```
INSERT / MERGE into spike_ext_tbl
> DML statements are only supported over tables that have data stored in BigQuery.
> Unsupported table: graphic-mission-505412-j7:spike_0b_read_ds.spike_ext_tbl
```

## Read check — resolved in follow-up (21 Sep 2026, same day)

The section above records the original finding ("live route unresolved, needs a
browser"). **It has since been resolved — the live route works.** The original
failure was a tooling/location artifact, not a product limitation:

1. **Live 4-part-name read — PASS, via both clients.**
   `SELECT id, name, value FROM graphic-mission-505412-j7.spike_iceberg_15ac3e23.spike_ns_15ac3e.spike_table_15ac3e`
   returns all rows with **no external table and no pinning** — directly against
   the BigLake metastore (runtime) catalog. Works via the BigQuery client
   library (`google-cloud-bigquery`, ADC user creds) **and** via the `bq` CLI —
   the CLI only needs `--location=europe-west3` (the resources live in
   europe-west3; the spike's earlier attempts ran with the default `EU`, per the
   `BQ_LOCATION=EU` mismatch documented under C1 consequences #6).
   Evidence: `.spratch/_0b_live_after_bqcli.txt`, `.spratch/_0b_live_after_pyclient.txt`.

2. **Freshness — PASS, not a pinned pointer.** Appending ids 6–10 (zeta…kappa)
   with pyiceberg against the *existing* catalog table (load → `table.append`)
   and immediately re-running the identical query returned **10 rows with zero
   BigQuery-side changes** — no repointing, no refresh, no cache invalidation
   step. Append script: `.spratch/spike_0b_append.py`; live query:
   `.spratch/spike_0b_livecheck.py`. (Note: the 0A roundtrip script
   `.scratch/spike_iceberg_roundtrip.py` cannot be reused for appends — it
   hardcodes catalog `spike_iceberg_catalog` and creates a *new random*
   namespace/table per run.)

3. **Consequence for the original read-check conclusion.** The pinned
   Iceberg external table experiment remains valid as history, but the pinned
   route is unnecessary for reads: the direct 4-part-name query is live. What
   the original spike got right: pyiceberg writes a new numbered metadata file
   per commit — which is why *pinning* goes stale, but a 4-part query resolves
   the current metadata at query time via the metastore.

4. ~~**Open question carried into C1/C2:** dbt sources render 3-part names
   (`database.schema.table`); a 4-part Lakehouse-catalog table has no standard
   dbt source representation. If C1 ever needs dbt to read bronze/silver from
   the runtime catalog, plan a shim (view over the 4-part name) or a custom
   relation render.~~ **Resolved by the 0C identity spike
   (`docs/specs/0c-sa-impersonation-and-dbt-source.md`):** a source with
   `database: "<project>.<catalog>"` renders
   `` `project.catalog`.`namespace`.`table` `` and BigQuery accepts it — verified
   with `dbt show` both as user ADC and via `impersonate_service_account`.
   No shim needed. This does not affect the verdict above (dims stay
   `managed_iceberg`; BigQuery still cannot DML a Lakehouse-catalog table).

## Config surface (differs from the ticket text)

- `file_format='iceberg'` and `table_format='iceberg'` are **both silent no-ops** as model
  configs in dbt-bigquery 1.11.1 — unknown config keys do not error, so a model with either
  key creates an ordinary table and the MERGE check passes while testing nothing.
- The real switch is `catalog_name:` (built-in integration `managed_iceberg`, type
  `biglake_metastore`, `table_format=iceberg`, `file_format=parquet`, `external_volume=None`).
- Model-level `storage_uri` works but triggers `CustomKeyInConfigDeprecation`.
- `storage_uri` is emitted unconditionally for iceberg relations; with no `external_volume`
  and no model `storage_uri` dbt would emit `storage_uri='None'`.

## C1 consequences

1. **Converting an existing dim is the hard part.** `CREATE OR REPLACE` refuses it:
   `Replacing <table> with BigQuery tables for Apache Iceberg are not supported.`
   The cutover is: **drop the plain table, then rebuild it as Iceberg** (verified — worked).
2. **Silent non-conversion is the dangerous case.** Adding `catalog_name` to an
   *incremental* model whose table already exists does **not** convert it — dbt takes the
   MERGE path, exits 0, and the table stays `type: TABLE`. The dims are
   `materialized='table'`, so they hit the hard error instead; verify with
   `bq show`'s `biglakeConfiguration.tableFormat`.
3. **`bq show` cannot evidence row counts.** Iceberg tables report `numRows: 0` /
   `numBytes: 0` (documented), while a real query returns 3 rows. Assert with SQL.
4. **The metadata under `storage_uri` is a stub, not Iceberg metadata:**
   `{"properties":{"bigquery-table-id":"…"},"current-snapshot-id":-1}`. Iceberg metadata is
   held by BigQuery, so this table is **not** readable by Spark from GCS. "dbt writes Iceberg
   that Spark can also read" is not what `managed_iceberg` delivers.
5. **`WITH CONNECTION default` is hard-coded** in `dbt/include/bigquery/macros/adapters.sql:31`.
   It resolves to BigQuery's system-managed connection
   `__default_cloudresource_connection__`; creating a connection named `default` is
   unnecessary (the one created during this spike was unused and has been deleted).
6. **Location mismatch (blocker).** `.env` has `BQ_LOCATION=EU` while
   `ai_compute_arbitrage_monitor_dataset` and the GCS bucket are `europe-west3`. dbt fails
   before doing anything:
   `Not found: Dataset …_spike_0b was not found in location EU`.
   `BQ_LOCATION` is consumed only by `profiles.yml:9` (ADR-013 calls it the single source of
   truth), so this breaks local/Cloud Run dbt runs until corrected.

## Scope gap

The ticket names three dims; the tariff family has **four** — `dim_electricity_tariff_window_schedule`
is also SCD2, is listed in ADR-011, and is joined by `mart_gpu_forecast` and
`mart_profitability_trends`. C1's scope must include it.

## Left in place for 0C (spike 0A's throwaway resources, since 0A deleted its own)

```
bucket   gs://spike-iceberg-test-15ac3e23        (europe-west3)
catalog  spike_iceberg_15ac3e23                  (bl://projects/graphic-mission-505412-j7/catalogs/spike_iceberg_15ac3e23)
table    spike_ns_15ac3e.spike_table_15ac3e      (10 rows: 5 original + 5 follow-up append, ids 6-10)
```

Teardown: `gcloud alpha biglake iceberg catalogs delete spike_iceberg_15ac3e23 --project graphic-mission-505412-j7 --quiet`
then `gcloud storage rm -r gs://spike-iceberg-test-15ac3e23`.
