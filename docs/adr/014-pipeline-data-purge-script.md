# ADR 014: Pipeline data purge drops the BigQuery dataset and deletes GCS stage prefixes

## Status

Accepted.

## Context

Local dev reset and testing need a way to wipe the data lake without
re-provisioning anything: bronze/silver objects under `gs://{bucket}/{stage}/`
and the BigQuery dataset's tables. Postgres/Airflow metadata is explicitly out
of scope — that already has its own reset path (`docker compose down -v`).
GCP identity values live in `.env` as the single source of truth (ADR-013,
`GCP_PROJECT_ID` / `GCS_BUCKET_NAME` / `BQ_DATASET_NAME`), and the shared
terraform provisions the BigQuery dataset (`google_bigquery_dataset`) but
declares no dataset-level IAM grants (`google_bigquery_dataset_access`), so
recreating the dataset cannot lose grants.

## Decision

Add `scripts/purge_data.py` following the repo's script style: `load_dotenv()`
+ `SETTINGS_PATH`-aware `load_settings()`, progress and errors through the
`logging` module (configured via `basicConfig` from settings.yaml's `logging`
section and the `run()` entry-point idiom, as the ingest/refine modules do),
`subprocess.run(..., check=True)`, and loud `sys.exit(1)` with the tool's error
logged verbatim on failure.

- An explicit stage flag is required (`--bronze`, `--silver`, `--gold`,
  or `--all` for all three). `--gold` maps to the BigQuery dataset drop, and
  the flags reuse the existing `DataStageType` enum (`src/common/enums.py`).
  No flags print usage and exits non-zero — there is no silent full-purge.
- Bronze/Silver purge: `gcloud storage rm -r gs://{bucket}/{stage}/**` — the
  `**` wildcard with `-r` deletes only live objects under the prefix (it does
  not touch the bucket itself or versions).
- Gold/BigQuery purge: drop the dataset with `bq rm -r -f -d {project}:{dataset}`;
  the next `dbt run` recreates the dataset and all tables. Per-table
  `TRUNCATE TABLE` was rejected: a table list would drift as dbt models evolve,
  no dataset-scoped grants exist to preserve, BigQuery has no dataset-wide
  truncate form, and truncating an SCD2/incremental model then running plain
  `dbt run` cannot restore its history (see ADR-008/012 on `fct_compute_offers`
  incremental semantics) — only a full rebuild can. Drop-and-recreate is the
  standard dbt+BigQuery `--full-refresh`-style dev reset.
- Destructive actions are gated twice: an interactive `y/n` prompt by default,
  and a `--confirm` flag that bypasses the prompt for scripting and Make
  targets. There is no default-yes path — a missing prompt answer aborts.
- Makefile targets `purge-bronze` / `purge-silver` / `purge-gold` /
  `purge-all` (each invoking the script with `--confirm`).

## Consequences

- **Positive**: one-liner stage-scoped reset for local dev; consistent with the
  existing scripts' environment-sourced config and loud-failure style; the
  cost-free `tests/test_deploy_scripts.py` harness covers the new script's
  control flow without touching GCP.
- **Negative**: dropping the terraform-owned dataset leaves benign drift until
  the next `terraform apply` (which recreates the empty dataset idempotently);
  `gcloud`/`bq` must be installed and authenticated in the local shell.
- The stale GCP identity values in `infra/terraform/terraform.tfvars.example`
  (project `ai-compute-arbitrage-monitor`, bucket
  `ai-compute-arbitrage-monitor-lake`) remain a known follow-up from ADR-013 —
  the purge script reads identity from the environment only, never from
  terraform config.