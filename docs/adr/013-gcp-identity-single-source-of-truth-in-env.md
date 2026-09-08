# ADR 013: GCP identity values live in `.env` as the single source of truth across dbt and Python config

## Status

Accepted.

## Context

The GCP project id (`graphic-mission-505412-j7`), BigQuery dataset name
(`ai_compute_arbitrage_monitor_dataset`), GCS bucket name
(`ai-compute-arbitrage-monitor-bucket`), and BigQuery location (`EU`) were each
hard-coded independently in `config/settings.yaml`, `src/transform/profiles.yml`,
and `src/transform/models/staging/sources.yaml`. Two consumers that must agree —
dbt (`env_var(...)`) and the Python `ConfigLoader` — could drift because no
single place owned the authoritative value.

The live GCP project was renamed to `graphic-mission-505412-j7` (commit
`0df5747`), but a stale `ai-compute-arbitrage-monitor` name still survives in
`infra/terraform/terraform.tfvars.example` and in `settings.yaml`'s dataproc
resource-name strings (`image_tag`, `subnetwork_name`,
`service_account_email`). Those are a separate provisioning-time concern and were
deliberately left unchanged by this ADR.

## Decision

`.env` is the single source of truth for the GCP identity values. They are set
once in `.env` (and documented as examples in `.env.example`) and consumed as:

- **Python config** — `ConfigLoader.get_cluster()` and `get_cloud_run()` read
  `GCP_PROJECT_ID` via `os.environ["GCP_PROJECT_ID"]`; `get_storage()` reads
  `os.environ["GCS_BUCKET_NAME"]`. If a var is unset these raise `KeyError`
  naming the variable — they fail loudly and never fall back to settings.yaml.
- **`src/transform/profiles.yml`** — `project` ← `env_var('GCP_PROJECT_ID')`,
  `dataset` ← `env_var('BQ_DATASET_NAME')`,
  `location` ← `env_var('BQ_LOCATION', 'EU')`.
- **`src/transform/models/staging/sources.yaml`** — every external table's
  `location` uses `gs://{{ env_var('GCS_BUCKET_NAME') }}/...`.

`config/settings.yaml` no longer contains `gcp.project_id` or
`gcp.gcs.bucket_name`. Its remaining non-identity `gcp` fields (`region_name`,
dataproc image tag / runtime packages / subnetwork / service account, cloud run
job name, composer runtime packages) stay sourced from settings.yaml and are
unchanged by this decision.

No fallback reads settings.yaml when the env var is missing; the values must come
from the environment or the failure is loud. `.env` is git-ignored, so only
`.env.example` documents the current production values as examples.

## Consequences

- **Positive**: one authoritative source, so dbt and Python cannot drift on
  project/bucket/dataset; live identity values stay out of committed config.
- **Negative**: the Airflow Compose stack (via `env_file: ../../.env` and the
  dbt `BashOperator`, which inherits the container environment) and local shells
  must load `.env` before running or config loading / dbt compilation fails
  fast. The provisioning scripts `scripts/package_dataproc_modules.py` and
  `scripts/sync_composer_modules.py` now read `GCS_BUCKET_NAME` from the
  environment rather than settings.yaml — they call `load_dotenv()` for local
  runs, and the CI workflows `dataproc-sync-jobs.yml` /
  `cloud-composer-sync-dags.yml` inject it via the `GCP_BUCKET_NAME` GitHub var
  (along with `pip install python-dotenv`).
- **Known follow-up**: the stale `ai-compute-arbitrage-monitor` name in
  `infra/terraform/terraform.tfvars.example` was reconciled to
  `graphic-mission-505412-j7`. The dataproc resource-name strings in
  `settings.yaml` (`image_tag`, `subnetwork_name`, `service_account_email`)
  still carry the old name and remain a follow-up.