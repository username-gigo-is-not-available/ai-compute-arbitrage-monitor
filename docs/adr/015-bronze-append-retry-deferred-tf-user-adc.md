# ADR 015: Bronze append-only, retry-safety deferred, terraform user-ADC identity

## Status

Accepted.

## Context

B1+B2 moves Bronze/Silver from Parquet to Iceberg. `compute_offers` runs
`@hourly` (24 snapshots/day), the other five datasets run `@daily`
(`infra/airflow/dags/core_dag.py`). `terraform apply` is hand-run as the
operator's user ADC — there is no terraform workflow in `.github/workflows`
(only composer/dataproc/dbt image jobs) — which is exactly the case where the
Iceberg-catalog IAM API can 403 without a quota/billing project. Ingest
`DEFAULT_ARGS` has `retries: 1, retry_delay: 5min`
(`infra/airflow/dags/core_dag_factory.py`), but plumbing Airflow run identity
down to `Ingestor.store()` is a separate change. Whether `compute_offers`
gets a derived `valid_from` (grain question) is explicitly left open.

## Decision

- Keep all 24 hourly `compute_offers` snapshots. Bronze is append-only; a
  day-partition-scoped overwrite is rejected because it would collapse 24
  observations into 1.
- Retry-safety (run-id/try-number token, check-before-append) is deferred to a
  future task. It is out of scope for B1+B2.
- When the production Iceberg catalog is added to terraform, set
  `billing_project` + `user_project_override = true` on the `google` provider
  blocks, so catalog IAM works under hand-applied user ADC.
- The `compute_offers` grain / derived-`valid_from` question is left open for
  last and decided separately.

## Consequences

- B1+B2 needs no Airflow-context plumbing (`callable_builder.py` untouched).
- A retried ingest attempt can still append a second copy until the deferred
  retry task lands; Iceberg's atomic commit narrows but does not close this.
- No day-grain or key-grain merge may silently drop hourly price history.
