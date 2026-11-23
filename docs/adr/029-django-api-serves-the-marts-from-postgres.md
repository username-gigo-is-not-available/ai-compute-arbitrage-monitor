# ADR 029: A read-only Django API serves the marts from its own Postgres

## Status

Accepted. Shapes Track B (the API). Amends ADR-021 (the app is the Django API) and ADR-026 (see its amendment).

## Context

The marts answer a host's questions (which GPU is worth hosting, what will my GPU earn), but only through BigQuery.
The API applies the opinions the marts leave out (ADR-026) and resolves the tariff block per request (ADR-021).
The five marts total about 91 MB (2026-11-23: `mart_gpu_forecast` 342k rows, 74.5 MB; `mart_gpu_profitability` 917
rows) and dbt rebuilds each one as a full table once a day. BigQuery answers a small lookup in 1–2 s, and Django's
ORM has no maintained BigQuery backend.

## Decision

- **A Django REST Framework API in `src/api/`**, in the same `pyproject.toml` as the pipeline. Pages come later as a
  separate program (likely Streamlit) that only calls the API and never reads BigQuery, so every rule lives once.
- **The API reads its own Postgres, not BigQuery.** A Postgres container of its own (`api-postgres`, port 5433),
  separate from the Airflow metastore, so resetting Airflow never touches the API's data.
- **Django owns the tables.** Each mart is a Django model (an `id`, plus a unique constraint on the mart's grain) and
  migrations track its columns, so each column is declared once.
- **`sync_marts` copies all five marts in one transaction.** A Django management command replaces every table inside
  one `transaction.atomic()`: readers see yesterday's complete data until the commit, a failure rolls everything
  back, and the API never mixes two days. It runs last in the marts DAG, `dbt run >> dbt test >> sync_marts`, as
  `python -m django sync_marts` through the existing `PythonJobStrategy`, so a failed mart test never publishes.
- **Read-only and public, with no users until a feature stores something per host** (a saved setup, an alert, a
  watchlist, #110). The data is public market and tariff data, so there are no accounts and no authentication; `GET` only. Abuse is handled by DRF's `AnonRateThrottle`, 60 requests per minute per IP.
- **Every list endpoint is paged** (100 per page) and states the opinions it applied and its `census_date`.
- **URLs are `/api/v1/gpus/<view>/`**: `profitability`, `summary`, `machines`, `history`, `forecast`, built in that
  order.
- **Tests run on Postgres**, locally and as a CI service container, since SQLite orders `NULL`s the other way.
- **Local only for now.** Postgres runs in Docker on the laptop, like the daily Airflow (ADR-028); a showcase would
  use the Cloud SQL 30-day free trial instance (one per project), a change of connection settings only. In `gcp`
  mode `sync_marts` stays off until a cloud Postgres exists.

## Considered Options

- **Query BigQuery on every request**: no copy, but raw SQL strings instead of the ORM, 1–2 s per request, and tests
  that fake BigQuery.
- **The pipeline owns the copy, Django reads `managed = False` tables**: keeps Django out of the pipeline, but each
  table is declared twice and the two drift.
- **A SQLite file built by `sync_marts`**: no server and still the ORM, enough for read-only data, but awkward once
  per-host features start writing, and Postgres is the standard Django pairing.
- **JWT or token auth**: needs user accounts, and with them signup, fake accounts and spam, to protect data that is
  public.
- **A separate `pyproject.toml` (uv workspace)**: a smaller Django image, but two environments to learn before
  Django. Splitting out `src/api/` later is a mechanical move.

## Consequences

- **Positive**:
  - Lookups take milliseconds, and the API is real Django: models, querysets, admin and DRF filters.
  - Postgres's unique constraint re-checks each mart's grain on every copy.
- **Negative**:
  - The API is up to a day behind the census plus the time until `sync_marts` runs, and shows its `census_date` so a
    stale copy is visible.
  - The throttle counts in Django's per-process memory cache; a deployment with several workers needs Redis.
  - A future Django image would carry the pipeline's dependencies (pyspark, dbt) until `src/api/` is split out.
