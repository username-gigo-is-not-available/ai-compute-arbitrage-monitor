# SPIKE 0C (identity portion) — spike-sa impersonation + dbt source() over the Lakehouse catalog

Throwaway spike. Scratch dbt source file and `spike` target deleted after the run;
evidence lives in `.spratch/_0c_*.log` (untracked). Complements
`docs/specs/0b-dbt-bigquery-iceberg-merge.md`.

## What was proven

1. **Impersonation works.** `iamcredentials.googleapis.com` enabled;
   `roles/iam.serviceAccountTokenCreator` on `spike-sa@…` bound to
   `user:grigorij.josifovski@gmail.com`. Identity proof: the impersonated token's
   `tokeninfo.azp` = `108423816014679296790` = spike-sa's `uniqueId`
   (SA access tokens carry no `email` claim — match on `azp`/uniqueId, not email).
   Script: `.spratch/spike_0c_impersonation_check.py`.

2. **Full pyiceberg round trip as spike-sa — PASS.** Catalog load, namespace +
   table create, append (5 rows), read-back all as the SA, with the SA token on
   **both** the REST catalog calls and the GCS FileIO
   (`FileIO = PyArrowFileIO; carries SA token: True` — no false pass).
   Script: `.spratch/spike_iceberg_roundtrip.py --sa … --skip-bq`
   (log: `_0c_roundtrip_sa_run3.log`; runs 1–2 are the 403s below).

3. **SA write to the production-shaped catalog table — PASS.** Appending to the
   *existing* kept table (and rewriting it via `table.overwrite`) as spike-sa
   works with the same project-level grants — the SA can be the writer identity
   for bronze/silver in the Lakehouse catalog. Scripts:
   `spike_0b_append.py` / `spike_0c_dedup.py` (both honour `IMPERSONATE_SA`).

4. **BigQuery live read as spike-sa — PASS.** Four-part-name query with
   `use_query_cache=False`: 15 rows, `cache_hit=False`. Needed
   `roles/bigquery.jobUser` (first attempt 403: `bigquery.jobs.create`).
   Script: `spike_0b_livecheck.py` with `IMPERSONATE_SA` env var
   (log: `_0c_livecheck_sa.log`).

5. **dbt `source()` resolves the Lakehouse catalog table — PASS, no macro
   fallback needed.** A source with `database: "<project>.<catalog>"` +
   `schema: <namespace>` renders
   `` `project.catalog`.`namespace`.`table` `` and BigQuery accepts it. Verified
   with `dbt show --inline` both as user ADC and with
   `impersonate_service_account: spike-sa@…` in the profile — both returned the
   full 15 rows. (First 5-row "failure" was just `dbt show`'s default
   `--limit 5`, not a resolution problem.)
   Scratch config (deleted after): `models/staging/spike_sources.yml`, a
   `spike` target in `profiles.yml`. Logs: `_0c_dbt_show_user.log`,
   `_0c_dbt_show_sa.log`. **This retires the "dbt sources are 3-part only" open
   question** recorded in the 0B spec — `database` absorbs the catalog.

## Grant ledger (what spike-sa needed — for iam.tf)

Project-level, in the order the runs demanded them:

| # | Role | Why (the 403 that named it) |
|---|------|------------------------------|
| 1 | `roles/biglake.editor` | `biglake.catalogs.get denied` on first REST-catalog call (runs 1–2) |
| 2 | `roles/bigquery.jobUser` | `bigquery.jobs.create denied` on the live query as SA |

**Pre-existing** (was already on spike-sa before this spike):
`roles/storage.objectAdmin` — covered bucket create/read/write for both the
fresh per-run buckets and the kept table's bucket (the catalog is in
end-user credential mode, so bucket read must come from IAM, and it did).

**Explicitly NOT needed** (no 403 ever named them, despite the docs implying
otherwise): `roles/serviceusage.serviceUsageConsumer` (the
`x-goog-user-project` header worked without it) and `roles/biglake.viewer`
(`biglake.editor` supersets it).

Mapping to `iam.tf`:
- `composer_sa` (Lakehouse writer): add `roles/biglake.editor`. It already has
  `storage.objectAdmin` + `bigquery.jobUser`. The spike-sa proof is exactly
  this triple.
- `dbt_cloud_run_sa` (Lakehouse reader via dbt): add `roles/biglake.editor`
  (or the minimal read set) + it already has `bigquery.jobUser`; the dbt
  `impersonate_service_account` path was proven with the same shape.
- Impersonation chain: identity holder needs
  `roles/iam.serviceAccountTokenCreator` on the target SA (proven above);
  composer→worker impersonation already uses `roles/iam.serviceAccountUser`
  in iam.tf for the attached-SA pattern.

## Incidents & side findings

- **Duplicate rows fixed.** ids 6–10 had been double-appended (a batch-1
  append ran twice across sessions), so the table showed 20 rows with 5 pairs.
  Fixed with `table.overwrite()` of the canonical 15-row frame as the SA —
  deliberately avoiding Iceberg delete files, whose BigLake-reader support was
  not tested here. The table is now 15 distinct rows (ids 1–15).
- **Stale `~/.dbt/profiles.yml`.** The user-level profiles file holds an old
  `transform` profile (`even-card-485518-r5`, dataset
  `gpu_compute_arbitrage_monitor_dataset`, location EU). dbt without
  `--profiles-dir` picks it up — every repo-invoked dbt command must pass
  `--profiles-dir src/transform` (the repo's logs show this is the established
  pattern; Composer uses `/opt/airflow/src/transform`). Consider deleting the
  stale user-level `transform` block to remove the trap.
- `~/.dbt` shadowing is why the first `dbt show` said "no target named spike".
