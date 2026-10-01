# SPIKE 0B — dbt-bigquery MERGE support + catalog choice for tariff dims

**Status:** Completed 21 Sep 2026. Verdict captured in `docs/specs/0b-dbt-bigquery-iceberg-merge.md`.

## Where the records live

- **Spec + verdict:** `docs/specs/0b-dbt-bigquery-iceberg-merge.md` (the canonical write-up the ticket asked for — a few sentences, not a full ADR).
- **Raw evidence:** untracked files under `.spratch/`, matching `_0b_*` (logs and JSON dumps for each check, plus a compile/ruff pass). The dbt log is at `src/transform/logs/dbt.log`.
- **State token:** `.spratch/.spike_0b_state.txt` — the kept resources for 0C and their teardown commands. Not tracked.

## What was run

- A standalone Python driver ran the original spike. It was throwaway and is
  **not kept**: during the follow-up session it was found corrupted and was
  deleted — the follow-up scripts below supersede it, and the original evidence
  (`_0b_*.log|json`) is intact.
- Preflight (tools, ADC, project/bucket/dataset identity), then the dbt runs against a scratch dataset `ai_compute_arbitrage_monitor_dataset_spike_0b`, then the Lakehouse read check against a separate scratch dataset/connection and the leftover 0C throwaway resources.
- Cleanup deleted the scratch model, its `target/` artifacts, the scratch dataset, the scratch bucket (`gs://spike-0b-iceberg-…`), the scratch connection, and the read-check dataset/connection. Production datasets were never touched.

## Follow-up: read check resolved (same day)

Two small scripts under `.spratch/` resolved the spec's open item
("live route unresolved"):

- `spike_0b_livecheck.py` — live 4-part-name query via `google-cloud-bigquery`
  (ADC). PASS: 5 rows before the append.
- `spike_0b_append.py` — pyiceberg append of ids 6–10 (zeta…kappa) to the
  **existing** kept table (`load_table` → `append`). PASS: 10 rows.
  (The 0A roundtrip script cannot be reused for appends — it targets catalog
  `spike_iceberg_catalog` and creates a new random table per run.)
- Re-running the live query returned **10 rows with zero BigQuery-side
  changes** → the read is live, not a pinned pointer. Evidence:
  `_0b_live_after_bqcli.txt` (also proves the `bq` CLI works with
  `--location=europe-west3`) and `_0b_live_after_pyclient.txt`.
- The corrupted `spike_0b_checks.py` driver was deleted.

## What still needs to run

- **0C:** the Spark roundtrip (local + Dataproc Serverless) against the 0A throwaway catalog. The 0B run left the needed throwaway resources in place so 0C does not rebuild them — see the "Left in place for 0C" block in the spike spec and the state token above.
