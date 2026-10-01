# ADR 019: Retry idempotency comes from a deterministic key, not from Bronze storage

## Status

Accepted. Supersedes the retry-safety reasoning of ADR-015 and ADR-017; amends the
snapshot key of ADR-008 and the Silver write-strategy table of ADR-016.

## Context

ADR-017 argued that Bronze sources could be true append because Silver already makes
retries harmless: `deduplicate_compute_offers` dedups on `(offer_id, offer_type,
ingested_at)` and the Silver watermark is `ingested_at > max(ingested_at)`. That reasoning
does not hold. Every ingestor sets `ingested_at = datetime.now(UTC)` at fetch time, so a
retried or re-run ingest gets a *new* `ingested_at`: it is neither a dedup hit nor below the
watermark, and passes straight through to Silver and Gold.

This was observed on `exchange_rates` (#32): two identical `USD/MKD @ 2026-09-30 00:00:01`
rows in Silver, differing only in `ingested_at`. The in-batch dedup on
`(from_currency, to_currency, timestamp)` could not see the copy already in Silver.

The fix depends on where a row's time comes from:

| Dataset | Key time | Stable across re-fetch? |
|---|---|---|
| `exchange_rates` | provider's `time_last_update_utc` | yes |
| `electricity_tariff_*` seeds | `valid_from` printed in the tariff document | yes |
| `compute_offers` | none: Vast.ai returns "current offers", no as-of time | only if we make it so |

## Decision

**Effective-dated data overwrites by its effective date.** `exchange_rates` is reference
data, not an event log: Silver moves from `IncrementalAppend` to
`PartitionedOverwrite(partition_column="timestamp")`, with full Bronze read and dedup
keeping the latest `ingested_at` per `(from_currency, to_currency, timestamp)`. Seeds are
unchanged.

**Event logs append, keyed on a deterministic snapshot time.** For `compute_offers`:

- An **offer snapshot** is the market *as of the scheduled hour*, not the moment of fetch.
  A new required column `snapshot_at` carries it in Bronze and Silver. `ingested_at`
  keeps its meaning (actual fetch time) in all six datasets. `valid_from` is not used
  before `int_compute_offers`, which maps `snapshot_at as valid_from`; it stays an SCD2
  term.
- `snapshot_at = floor_to_hour(scheduled_at or now())`. Every ingest task receives
  `op_kwargs={"scheduled_at": "{{ data_interval_end }}"}`; every ingest `run()` accepts
  it, only `compute_offers` uses it. `data_interval_end` is the fire time under both
  Airflow 3 trigger timetables and Airflow 2-style interval timetables. `None`, `""` and
  `"None"` count as absent. Flooring always applies, so manual triggers and standalone
  runs land on the hourly grid. Explicit `op_kwargs` was chosen over Airflow's
  context-by-parameter-name injection because the latter fails silently (a renamed
  parameter falls back to `now()`, reintroducing this bug).
- **Backfill guard**: if `now() - snapshot_at > 1h`, the ingest skips the write, logs why,
  and succeeds. A live-only API cannot be backfilled; this keeps Bronze from storing
  today's market under an old label.
- **Bronze stays true append**, partitioned by `hour(snapshot_at)`. Retry duplicates are
  kept as the raw log (ADR-017's principle stands; its justification is replaced by this
  ADR).
- **Silver is idempotent**: watermark `snapshot_at > max(snapshot_at)`. Within a batch,
  the **latest attempt wins per snapshot**: keep only rows whose `ingested_at` is the max
  for their `snapshot_at` (each attempt stamps one `ingested_at`), then dedup on
  `(offer_id, offer_type, snapshot_at)`. A per-key "keep latest" is not enough: Vast.ai
  returns a different offer set on each call (verified 2026-10-01: two fetches 50 s apart,
  1,536 offers each, 2,570 distinct keys combined), so per-key dedup would merge two
  market views into one snapshot. A retry lands in the same refine batch as the failed
  attempt and replaces it; a rerun of an already-refined hour equals the watermark and is
  ignored, so the first successfully refined snapshot of an hour wins.
- **dbt**: `stg_compute_offers.unique_key = (offer_id, snapshot_at, offer_type)` with an
  incremental filter on `snapshot_at`. `fct_compute_offers` already keys on `valid_from`
  and is unchanged.

## Consequences

- Retries, reruns and manual triggers converge on the same Silver for both strategies.
- The `compute_offers` Bronze and Silver tables are dropped and recreated (project in
  testing; raw history discarded deliberately), followed by `dbt --full-refresh` for
  `tag:compute_offers` and the marts.
- A run delayed by Composer up to an hour is labelled with its scheduled hour although it
  shows a later market; `ingested_at` records the gap.
- An extra mid-hour snapshot cannot be taken by design.
- To verify on deploy: Airflow 3.1 renders `data_interval_end` equal to the fire time for
  `@hourly`, and what a manual trigger renders for it (the floor + fallback covers both).
