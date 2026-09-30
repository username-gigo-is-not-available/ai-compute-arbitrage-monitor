# ADR 017: Bronze sources write strategy — true append, superseding ADR-016

## Status

Accepted.

## Context

ADR-016 decided Bronze sources (`compute_offers`, `exchange_rates`) use
**overwrite-hour-partition** to provide retry safety: a retried ingest
within the same hour replaces rather than appends. This was a pragmatic
choice at the time because run-id-tagged dedup (ADR-015) was deferred.

During B1+B2 implementation planning it became clear this deviates from
the canonical lakehouse principle: **Bronze is an immutable, append-only
raw event log** — the source of truth you can always rebuild from. Silently
discarding the raw ingest output of a retry makes Bronze a curated layer,
not a raw one, and loses the audit trail of what the ingestor actually sent.

Two observations changed the calculus:

1. **Silver already deduplicates.** `deduplicate_compute_offers` in
   `transform_steps` deduplicates on `["offer_id", "offer_type",
   "ingested_at"]`. Duplicate Bronze records from a retry produce no
   duplicate Silver records.

2. **The Silver watermark excludes already-processed records.** The
   incremental Silver read filters Bronze to `ingested_at > max(ingested_at
   in Silver)`. A late-arriving Bronze retry with an already-processed
   `ingested_at` is never reprocessed into Silver.

Retry safety for Silver is therefore already guaranteed by the transform
layer — it does not need to be enforced in Bronze storage.

## Decision

- **Bronze sources** (`compute_offers`, `exchange_rates`): **true append**,
  partitioned by `hour(ingested_at)`. Every ingest attempt is recorded.
  Duplicate raw files from retries are accepted; they waste a small amount
  of storage but do not affect Silver correctness.
- **Bronze seeds** (`electricity_tariff_*`): unchanged from ADR-016 —
  **overwrite `valid_from` partition**. Seeds are reference data where
  re-scraping the same `valid_from` date is the same fact, not a new event.
- The run-id-tagged dedup for Bronze remains deferred (ADR-015).

## Consequences

- Bronze sources are a faithful raw log — every ingest attempt is
  preserved, which aids debugging and is the standard lakehouse answer to
  "why did you use a Bronze layer."
- Bronze may accumulate duplicate raw files if Airflow retries fire. This
  is a storage concern only; Silver is unaffected.
- ADR-016's overwrite-hour-partition decision for Bronze sources is
  superseded by this ADR. ADR-016's decisions for Bronze seeds, the catalog
  identity, namespace naming, and cutover strategy remain in effect.
