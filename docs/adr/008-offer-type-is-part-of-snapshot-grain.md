# ADR 008: `offer_type` is part of the compute-offers snapshot grain

## Status

Accepted

## Context

The Vast.ai API is queried separately per `offer_type` (`on_demand`, `bid`, `reserved`) in a single
ingest run, and every offer fetched in that run is stamped with the same `ingested_at`
(`src/ingest/sources/compute_offers.py`). The same `offer_id` (a `(host, machine)` pair) can be
returned under 2–3 different `offer_type` values in that same run, with different prices per type.

Empirical verification against real warehouse and raw-bronze data (BigQuery
`graphic-mission-505412-j7`, ingests 2026-08-14 → 2026-08-15, 11 hourly ingests):

- **Bronze (raw parquet)**: 236 `(offer_id, ingested_at)` keys carry ≥2 distinct `offer_type`
  (204 pairs, 32 triples), across 5 of 11 ingests.
- **Silver (`source.compute_offers`)**: 107 such keys (a *floor* — the refine layer's
  `deduplicate` on `(offer_id, offer_type)` collapses across all time).
- **Staging (`stg_compute_offers`)**: 203 multi-type snapshot keys; sample rows show the same offer
  under `bid + on_demand + reserved` at one timestamp with different prices (e.g. offer `19602471`
  bid $0.1022 vs on_demand $0.2129).
- **Downstream**: `int_compute_offers` and `fct_compute_offers` carry the same 203 multi-type
  snapshots (`fct` has 1,421 rows above a `(offer_id, valid_from, tariff_tier_skey)`-only grain,
  i.e. 203 × 7 tiers).

Spec 0001 asserted a `(offer_id, ingested_at)` grain based on a single spot-check reporting zero
duplicate rows; that spot-check is contradicted by the accumulated history above.

## Decision

- **`offer_type` is part of the offer snapshot identity.** A Vast.ai offer listing genuinely exists
  as multiple simultaneously-active pricing modalities; collapsing to one row per
  `(offer_id, ingested_at)` silently drops real bid prices and breaks downstream mart grains.
- **`stg_compute_offers.unique_key`** → `(offer_id, ingested_at, offer_type)`.
- **`fct_compute_offers.unique_key`** → `(offer_id, valid_from, offer_type, tariff_tier_skey)`.
- **SCD close-out post-hook** correlates on `(offer_id, offer_type)`, so each offer_type's history
  expires independently when that modality drops out of the latest scrape.
- **`int_compute_offers`** passes `offer_type` through unchanged.

## Consequences

- **Positive**: No data loss (bid listings preserved); matches the mart-layer grain which already
  keyed on `offer_type`; SCD close-out is correct per modality.
- **Negative**: The snapshot grain is wider than ADR-003 documented (which predates this finding —
  ADR-003 is amended by this decision); `stg`/`fct` uniqueness tests and the unpivoted-grain
  assertions were updated to include `offer_type`.
- **Validation**: dbt `unique_combination_of_columns` tests on `stg`/`fct`, the
  `assert_*_unpivoted_grain` tests, and a new `assert_stg_compute_offers_offer_type_snapshot_grain`
  test guard against regression.
