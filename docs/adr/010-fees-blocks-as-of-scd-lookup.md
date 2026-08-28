# ADR 010: Fees and Blocks Use As-of (SCD) Lookup in `fct_compute_offers`

## Status

Accepted

## Context

`fct_compute_offers` joins three tariff dimensions to each offer snapshot:

- **Tiers** (`dim_electricity_tariff_tiers`) — joined as-of over all SCD Type 2 versions:
  `valid_from <= offer_date < valid_to`, so each offer is costed with the rate in
  effect on its `valid_from` date.
- **Fees** (`dim_electricity_tariff_fees`) — previously filtered to `is_latest = true`
  before the join.
- **Blocks** (`dim_electricity_tariff_blocks`) — previously filtered to `is_latest = true`
  before the join.

This was a temporal inconsistency: tiers honored historical versioning, but fees and
blocks silently substituted the *current* (latest) values for every offer — including
historical ones. As long as every consuming mart filtered the fact to
`valid_to = 9999-12-31` (current offers only), the shortcut produced the same numbers
and was invisible. But any time-slicing use case (cost trend over time, historical
arbitrage analysis, "slice and dice by date") would have costed historical offers with
today's fee/block definitions — a silent temporal misalignment of exactly the kind
ADR 009's as-of macros were introduced to prevent for tiers.

## Decision

Remove the `where is_latest = true` filters on the fee and block CTEs in
`fct_compute_offers`, making all three tariff dimensions use **as-of SCD lookup**:
every version is retained and matched against each offer's `valid_from` date via
`valid_from <= date < valid_to`.

Two join-safety properties preserve the fact grain `(offer_id, valid_from, offer_type,
tariff_tier_skey)`:

1. **Fees** stay pivoted to one row per `(consumer_category, valid_from, valid_to)`
   via the existing `max(case when fee_type = 'distribution'/'access' ...)` +
   `group by consumer_category, valid_from, valid_to`. SCD contiguity (non-overlapping
   version ranges per natural key) guarantees exactly one version covers any offer
   date, so the as-of join remains 1:1 — no fan-out.
2. **Blocks** join on `(consumer_category, tariff_window_type, tariff_block_number)`
   with the same as-of range predicates. A tier maps to exactly one block version per
   date (left join), so the grain is unchanged.

## Consequences

- **Positive**:
  - Historical offers are costed with the fees and block boundaries actually in effect
    on their `valid_from` date, matching the tiers' existing as-of semantics.
  - Enables correct time-slicing analytics over the fact table without retro-costing
    drift when fees or block boundaries change.
  - Removes the last temporal inconsistency in the fact table's tariff context lookups.
- **Negative**:
  - The fee and block CTEs now scan full version history instead of a single active row
    per category — a slight compute cost at build time, mitigated because the join
    predicates are equality + as-of date filters.
  - If distribution and access fees ever version on *different* dates for the same
    category, a pivoted row may contain only one of the two fees, leaving the other
    `NULL` for dates in between. EVN publishes fees as a set, so this is not expected;
    a future guard (per-fee-type as-of lookups) is the fallback if it ever occurs.