# ADR 004: Rename `tariff_window_type` to `scheduled_tariff_window_type` in `mart_profitability_trends`

## Status

Proposed

## Context

`mart_profitability_trends` was refactored to filter offer rows to only those hours
where the tariff tier's `tariff_window_type` (low/high) matches the EVN time-of-use
schedule's `tariff_window_type` at that hour. The output column was kept as
`tariff_window_type`, but this name is ambiguous: in `fct_compute_offers` the same
column name refers to the tariff tier's *own* window (a property of the tier, not of
the time of day), whereas in the mart it now represents the *effective* window — the
tier's window intersected with the live schedule.

## Decision

Rename the column in `mart_profitability_trends` to `scheduled_tariff_window_type` to
make the distinction explicit. The rename applies only to this mart; the other marts
(`mart_arbitrage_opportunities`, `mart_best_offers_by_gpu`, `mart_gpu_model_summary`)
retain `tariff_window_type` because they show the tier's own window without schedule
filtering.

## Consequences

- **Positive**: Eliminates ambiguity between the tier's window and the effective
  scheduled window. A future reader can immediately tell that
  `scheduled_tariff_window_type` in the trends mart is schedule-derived, while
  `tariff_window_type` in the fact table and other marts is tier-derived.
- **Negative**: Breaking change to the `mart_profitability_trends` output schema.
  Downstream consumers (dashboards, reports) referencing `tariff_window_type` on this
  mart must update to `scheduled_tariff_window_type`.
