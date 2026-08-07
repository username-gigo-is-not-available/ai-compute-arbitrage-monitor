# ADR 007: Add a Forward-Looking Forecasting Mart

## Status

Proposed

## Context

The project serves two distinct use cases. The first — market-scanning — is well covered by `mart_arbitrage_opportunities`, `mart_best_offers_by_gpu`, and `mart_gpu_model_summary`, which rank current Vast.ai offers by profitability. The second — forecasting — is not covered: a host who owns a specific GPU (e.g., a GTX 3080) and wants to know the electricity cost and profit of renting it over a specific future window (e.g., a weekend, where Saturday has mixed low/high tariff windows and Sunday is all-low) has no model to answer that. `mart_user_profitability_scenario` was the closest, but it models a renter/consumer framing (household `kwh_consumed_so_far`) that contradicts the host-side actor, and it reuses host-side cost semantics for a renter question.

## Decision

- **Remove `mart_user_profitability_scenario`** — it is the only renter-framed mart in a host-framed project, is not used by any chart, and its `kwh_consumed_so_far` input is a household-consumption concept that does not fit the host-side model.
- **Add a new forecasting mart** — a materialized table over GPU models × future hours, generated from `dim_electricity_tariff_window_schedule` as the future-hours spine. It answers "given my specific GPU over a specific future window, what is the tariff-schedule-aware electricity cost and profit?"
- **Revenue** defaults to the current market ask price for that GPU model, overridable by a user-provided ask price (dbt var).
- **Access fee is excluded** from the forecast — it is a fixed monthly sunk cost, so it should not reduce the marginal profit of a short rental window. The amortized access fee remains in `fct_compute_offers` for full-month average analysis.

## Consequences

- **Positive**: Serves the host's real weekend-rental decision; removes the renter/host framing contradiction; keeps the market-scanning marts intact.
- **Negative**: New mart adds maintenance overhead; the forecast's revenue is a proxy (market ask) unless the user supplies their own price; the future-hours spine depends on the tariff schedule being current.