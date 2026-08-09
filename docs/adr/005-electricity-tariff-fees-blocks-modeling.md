# ADR 005: Electricity Tariff Fees and Blocks Modeling

## Status

Accepted

## Context

The project ingests three types of EVN electricity tariff data:
- **Tiers** — per-kWh energy prices keyed by `(consumer_category, tariff_window_type, tariff_block_number)`
- **Fees** — fixed monthly charges (access fee) and per-kWh surcharges (distribution fee), keyed by `(consumer_category, label)`
- **Blocks** — kWh consumption boundaries that define which tier applies at different usage levels

Previously, only tiers were wired into the dbt transform layer. Fees and blocks existed in ingest/refine but were not exposed in the DWH. The question was whether to merge fees and blocks into `dim_electricity_tariff_tiers` (renaming it to something like `electricity_tariff_plan`) or keep them separate.

## Decision

We will keep **three separate dimension tables**:

1. **`dim_electricity_tariff_tiers`** — per-kWh energy prices (existing, unchanged)
2. **`dim_electricity_tariff_fees`** — fixed and per-kWh fees, long format with `fee_type` column, SCD Type 2
3. **`dim_electricity_tariff_blocks`** — consumption block boundaries, SCD Type 2

The fact table `fct_compute_offers` will join to all three dimensions. The cost calculation will include both the per-kWh distribution fee and the amortized access fee.

A new parameterized mart `mart_user_profitability_scenario` will enable user-specific profitability analysis based on `kwh_consumed_so_far` and `consumer_category` inputs.

## Consequences

- **Positive**:
  - Clean separation of concerns: prices, fees, and boundaries each have their own lifecycle
  - No data duplication or grain mismatch that would result from merging
  - Fees dimension uses `fee_type` column for robust filtering (resilient to label changes)
  - Blocks dimension enables both reference lookups and user-specific scenario analysis
  - `mart_profitability_trends` remains unchanged (hourly trends), new mart handles user scenarios

- **Negative**:
  - `fct_compute_offers` now joins to three dimensions instead of one (plus exchange rates)
  - Slightly more complex cost calculation in the fact table
  - New mart adds maintenance overhead
  - Access fee amortization uses a fixed constant (730 hours/month) which is an approximation

## Cost Model

```
per_kwh_cost = (tariff_value + distribution_fee) * total_system_kwh_per_hr / usd_to_mkd_rate
access_fee_cost = access_fee / 730 / usd_to_mkd_rate
cost_usd_per_hr = per_kwh_cost + access_fee_cost
```

The access fee is amortized over 730 hours (average month: 365.25 / 12 * 24).

## New Mart

`mart_user_profitability_scenario` — parameterized dbt model that:
- Accepts `consumer_category` and `kwh_consumed_so_far` as dbt vars
- Joins to `dim_electricity_tariff_blocks` to determine the user's current block
- Filters `fct_compute_offers` to offers in that block's tier
- Returns ranked profitable offers for the user's specific scenario