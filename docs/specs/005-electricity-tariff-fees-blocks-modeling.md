# Spec: Electricity Tariff Fees and Blocks Modeling

## Problem Statement

The electricity tariff model currently only includes per-kWh energy prices (tariff tiers) in the DWH. However, EVN electricity bills consist of three components: per-kWh energy prices, per-kWh distribution fees, and fixed monthly access fees. Additionally, the tiered pricing structure uses consumption blocks (e.g., block 1: 0-210 kWh/month, block 2: 210-420 kWh/month) to determine which per-kWh rate applies. Without fees and blocks in the DWH, the cost calculations in `fct_compute_offers` are incomplete, and the application cannot provide user-specific profitability analysis based on actual consumption patterns.

## Solution

Expand the DWH to include electricity tariff fees and blocks as separate dimension tables, update the fact table cost calculation to include all bill components, and add a new parameterized mart for user-specific profitability scenarios.

## User Stories

1. As a data engineer, I want fees and blocks to be separate dimension tables (not merged into tiers), so that each concept has a clear lifecycle and grain without data duplication or circular dependencies.

2. As a data engineer, I want the fees dimension to use a `fee_type` column (not just label text), so that the model is resilient to Macedonian label changes from EVN.

3. As a data engineer, I want the fees dimension in long format (one row per fee per consumer category), so that the fact table can join and pivot to get distribution and access fees.

4. As a data engineer, I want the blocks dimension to be a full SCD Type 2 dimension table, so that block boundary changes are tracked over time.

5. As a data engineer, I want `fct_compute_offers` to join to `dim_electricity_tariff_fees` and `dim_electricity_tariff_blocks`, so that the fact table has access to all tariff components.

6. As a data engineer, I want the cost calculation in `fct_compute_offers` to include the distribution fee (per-kWh) and access fee (fixed monthly amortized), so that profitability analysis reflects the true electricity cost.

7. As a data engineer, I want the access fee to be amortized over 730 hours per month (average month), so that the per-hour cost is consistent and doesn't require per-month logic in the fact table.

8. As a data engineer, I want `tariff_block_number` to remain sourced from `dim_electricity_tariff_tiers` (not re-sourced from blocks), so that there is no circular dependency in the fact table joins.

9. As a data engineer, I want `fct_compute_offers` to include `lower_bound_kwh` and `upper_bound_kwh` from the blocks dimension, so that marts can reference block boundaries without additional joins.

10. As a data engineer, I want `mart_profitability_trends` to remain unchanged, so that existing hourly trend analysis continues to work without modification.

11. As a data engineer, I want a new parameterized mart `mart_user_profitability_scenario` that accepts `consumer_category` and `kwh_consumed_so_far` as dbt vars, so that Streamlit can query user-specific profitability.

12. As a data engineer, I want the new mart to join to `dim_electricity_tariff_blocks` to determine the user's current block based on `kwh_consumed_so_far`, so that the correct per-kWh price is applied.

13. As a data engineer, I want the new mart to filter `fct_compute_offers` to offers in the user's current block tier, so that the results are relevant to the user's actual pricing situation.

14. As a data engineer, I want the new mart to return ranked profitable offers, so that Streamlit can display the best opportunities for the user's scenario.

15. As a data engineer, I want the refine layer to expose fees and blocks to dbt (uncomment sources.yaml entries), so that the DWH can consume these datasets.

16. As a data engineer, I want the staging layer to include `stg_electricity_tariff_fees` and `stg_electricity_tariff_blocks` models, so that fees and blocks are typed and validated like other seed data.

17. As a data engineer, I want the intermediate layer to include `int_electricity_tariff_fees` and `int_electricity_tariff_blocks` models, so that fees and blocks have a transformation layer consistent with other dimensions.

18. As a data engineer, I want the DWH layer to include `dim_electricity_tariff_fees` and `dim_electricity_tariff_blocks` models with SCD Type 2 logic, so that historical accuracy is maintained.

19. As a data engineer, I want the `fee_type` column to be populated during the intermediate or DWH layer (not in ingest), so that the raw ingest model remains simple and the type assignment is a business logic decision.

20. As a data engineer, I want the fact table's `cost_usd_per_hr` calculation to be: `(tariff_value + distribution_fee) * total_system_kwh_per_hr / usd_to_mkd_rate + access_fee / 730 / usd_to_mkd_rate`, so that all cost components are included.

21. As a data engineer, I want the marts (`mart_arbitrage_opportunities`, `mart_best_offers_by_gpu`, `mart_gpu_model_summary`) to automatically pick up the updated `cost_usd_per_hr` from `fct_compute_offers`, so that no mart changes are required.

22. As a data engineer, I want the new `mart_user_profitability_scenario` to have a grain of `(offer_id, valid_from, tariff_tier_skey)`, so that each offer in the user's block is represented exactly once.

23. As a data engineer, I want the new mart to be materialized as a table (not a view), so that Streamlit queries are performant.

24. As a data engineer, I want the new mart to include all offer specs and profitability metrics (not just ranking), so that Streamlit can display full offer details.

25. As a data engineer, I want the CONTEXT.md glossary to include definitions for "Tariff fee" and "Tariff block", so that the domain language is explicit and consistent.

## Implementation Decisions

- **Three separate dimension tables**: `dim_electricity_tariff_tiers` (existing, per-kWh prices), `dim_electricity_tariff_fees` (new, long format with `fee_type`, SCD Type 2), `dim_electricity_tariff_blocks` (new, SCD Type 2). Merging was rejected due to grain mismatch and data duplication.

- **Fees dimension design**: Long format (Option A) with grain `(consumer_category, fee_type, valid_from)`. Columns: `skey`, `consumer_category`, `fee_type` (`distribution` or `access`), `label` (Macedonian), `metric`, `value`, `valid_from`, `valid_to`, `is_latest`. SCD Type 2 tracking.

- **Blocks dimension design**: Grain `(consumer_category, tariff_window_type, tariff_block_number, valid_from)`. Columns: `skey`, `consumer_category`, `tariff_window_type`, `tariff_block_number`, `lower_bound_kwh`, `upper_bound_kwh`, `valid_from`, `valid_to`, `is_latest`. SCD Type 2 tracking.

- **Fact table joins**: `fct_compute_offers` joins to `dim_electricity_tariff_tiers` (for price, window type, block number), `dim_electricity_tariff_fees` (for distribution and access fees), and `dim_electricity_tariff_blocks` (for block boundaries). `tariff_block_number` remains sourced from tiers to avoid circular dependency.

- **Cost calculation**: `cost_usd_per_hr = (tariff_value + distribution_fee) * total_system_kwh_per_hr / usd_to_mkd_rate + access_fee / 730 / usd_to_mkd_rate`. The access fee is amortized over 730 hours (average month: 365.25 / 12 * 24).

- **New mart**: `mart_user_profitability_scenario` — parameterized dbt model that accepts `consumer_category` and `kwh_consumed_so_far` as dbt vars. Joins to `dim_electricity_tariff_blocks` to determine the user's block, filters `fct_compute_offers` to that block's tier, and returns ranked profitable offers. Materialized as a table.

- **Existing marts unchanged**: `mart_profitability_trends` remains as-is (hourly aggregated trends). `mart_arbitrage_opportunities`, `mart_best_offers_by_gpu`, and `mart_gpu_model_summary` automatically pick up the updated `cost_usd_per_hr` from the fact table.

- **Refine/staging exposure**: Uncomment `electricity_tariff_fees` and `electricity_tariff_blocks` in `sources.yaml`. Add `stg_electricity_tariff_fees` and `stg_electricity_tariff_blocks` staging models. Add `int_electricity_tariff_fees` and `int_electricity_tariff_blocks` intermediate models.

- **`fee_type` assignment**: The `fee_type` column is populated in the intermediate or DWH layer based on the `label` or `metric` field, not in the ingest layer. This keeps the ingest model simple and makes the type assignment a business logic decision that can evolve.

## Testing Decisions

- **Dimension tests**: All three dimensions will have standard SCD Type 2 tests (`unique_combination_of_columns` on natural key + `valid_from`, `expression_is_true` for `valid_from < valid_to`).

- **Fact table tests**: Existing tests on `fct_compute_offers` will be updated to reflect the new joins and cost calculation. The `unique_combination_of_columns` test on `(offer_id, valid_from, tariff_tier_skey)` remains unchanged.

- **New mart tests**: `mart_user_profitability_scenario` will have a `unique_combination_of_columns` test on `(offer_id, valid_from, tariff_tier_skey)` and a `not_null` test on the ranking column.

- **Cost calculation test**: A new test or assertion will verify that `cost_usd_per_hr` includes both the distribution fee and the amortized access fee.

- **Integration test**: Verify that the new mart correctly filters to the user's block based on `kwh_consumed_so_far` and `consumer_category` vars.

## Out of Scope

- **Streamlit implementation**: This spec covers the dbt models only. The Streamlit frontend that collects `consumer_category` and `kwh_consumed_so_far` from users and queries `mart_user_profitability_scenario` is out of scope.

- **Business tier blocks**: Currently, blocks only apply to household high-tariff consumption. The blocks dimension is designed to support business blocks if EVN introduces them, but the initial implementation only handles household blocks.

- **Per-month actual days**: The access fee amortization uses a fixed 730 hours/month. A more accurate approach using actual days per month is out of scope for the initial implementation.

- **Fee label translation**: The `label` column remains in Macedonian. English translation is out of scope.

- **Incremental strategy for new dims**: The initial implementation will use full refresh for `dim_electricity_tariff_fees` and `dim_electricity_tariff_blocks`. Incremental strategies can be added later if needed.

## Further Notes

- The design was validated through a grilling session (see ADR 005) that explored the semantic relationships between tiers, fees, and blocks, and resolved the circular dependency issue by keeping `tariff_block_number` sourced from tiers while adding block boundaries from the blocks dimension.

- The `fee_type` column (`distribution` or `access`) is derived from the `label` or `metric` field during the intermediate or DWH layer transformation. The exact derivation logic (e.g., matching on known labels or metric patterns) is an implementation detail to be determined during development.

- The `mart_user_profitability_scenario` is designed to be queried by Streamlit with specific var values. It is not intended to be run on a schedule like the other marts; instead, it should be run on-demand or materialized as a view for interactive queries.