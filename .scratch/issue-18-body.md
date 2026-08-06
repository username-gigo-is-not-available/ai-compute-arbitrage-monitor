## What to build

Update existing marts and tests to work with the updated fact table.

## Details

- mart_arbitrage_opportunities, mart_best_offers_by_gpu, mart_profitability_trends, mart_gpu_model_summary should automatically pick up the updated cost_usd_per_hr from fct_compute_offers
- Update dwh/schema.yml for fct_compute_offers with new columns (distribution_fee, access_fee, lower_bound_kwh, upper_bound_kwh)
- Update marts/schema.yml for all marts with tier-scoped cost/profit columns
- Add schema tests for dim_electricity_tariff_fees and dim_electricity_tariff_blocks
- Add/extend fact table tests for the updated cost calculation
- Ensure the new mart_user_profitability_scenario has a unique_combination_of_columns test on (offer_id, valid_from, tariff_tier_skey) and a not_null test on profitability_rank

## Scope

Part of spec #12 (Electricity Tariff Fees and Blocks Modeling). This is a vertical slice focused on updating existing marts and tests.