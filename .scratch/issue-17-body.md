## What to build

Implement the new parameterized user profitability mart.

## Details

- Create mart_user_profitability_scenario that accepts consumer_category and kwh_consumed_so_far as dbt vars
- Join to dim_electricity_tariff_blocks to determine the user's current block based on kwh_consumed_so_far
- Filter fct_compute_offers to offers in the user's block tier
- Return ranked profitable offers for the user's scenario
- Materialized as a table for performant Streamlit queries
- Grain: (offer_id, valid_from, tariff_tier_skey)
- Include all offer specs and profitability metrics (not just ranking)
- Add schema tests (unique_combination_of_columns on offer_id, valid_from, tariff_tier_skey; not_null on profitability_rank)

## Scope

Part of spec #12 (Electricity Tariff Fees and Blocks Modeling). This is a vertical slice focused on the new mart.