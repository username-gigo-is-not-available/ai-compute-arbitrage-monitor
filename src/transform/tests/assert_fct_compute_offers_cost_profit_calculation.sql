-- Test: Verify that cost_usd_per_hr and profit_usd_per_hr are calculated correctly
-- for the unpivoted fct_compute_offers table.
-- This test checks that:
--   1. cost_usd_per_hr = (total_system_kwh_per_hr * tariff_value) / usd_to_mkd_rate
--   2. profit_usd_per_hr = revenue_usd_per_hr - cost_usd_per_hr
-- If any row violates these calculations, it will be returned.

with calculations_check as (
    select
        offer_id,
        valid_from,
        tariff_tier_skey,
        total_system_kwh_per_hr,
        tariff_value,
        usd_to_mkd_rate,
        revenue_usd_per_hr,
        cost_usd_per_hr,
        profit_usd_per_hr,
        (total_system_kwh_per_hr * tariff_value) / nullif(usd_to_mkd_rate, 0) as expected_cost,
        revenue_usd_per_hr - ((total_system_kwh_per_hr * tariff_value) / nullif(usd_to_mkd_rate, 0)) as expected_profit
    from {{ ref('fct_compute_offers') }}
)

select *
from calculations_check
where cost_usd_per_hr != expected_cost
   or profit_usd_per_hr != expected_profit
