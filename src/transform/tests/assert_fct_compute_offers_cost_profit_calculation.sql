-- Test: Verify that the derived cost/profit metrics in fct_compute_offers are internally consistent.
-- These are invariant checks that must hold regardless of the exact cost formula:
--   1. profit_usd_per_hr = revenue_usd_per_hr - cost_usd_per_hr
--   2. cost_per_tflop_usd = cost_usd_per_hr / total_system_tflops
--   3. profit_per_tflop_usd = profit_usd_per_hr / total_system_tflops
--   4. cost_usd_per_hr > 0 (electricity cost is always positive)
-- If any row violates these invariants, it will be returned.

with invariant_check as (
    select
        offer_id,
        valid_from,
        tariff_tier_skey,
        revenue_usd_per_hr,
        cost_usd_per_hr,
        profit_usd_per_hr,
        total_system_tflops,
        cost_per_tflop_usd,
        profit_per_tflop_usd,
        revenue_usd_per_hr - cost_usd_per_hr as expected_profit,
        cost_usd_per_hr / nullif(total_system_tflops, 0) as expected_cost_per_tflop,
        profit_usd_per_hr / nullif(total_system_tflops, 0) as expected_profit_per_tflop
    from {{ ref('fct_compute_offers') }}
)

select *
from invariant_check
where profit_usd_per_hr != expected_profit
   or cost_per_tflop_usd != expected_cost_per_tflop
   or profit_per_tflop_usd != expected_profit_per_tflop
   or cost_usd_per_hr <= 0