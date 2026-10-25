-- Test: Verify that the derived cost/profit metrics in fct_compute_offers are internally consistent (ADR-021).
--   1. profit = revenue_usd_per_hr - cost
--   2. cost per TFLOP = cost / total_system_tflops
--   3. profit per TFLOP = profit / total_system_tflops
--   4. cost > 0 (electricity cost is always positive)
-- Floats are compared within 1e-9. If any row violates these invariants, it will be returned.

with invariant_check as (
    select
        machine_id,
        valid_from,
        tariff_tier_skey,
        revenue_usd_per_hr,
        total_system_tflops,
        cost_usd_per_hr,
        profit_usd_per_hr,
        cost_per_tflop_usd,
        profit_per_tflop_usd
    from {{ ref('fct_compute_offers') }}
)

select *
from invariant_check
where abs(profit_usd_per_hr - (revenue_usd_per_hr - cost_usd_per_hr)) > 1e-9
   or abs(cost_per_tflop_usd - (cost_usd_per_hr / nullif(total_system_tflops, 0))) > 1e-9
   or abs(profit_per_tflop_usd - (profit_usd_per_hr / nullif(total_system_tflops, 0))) > 1e-9
   or cost_usd_per_hr <= 0
