-- Test: Verify that the derived cost/profit metrics in fct_compute_offers are internally consistent (ADR-021).
-- For each of the marginal and average cost sets:
--   1. profit = revenue_usd_per_hr - cost
--   2. cost per TFLOP = cost / total_system_tflops
--   3. profit per TFLOP = profit / total_system_tflops
--   4. cost > 0 (electricity cost is always positive)
-- And between the sets: average cost >= marginal cost (it adds the access fee).
-- If any row violates these invariants, it will be returned.

with invariant_check as (
    select
        machine_id,
        valid_from,
        tariff_tier_skey,
        revenue_usd_per_hr,
        total_system_tflops,
        marginal_cost_usd_per_hr,
        marginal_profit_usd_per_hr,
        marginal_cost_per_tflop_usd,
        marginal_profit_per_tflop_usd,
        average_cost_usd_per_hr,
        average_profit_usd_per_hr,
        average_cost_per_tflop_usd,
        average_profit_per_tflop_usd
    from {{ ref('fct_compute_offers') }}
)

select *
from invariant_check
where marginal_profit_usd_per_hr    != revenue_usd_per_hr - marginal_cost_usd_per_hr
   or marginal_cost_per_tflop_usd   != marginal_cost_usd_per_hr / nullif(total_system_tflops, 0)
   or marginal_profit_per_tflop_usd != marginal_profit_usd_per_hr / nullif(total_system_tflops, 0)
   or average_profit_usd_per_hr     != revenue_usd_per_hr - average_cost_usd_per_hr
   or average_cost_per_tflop_usd    != average_cost_usd_per_hr / nullif(total_system_tflops, 0)
   or average_profit_per_tflop_usd  != average_profit_usd_per_hr / nullif(total_system_tflops, 0)
   or marginal_cost_usd_per_hr <= 0
   or average_cost_usd_per_hr < marginal_cost_usd_per_hr
