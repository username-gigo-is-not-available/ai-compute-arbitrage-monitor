-- Test: Verify that the derived per-GPU cost/profit metrics in mart_gpu_forecast are internally consistent.
-- These are invariant checks that must hold regardless of the exact cost formula:
--   1. marginal_profit_per_gpu = market_ask_per_gpu - marginal_cost_per_gpu
--   2. marginal_cost_per_tflop = marginal_cost_per_gpu / tflops_per_gpu
--   3. marginal_profit_per_tflop = marginal_profit_per_gpu / tflops_per_gpu
--   4. marginal_cost_per_gpu >= 0 (electricity cost is non-negative)
-- If any row violates these invariants, it will be returned.

select
    gpu_model_name,
    forecast_hour,
    tariff_tier_skey,
    market_ask_per_gpu_usd_per_hr,
    marginal_cost_per_gpu_usd_per_hr,
    marginal_profit_per_gpu_usd_per_hr,
    tflops_per_gpu,
    marginal_cost_per_tflop_usd,
    marginal_profit_per_tflop_usd
from {{ ref('mart_gpu_forecast') }}
where marginal_profit_per_gpu_usd_per_hr != market_ask_per_gpu_usd_per_hr - marginal_cost_per_gpu_usd_per_hr
   or marginal_cost_per_tflop_usd        != marginal_cost_per_gpu_usd_per_hr / nullif(tflops_per_gpu, 0)
   or marginal_profit_per_tflop_usd      != marginal_profit_per_gpu_usd_per_hr / nullif(tflops_per_gpu, 0)
   or marginal_cost_per_gpu_usd_per_hr < 0
