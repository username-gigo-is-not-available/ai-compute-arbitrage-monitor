-- Test: Verify that the derived per-GPU cost/profit metrics in mart_gpu_forecast are internally consistent.
-- These are invariant checks that must hold regardless of the exact cost formula:
--   1. profit_per_gpu = market_ask_per_gpu - cost_per_gpu
--   2. cost_per_tflop = cost_per_gpu / tflops_per_gpu
--   3. profit_per_tflop = profit_per_gpu / tflops_per_gpu
--   4. cost_per_gpu >= 0 (electricity cost is non-negative)
-- Floats are compared within 1e-9. If any row violates these invariants, it will be returned.

select
    gpu_model_name,
    forecast_hour,
    tariff_tier_skey,
    market_ask_per_gpu_usd_per_hr,
    cost_per_gpu_usd_per_hr,
    profit_per_gpu_usd_per_hr,
    tflops_per_gpu,
    cost_per_tflop_usd,
    profit_per_tflop_usd
from {{ ref('mart_gpu_forecast') }}
where abs(profit_per_gpu_usd_per_hr - (market_ask_per_gpu_usd_per_hr - cost_per_gpu_usd_per_hr)) > 1e-9
   or abs(cost_per_tflop_usd - (cost_per_gpu_usd_per_hr / nullif(tflops_per_gpu, 0))) > 1e-9
   or abs(profit_per_tflop_usd - (profit_per_gpu_usd_per_hr / nullif(tflops_per_gpu, 0))) > 1e-9
   or cost_per_gpu_usd_per_hr < 0
