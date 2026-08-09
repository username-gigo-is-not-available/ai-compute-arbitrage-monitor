-- Test: Verify that mart_gpu_forecast EXCLUDES the fixed monthly access fee.
-- The forecast answers a marginal "should I run my GPU this window?" decision, where the
-- access fee is a sunk monthly cost. So the forecast cost must equal the electricity cost
-- WITHOUT the access_fee / 730 amortization term that fct_compute_offers includes.
--
-- We recompute the expected cost from the forecast's own columns (kwh, tariff_value,
-- distribution_fee, usd_to_mkd_rate) and assert it matches cost_usd_per_hr exactly.
-- If the access fee were included, the recomputed value would differ.

with expected_cost as (
    select
        gpu_model_name,
        forecast_hour,
        consumer_category,
        cost_usd_per_hr,
        -- electricity-only cost: kwh * (tariff_value + distribution_fee) / rate
        ((gpu_tdp_watts * number_of_gpus) / 1000.0
            * (tariff_value + coalesce(distribution_fee, 0)))
            / nullif(usd_to_mkd_rate, 0) as expected_cost_no_access_fee
    from {{ ref('mart_gpu_forecast') }}
)

select *
from expected_cost
where abs(cost_usd_per_hr - expected_cost_no_access_fee) > 0.0001
