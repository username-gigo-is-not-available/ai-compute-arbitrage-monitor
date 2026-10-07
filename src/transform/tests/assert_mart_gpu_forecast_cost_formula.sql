-- Test: mart_gpu_forecast's cost is marginal: it EXCLUDES the fixed monthly access fee (ADR-007, ADR-021).
-- The forecast answers "should I run my GPU this window?", where the access fee is paid either way.
-- We recompute the per-GPU cost from the forecast's own columns and assert it matches:
--   kWh per GPU x (tariff + distribution fee) x (1 + VAT, households only) / USD->MKD rate
-- If the access fee were included, or VAT misapplied, the recomputed value would differ.

with expected as (
    select
        gpu_model_name,
        forecast_hour,
        tariff_tier_skey,
        consumer_category,
        marginal_cost_per_gpu_usd_per_hr,
        (gpu_tdp_watts / 1000.0 * (tariff_value + coalesce(distribution_fee, 0)))
            * (1 + case when consumer_category = 'household' then {{ var('household_vat_rate') }} else 0 end)
            / nullif(usd_to_mkd_rate, 0) as expected_marginal_cost
    from {{ ref('mart_gpu_forecast') }}
)

select *
from expected
where abs(marginal_cost_per_gpu_usd_per_hr - expected_marginal_cost) > 1e-9
