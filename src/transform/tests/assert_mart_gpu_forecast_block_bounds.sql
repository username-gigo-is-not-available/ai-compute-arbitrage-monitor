-- Test: every blocked tier in mart_gpu_forecast carries its block bounds, and no other tier does (ADR-021).
-- The app walks the blocks with a running total, so a block row with a null lower bound would break it silently
-- (the bounds come from a left join to dim_electricity_tariff_blocks). Tiers without a block number (low tariff,
-- business) must have null bounds. Only the last block may have a null upper bound.
-- If any row breaks these rules, it will be returned.

with blocks as (
    select
        consumer_category,
        tariff_window_type,
        max(tariff_block_number) as last_block_number
    from {{ ref('dim_electricity_tariff_blocks') }}
    where is_latest = true
    group by consumer_category, tariff_window_type
)

select
    f.gpu_model_name,
    f.forecast_hour,
    f.tariff_tier_skey,
    f.consumer_category,
    f.scheduled_tariff_window_type,
    f.tariff_block_number,
    f.lower_bound_kwh,
    f.upper_bound_kwh
from {{ ref('mart_gpu_forecast') }} f
left join blocks b
    on  b.consumer_category  = f.consumer_category
    and b.tariff_window_type = f.scheduled_tariff_window_type
where (f.tariff_block_number is not null and f.lower_bound_kwh is null)
   or (f.tariff_block_number is not null and f.upper_bound_kwh is null
       and f.tariff_block_number != b.last_block_number)
   or (f.tariff_block_number is null and (f.lower_bound_kwh is not null or f.upper_bound_kwh is not null))
