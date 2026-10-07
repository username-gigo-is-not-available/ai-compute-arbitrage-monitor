-- Test: mart_gpu_forecast carries every current tariff tier whose window matches the hour (ADR-021).
-- The app picks the user's consumer category and walks the tariff blocks, so no tier may be dropped: a household
-- high-tariff hour must carry all of its blocks, not just block 1. The expected count is derived from
-- dim_electricity_tariff_tiers, not hardcoded.
-- If any (GPU model, hour) has a different number of tiers than expected, it will be returned.

with row_counts as (
    select
        gpu_model_name,
        forecast_hour,
        scheduled_tariff_window_type,
        count(*) as tier_count
    from {{ ref('mart_gpu_forecast') }}
    group by gpu_model_name, forecast_hour, scheduled_tariff_window_type
),

expected as (
    select
        tariff_window_type,
        count(*) as expected_tier_count
    from {{ ref('dim_electricity_tariff_tiers') }}
    where is_latest = true
    group by tariff_window_type
)

select r.*, e.expected_tier_count
from row_counts r
left join expected e
    on e.tariff_window_type = r.scheduled_tariff_window_type
where e.expected_tier_count is null
   or r.tier_count != e.expected_tier_count
