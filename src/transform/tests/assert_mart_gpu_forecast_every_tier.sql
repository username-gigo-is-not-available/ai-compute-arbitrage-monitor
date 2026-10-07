-- Test: mart_gpu_forecast carries, for every hour, every GPU model and every current tariff tier whose window
-- matches the hour (ADR-021). The app picks the user's consumer category and walks the tariff blocks, so no tier
-- may be dropped: a household high-tariff hour must carry all of its blocks, not just block 1. Expected counts are
-- derived from dim_electricity_tariff_tiers and from the mart's own GPU models, not hardcoded.
-- Missing hours are caught by assert_mart_gpu_forecast_horizon.
-- If any hour has a different number of GPU models, or any (GPU model, hour) a different number of tiers, than
-- expected, it will be returned.

with row_counts as (
    select
        gpu_model_name,
        forecast_hour,
        scheduled_tariff_window_type,
        count(*) as tier_count
    from {{ ref('mart_gpu_forecast') }}
    group by gpu_model_name, forecast_hour, scheduled_tariff_window_type
),

expected_tiers as (
    select
        tariff_window_type,
        count(*) as expected_tier_count
    from {{ ref('dim_electricity_tariff_tiers') }}
    where is_latest = true
    group by tariff_window_type
),

tier_mismatches as (
    select
        r.gpu_model_name,
        r.forecast_hour,
        'tier count' as failure,
        r.tier_count as actual_count,
        e.expected_tier_count as expected_count
    from row_counts r
    left join expected_tiers e
        on e.tariff_window_type = r.scheduled_tariff_window_type
    where e.expected_tier_count is null
       or r.tier_count != e.expected_tier_count
),

model_mismatches as (
    select
        cast(null as string) as gpu_model_name,
        forecast_hour,
        'gpu model count' as failure,
        count(*) as actual_count,
        (select count(distinct gpu_model_name) from row_counts) as expected_count
    from row_counts
    group by forecast_hour
    having count(*) != (select count(distinct gpu_model_name) from row_counts)
)

select * from tier_mismatches
union all
select * from model_mismatches
