-- Test: mart_gpu_forecast covers every hour of its forecast_days horizon (ADR-021).
-- The app forecasts up to 30 days ahead, so the mart must hold forecast_days x 24 + 1 consecutive hours (the spine
-- includes both ends). An hour missing from the EVN schedule would be dropped by the spine's inner join, and an
-- empty mart would have no hours at all; both fail here.
-- If the hour count or span is wrong, one row describing it will be returned.

with hours as (
    select
        count(distinct forecast_hour)                              as number_of_hours,
        timestamp_diff(max(forecast_hour), min(forecast_hour), hour) as span_hours
    from {{ ref('mart_gpu_forecast') }}
)

select *
from hours
where number_of_hours != {{ var('forecast_days') }} * 24 + 1
   or span_hours is null
   or span_hours != {{ var('forecast_days') }} * 24
