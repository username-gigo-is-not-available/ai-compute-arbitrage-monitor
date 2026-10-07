-- Test: mart_gpu_forecast matches each hour to the EVN schedule in Macedonian local time, not UTC (ADR-021).
-- Recomputes the local day of week and hour with datetime(), independently of the evn_* macros, and checks the
-- scheduled tariff window against the schedule at that local hour.
-- If any row's window, day_of_week or hour_of_day disagrees, it will be returned.

with local_hours as (
    select distinct
        forecast_hour,
        day_of_week,
        hour_of_day,
        scheduled_tariff_window_type,
        datetime(forecast_hour, '{{ var("evn_timezone") }}') as local_forecast_datetime
    from {{ ref('mart_gpu_forecast') }}
)

select l.*, s.tariff_window_type as expected_tariff_window_type
from local_hours l
left join {{ ref('dim_electricity_tariff_window_schedule') }} s
    on  s.is_latest = true
    and s.day_of_week = mod(extract(dayofweek from l.local_forecast_datetime) + 5, 7) + 1
    and s.hour        = extract(hour from l.local_forecast_datetime)
where s.tariff_window_type is null
   or s.tariff_window_type != l.scheduled_tariff_window_type
   or l.day_of_week        != mod(extract(dayofweek from l.local_forecast_datetime) + 5, 7) + 1
   or l.hour_of_day        != extract(hour from l.local_forecast_datetime)
