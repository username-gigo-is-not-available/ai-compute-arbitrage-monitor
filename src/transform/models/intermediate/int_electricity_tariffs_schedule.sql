with recursive calendar AS (
    select 1 as day_of_week, 0 as hour
    union all
    select
        case
            when hour = 23
            then day_of_week + 1
            else day_of_week
        end,
        case
            when hour = 23
            then 0
            else hour + 1
        end
    from calendar
    where not (day_of_week = 7 and hour = 23)
),
source as (
    select
        *
    from {{ ref('stg_electricity_tariffs_schedule') }}
)
select
    c.day_of_week,
    c.hour,
    s.tariff_type,
    s.valid_from,
    s.ingested_at,
    s.processed_at
from calendar as c
join source as s on
    s.day_of_week = c.day_of_week
    and s.start_hour <= c.hour
    and s.end_hour > c.hour
order by c.day_of_week, c.hour


