{{
    config(
        tags = ['electricity_tariff_schedule']
    )
}}
with calendar as (
    select
        day_of_week,
        hour
    from
        unnest(generate_array(1, 7)) as day_of_week,
        unnest(generate_array(0, 23)) as hour
),

source as (
    select *
    from {{ ref('stg_electricity_tariff_schedule') }}
)

select
    c.day_of_week,
    c.hour,
    s.tariff_type,
    s.valid_from,
    s.ingested_at,
    s.processed_at
from calendar as c
join source as s
    on  s.day_of_week = c.day_of_week
    and s.start_hour <= c.hour
    and s.end_hour > c.hour
order by c.day_of_week, c.hour