-- Test: mart_gpu_history prices each census at every local hour of its census date, with every machine (#40).
--   1. Each census has one row set per local hour of its date: 24 hours, or 23/25 on a DST change day.
--   2. Each (census, hour, tier) counts every machine of the census exactly once - no filters, nothing dropped.
--   3. Each hour's window is the EVN schedule's window at that local hour, recomputed with datetime() independently
--      of the evn_* macros (ADR-021).
-- If any census, hour or row breaks these rules, it will be returned.

with census_machines as (
    select
        valid_from                  as census_at,
        count(distinct machine_id)  as expected_count
    from {{ ref('fct_compute_offers') }}
    group by valid_from
),

-- Starts from the fact's censuses, so a census missing from the mart fails with 0 hours.
hour_counts as (
    select
        c.census_at,
        cast(null as timestamp)        as history_hour,
        'hour count'                   as failure,
        count(distinct h.history_hour) as actual_count,
        any_value(timestamp_diff(
            timestamp(date_add({{ evn_date('c.census_at') }}, interval 1 day), '{{ var("evn_timezone") }}'),
            timestamp({{ evn_date('c.census_at') }}, '{{ var("evn_timezone") }}'),
            hour
        ))                             as expected_count
    from census_machines c
    left join {{ ref('mart_gpu_history') }} h
        on h.census_at = c.census_at
    group by c.census_at
),

machine_counts as (
    select
        h.census_at,
        h.history_hour,
        'machine count'             as failure,
        sum(h.number_of_machines)   as actual_count,
        any_value(c.expected_count) as expected_count
    from {{ ref('mart_gpu_history') }} h
    join census_machines c
        on c.census_at = h.census_at
    group by h.census_at, h.history_hour, h.tariff_tier_skey
),

local_hours as (
    select distinct
        census_at,
        history_hour,
        scheduled_tariff_window_type,
        datetime(history_hour, '{{ var("evn_timezone") }}') as local_history_datetime
    from {{ ref('mart_gpu_history') }}
),

window_mismatches as (
    select
        l.census_at,
        l.history_hour,
        'scheduled window'          as failure,
        null                        as actual_count,
        null                        as expected_count
    from local_hours l
    left join {{ ref('dim_electricity_tariff_window_schedule') }} s
        on  s.day_of_week = mod(extract(dayofweek from l.local_history_datetime) + 5, 7) + 1
        and s.hour        = extract(hour from l.local_history_datetime)
        and date(l.local_history_datetime) >= s.valid_from
        and date(l.local_history_datetime) <  s.valid_to
    where s.tariff_window_type is null
       or s.tariff_window_type != l.scheduled_tariff_window_type
)

select * from hour_counts where actual_count != expected_count
union all
select * from machine_counts where actual_count != expected_count
union all
select * from window_mismatches
