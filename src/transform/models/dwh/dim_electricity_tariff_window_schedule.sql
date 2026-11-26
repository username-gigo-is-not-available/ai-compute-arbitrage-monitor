{{ config(
    tags         = ['electricity_tariff_window_schedule'],
    materialized = 'table'
) }}
with source as (
    select * from {{ ref('int_electricity_tariff_window_schedule') }}
),

versioned as (
    {{ with_snapshot_valid_to('source') }}
),

scd as (
    select
        {{ dbt_utils.generate_surrogate_key(['day_of_week', 'hour', 'valid_from']) }} as skey,
        day_of_week,
        hour,
        tariff_window_type,
        valid_from,
        valid_to,
        valid_to = timestamp '9999-12-31' as is_latest
    from versioned
)

select * from scd
