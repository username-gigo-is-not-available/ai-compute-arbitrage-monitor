with source as (
    select * from {{ ref('int_electricity_tariffs_schedule') }}
),

scd as (
    select
        {{ dbt_utils.generate_surrogate_key(['day_of_week', 'hour', 'valid_from']) }} as skey,
        day_of_week,
        hour,
        tariff_type,
        valid_from,
        coalesce(
            {{ valid_to('valid_from', 'day_of_week, hour') }},
            '9999-12-31'::date
        )                                                                               as valid_to,
        {{ is_latest('valid_from', 'day_of_week, hour') }}                             as is_latest
    from source
)

select * from scd
