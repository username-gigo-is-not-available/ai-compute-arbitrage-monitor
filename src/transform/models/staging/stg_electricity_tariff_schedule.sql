{{ config(
    materialized = 'incremental',
    unique_key = 'valid_from',
    tags = ['electricity_tariff_schedule']
) }}

with source as (
    select *
    from {{ source('seeds', 'electricity_tariff_schedule') }}
),

renamed as (
    select
        cast(tariff_type as string)                                    as tariff_type,
        cast(day_of_week as int64)                                     as day_of_week,
        cast(start_hour as int64)                                      as start_hour,
        cast(end_hour as int64)                                        as end_hour,
        cast(valid_from as date)                                       as valid_from,
        {{ cast_utc('ingested_at') }}                                  as ingested_at,
        {{ cast_utc('processed_at') }}                                 as processed_at
    from source
)

select * from renamed

{% if is_incremental() %}
  where valid_from > (select max(valid_from) from {{ this }})
{% endif %}