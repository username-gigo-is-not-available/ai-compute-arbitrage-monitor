{{ config(
    materialized = 'incremental',
    unique_key = 'valid_from',
    tags = ['electricity_tariff_blocks']
) }}

with source as (
    select *
    from {{ source('seeds', 'electricity_tariff_blocks') }}
),

renamed as (
    select
        cast(consumer_category as string)                              as consumer_category,
        cast(tariff_window_type as string)                             as tariff_window_type,
        cast(tariff_block_number as int64)                             as tariff_block_number,
        cast(lower_bound_kwh as int64)                                 as lower_bound_kwh,
        cast(upper_bound_kwh as int64)                                 as upper_bound_kwh,
        cast(valid_from as date)                                       as valid_from,
        {{ cast_utc('ingested_at') }}                                  as ingested_at,
        {{ cast_utc('processed_at') }}                                 as processed_at
    from source
)

select *
from renamed {% if is_incremental() %}
where valid_from
    > (select max (valid_from) from {{ this }})
    {% endif %}