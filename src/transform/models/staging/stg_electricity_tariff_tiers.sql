{{ config(
    materialized = 'incremental',
    unique_key = 'valid_from',
    tags = ['electricity_tariff_tiers']
) }}

with source as (
    select *
    from {{ source('seeds', 'electricity_tariff_tiers') }}
),

renamed as (
    select
        cast(tariff_description as string)                             as tariff_description,
        cast(price_mkd_per_kwh as float64)                             as price_mkd_per_kwh,
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