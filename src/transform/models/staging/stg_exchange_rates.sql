{{ config(
    materialized = 'incremental',
    unique_key = 'timestamp',
    tags = ['exchange_rates']
) }}

with source as (
    select *
    from {{ source('sources', 'exchange_rates') }}
),

renamed as (
    select
        cast(from_currency as string)                                  as from_currency,
        cast(to_currency as string)                                    as to_currency,
        cast(value as float64)                                         as value,
        {{ cast_utc('timestamp') }}                                    as timestamp,
        {{ cast_utc('ingested_at') }}                                  as ingested_at,
        {{ cast_utc('processed_at') }}                                 as processed_at
    from source
)

select * from renamed

{% if is_incremental() %}
  where timestamp > (select max(timestamp) from {{ this }})
{% endif %}