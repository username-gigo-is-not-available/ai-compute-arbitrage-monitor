{{ config(
    materialized = 'incremental',
    unique_key = 'valid_from'
) }}

with source as (
    select *
    from read_parquet('C:\\Users\\grigo\\PycharmProjects\\gpu_ai_compute_arbitrage_monitor\\data\\silver\\seeds\\electricity_tariff_prices\\*.parquet')
),

renamed as (
    select
        cast(tariff_description as varchar)                             as tariff_description,
        cast(price_mkd_per_kwh as double)                               as price_mkd_per_kwh,
        cast(valid_from as date)                                        as valid_from,
        {{ cast_utc('ingested_at') }}                                   as ingested_at,
        {{ cast_utc('processed_at') }}                                  as processed_at
    from source
)

select * from renamed

{% if is_incremental() %}
  where valid_from > (select max(valid_from) from {{ this }})
{% endif %}