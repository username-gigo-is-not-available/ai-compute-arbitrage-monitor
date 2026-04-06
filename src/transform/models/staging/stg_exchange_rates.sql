{{ config(
    materialized = 'incremental',
    unique_key = 'timestamp'
) }}

with source as (
    select *
    from read_parquet('C:\\Users\\grigo\\PycharmProjects\\gpu_ai_compute_arbitrage_monitor\\data\\silver\\sources\\exchange_rates\\*.parquet')
),

renamed as (
    select
        cast(from_currency as varchar)                                  as from_currency,
        cast(to_currency as varchar)                                    as to_currency,
        cast(value as double)                                           as value,
        {{ cast_utc('timestamp') }}                                     as timestamp,
        {{ cast_utc('ingested_at') }}                                   as ingested_at,
        {{ cast_utc('processed_at') }}                                  as processed_at
    from source
)

select * from renamed

{% if is_incremental() %}
  where timestamp > (select max(timestamp) from {{ this }})
{% endif %}