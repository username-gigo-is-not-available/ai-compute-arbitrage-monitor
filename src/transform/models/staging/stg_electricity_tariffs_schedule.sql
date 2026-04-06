{{ config(
    materialized = 'incremental',
    unique_key = 'valid_from'
) }}

with source as (
    select *
    from read_parquet('C:\\Users\\grigo\\PycharmProjects\\gpu_ai_compute_arbitrage_monitor\\data\\silver\\seeds\\electricity_tariffs_schedule\\*.parquet')
),

renamed as (
    select
        cast(tariff_type as varchar)                                    as tariff_type,
        cast(day_of_week as integer)                                    as day_of_week,
        cast(start_hour as integer)                                     as start_hour,
        cast(end_hour as integer)                                       as end_hour,
        cast(valid_from as date)                                        as valid_from,
        {{ cast_utc('ingested_at') }}                                   as ingested_at,
        {{ cast_utc('processed_at') }}                                  as processed_at
    from source
)

select * from renamed

{% if is_incremental() %}
  where valid_from > (select max(valid_from) from {{ this }})
{% endif %}