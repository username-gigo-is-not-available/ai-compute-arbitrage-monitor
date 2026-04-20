{{
    config(
        tags = ['electricity_tariff_prices']
    )
}}
with source as (
    select * from {{ ref('stg_electricity_tariff_prices') }}
),

transformed as (
    select
        {{ extract_tariff_type('tariff_description') }}         as tariff_type,
        {{ translate_tariff_description('tariff_description') }} as tariff_description_en,
        tariff_description                                      as tariff_description_mk,
        price_mkd_per_kwh,
        valid_from,
        ingested_at,
        processed_at
    from source
)

select * from transformed