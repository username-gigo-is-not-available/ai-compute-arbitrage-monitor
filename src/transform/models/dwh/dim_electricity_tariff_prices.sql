{{ config(
    tags         = ['electricity_tariff_prices'],
    materialized = 'table'
) }}
with source as (
    select * from {{ ref('int_electricity_tariff_prices') }}
),

scd as (
    select
        {{ dbt_utils.generate_surrogate_key(['tariff_description_mk', 'valid_from']) }} as skey,
        tariff_type,
        tariff_description_en,
        tariff_description_mk,
        price_mkd_per_kwh,
        valid_from,
        coalesce(
            cast({{ valid_to('valid_from', 'tariff_description_mk') }} as date),
            date '9999-12-31'
        )                                                               as valid_to,
        {{ is_latest('valid_from', 'tariff_description_mk') }}         as is_latest
    from source
)

select * from scd