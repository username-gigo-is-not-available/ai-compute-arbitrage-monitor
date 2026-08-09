{{ config(
    tags         = ['electricity_tariff_tiers'],
    materialized = 'table'
) }}
with source as (
    select * from {{ ref('int_electricity_tariff_tiers') }}
),

scd as (
    select
        {{ dbt_utils.generate_surrogate_key(['consumer_category', 'tariff_window_type', 'tariff_block_number', 'valid_from']) }} as skey,
        consumer_category,
        tariff_type,
        label,
        metric,
        value,
        tariff_window_type,
        tariff_block_number,
        valid_from,
        coalesce(
            cast({{ valid_to('valid_from', 'consumer_category, tariff_window_type, tariff_block_number') }} as date),
            date '9999-12-31'
        )                                                                                                   as valid_to,
        {{ is_latest('valid_from', 'consumer_category, tariff_window_type, tariff_block_number') }}         as is_latest
    from source
)

select * from scd