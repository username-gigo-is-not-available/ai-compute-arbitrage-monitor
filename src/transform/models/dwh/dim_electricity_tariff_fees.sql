{{ config(
    tags         = ['electricity_tariff_fees'],
    materialized = 'table'
) }}
with source as (
    select * from {{ ref('int_electricity_tariff_fees') }}
),

scd as (
    select
        {{ dbt_utils.generate_surrogate_key(['consumer_category', 'fee_type', 'valid_from']) }} as skey,
        consumer_category,
        fee_type,
        label,
        metric,
        value,
        valid_from,
        coalesce(
            cast({{ valid_to('valid_from', 'consumer_category, fee_type') }} as date),
            date '9999-12-31'
)                                                                                                       as valid_to,
        {{ is_latest('valid_from', 'consumer_category, fee_type') }}         as is_latest
    from source
)

select * from scd