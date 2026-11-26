{{ config(
    tags         = ['electricity_tariff_fees'],
    materialized = 'table'
) }}
with source as (
    select * from {{ ref('int_electricity_tariff_fees') }}
),

versioned as (
    {{ with_snapshot_valid_to('source') }}
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
        valid_to,
        valid_to = timestamp '9999-12-31' as is_latest
    from versioned
)

select * from scd
