{{ config(
    tags         = ['electricity_tariff_blocks'],
    materialized = 'table'
) }}
with source as (
    select * from {{ ref('int_electricity_tariff_blocks') }}
),

versioned as (
    {{ with_snapshot_valid_to('source') }}
),

scd as (
    select
        {{ dbt_utils.generate_surrogate_key(
            ['consumer_category', 'tariff_window_type', 'tariff_block_number', 'valid_from']
        ) }}
            as skey,
        consumer_category,
        tariff_window_type,
        tariff_block_number,
        lower_bound_kwh,
        upper_bound_kwh,
        valid_from,
        valid_to,
        valid_to = timestamp '9999-12-31' as is_latest
    from versioned
)

select * from scd
