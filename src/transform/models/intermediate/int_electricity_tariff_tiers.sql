{{
    config(
        tags = ['electricity_tariff_tiers']
    )
}}
with source as (
    select * from {{ ref('stg_electricity_tariff_tiers') }}
),

transformed as (
    select
        consumer_category,
        label,
        metric,
        value,
        {{ extract_tariff_window_type('tariff_tier') }} as tariff_window_type,
        {{ extract_tariff_block_number('tariff_tier') }} as tariff_block_number,
        valid_from,
        ingested_at,
        processed_at
    from source
)

select * from transformed