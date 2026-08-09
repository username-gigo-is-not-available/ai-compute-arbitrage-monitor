{{
    config(
        tags = ['electricity_tariff_blocks']
    )
}}
with source as (
    select * from {{ ref('stg_electricity_tariff_blocks') }}
),

transformed as (
    select
        consumer_category,
        tariff_window_type,
        tariff_block_number,
        lower_bound_kwh,
        upper_bound_kwh,
        valid_from,
        ingested_at,
        processed_at
    from source
)

select * from transformed