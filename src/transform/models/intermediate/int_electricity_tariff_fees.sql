{{
    config(
        tags = ['electricity_tariff_fees']
    )
}}
with source as (
    select * from {{ ref('stg_electricity_tariff_fees') }}
),

transformed as (
    select
        consumer_category,
        label,
        metric,
        value,
        {{ extract_fee_type('label') }} as fee_type,
        valid_from,
        ingested_at,
        processed_at
    from source
)

select * from transformed