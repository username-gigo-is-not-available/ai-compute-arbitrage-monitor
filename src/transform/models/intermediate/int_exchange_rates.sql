with source as (
    select * from {{ ref('stg_exchange_rates') }}
),

transformed as (
    select
        from_currency,
        to_currency,
        value,
        {{ calculate_inverse('value') }} as inverse_value,
        timestamp,
        ingested_at,
        processed_at
    from source
)

select * from transformed