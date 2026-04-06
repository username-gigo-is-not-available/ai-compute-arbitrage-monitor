with source as (
    select * from {{ ref('int_exchange_rates') }}
),

detected_changes as (
    select
        *,
        lag(value) over (
            partition by from_currency, to_currency
            order by timestamp
        ) as prev_value
    from source
),

changes_only as (
    select * from detected_changes
    where value != prev_value
       or prev_value is null
),

scd as (
    select
        {{ dbt_utils.generate_surrogate_key(['from_currency', 'to_currency', 'value', 'timestamp']) }} as skey,
        from_currency,
        to_currency,
        value,
        inverse_value,
        cast(timestamp as date)                                                         as valid_from,
        coalesce(
            cast({{ valid_to('timestamp', 'from_currency, to_currency') }} as date),
            '9999-12-31'::date
        )                                                                               as valid_to,
        {{ is_latest('timestamp', 'from_currency, to_currency') }}                     as is_latest
    from changes_only
)

select * from scd
