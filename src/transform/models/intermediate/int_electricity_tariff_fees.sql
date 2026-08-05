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
        case
            when label = 'Активна ел. енергија' then 'distribution'
            when label = 'Надоместок за пристап на електродистрибутивниот систем' then 'access'
        end as fee_type,
        valid_from,
        ingested_at,
        processed_at
    from source
)

select * from transformed