{{ config(
    materialized = 'incremental',
    unique_key = ['consumer_category', 'label', 'valid_from'],
    tags = ['electricity_tariff_fees']
) }}

with source as (
    select *
    from {{ source('seeds', 'electricity_tariff_fees') }}
),

renamed as (
    select
        cast(consumer_category as string) as consumer_category,
        cast(label as string) as label,
        cast(metric as string) as metric,
        cast(value as float64) as value,
        cast(valid_from as date) as valid_from,
        {{ cast_utc('ingested_at') }} as ingested_at,
        {{ cast_utc('processed_at') }} as processed_at
    from source
)

select *
from renamed

{% if is_incremental() %}
    -- >= re-merges the current version, so a same-date EVN correction updates it (#132)
    where valid_from >= (select max(valid_from) from {{ this }})
{% endif %}
