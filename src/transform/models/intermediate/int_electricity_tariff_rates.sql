{#- Ephemeral: dbt pastes it into each model that refs it, so it has no table, no dbt tag and no DAG of its own. -#}
{{ config(materialized = 'ephemeral') }}

-- What one kWh costs under each tariff tier: one row per tier per period in which its price, distribution fee and
-- block bounds all stay the same, so a change to any of the three starts a new row. fct_compute_offers takes the
-- row in force at each census; mart_gpu_forecast takes the current one (valid_to = 9999-12-31).
with tariff_tiers as (
    select
        skey   as tariff_tier_skey,
        consumer_category,
        tariff_type,
        metric as tariff_metric,
        value  as tariff_value,
        tariff_window_type,
        tariff_block_number,
        valid_from,
        valid_to
    from {{ ref('dim_electricity_tariff_tiers') }}
),

tariff_fees as (
    select
        consumer_category,
        max(case when fee_type = 'distribution' then value end) as distribution_fee,
        valid_from,
        valid_to
    from {{ ref('dim_electricity_tariff_fees') }}
    group by consumer_category, valid_from, valid_to
),

-- Bounds are for a 30-day billing period; the app scales them to the user's period length (ADR-021).
tariff_blocks as (
    select
        consumer_category,
        tariff_window_type,
        tariff_block_number,
        lower_bound_kwh,
        upper_bound_kwh,
        valid_from,
        valid_to
    from {{ ref('dim_electricity_tariff_blocks') }}
)

select
    tt.tariff_tier_skey,
    tt.consumer_category,
    tt.tariff_type,
    tt.tariff_metric,
    tt.tariff_value,
    tt.tariff_window_type,
    tt.tariff_block_number,
    tf.distribution_fee,
    tb.lower_bound_kwh,
    tb.upper_bound_kwh,
    {{ vat_rate('tt.consumer_category') }}                                             as vat_rate,

    -- The period in which the tier, its fee and its block are all valid. Tiers without a block (low tariff,
    -- business) take the tier's own period.
    greatest(tt.valid_from, tf.valid_from, coalesce(tb.valid_from, tt.valid_from))     as valid_from,
    least(tt.valid_to, tf.valid_to, coalesce(tb.valid_to, tt.valid_to))                as valid_to

from tariff_tiers tt
join tariff_fees tf
    on  tf.consumer_category = tt.consumer_category
    and tf.valid_from < tt.valid_to
    and tt.valid_from < tf.valid_to
left join tariff_blocks tb
    on  tb.consumer_category   = tt.consumer_category
    and tb.tariff_window_type  = tt.tariff_window_type
    and tb.tariff_block_number = tt.tariff_block_number
    and tb.valid_from < tt.valid_to
    and tt.valid_from < tb.valid_to
