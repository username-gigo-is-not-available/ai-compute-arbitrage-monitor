-- Test: Recompute fct_compute_offers' costs from their inputs (ADR-021).
--   marginal = kWh x (tariff + distribution fee) x (1 + VAT) / USD->MKD rate
--   average  = (kWh x (tariff + distribution fee) + access fee / 730) x (1 + VAT) / USD->MKD rate
-- VAT applies to households only: a VAT-registered business reclaims it.
-- If any row's cost differs from the recomputed one, it will be returned.

with expected as (
    select
        machine_id,
        valid_from,
        tariff_tier_skey,
        consumer_category,
        vat_rate,
        marginal_cost_usd_per_hr,
        average_cost_usd_per_hr,
        case when consumer_category = 'household' then {{ var('household_vat_rate') }} else 0 end as expected_vat_rate,
        total_system_kwh_per_hr * (tariff_value + coalesce(distribution_fee, 0))                 as energy_mkd_per_hr,
        coalesce(access_fee, 0) / 730.0                                                         as access_fee_mkd_per_hr,
        usd_to_mkd_rate
    from {{ ref('fct_compute_offers') }}
)

select *
from expected
where vat_rate != expected_vat_rate
   or abs(marginal_cost_usd_per_hr
          - energy_mkd_per_hr * (1 + expected_vat_rate) / nullif(usd_to_mkd_rate, 0)) > 1e-9
   or abs(average_cost_usd_per_hr
          - (energy_mkd_per_hr + access_fee_mkd_per_hr) * (1 + expected_vat_rate) / nullif(usd_to_mkd_rate, 0)) > 1e-9
