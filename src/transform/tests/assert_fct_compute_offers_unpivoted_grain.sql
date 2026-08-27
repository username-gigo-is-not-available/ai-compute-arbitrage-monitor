-- Test: fct_compute_offers must have one row per (offer snapshot, offer type, ACTIVE tariff tier).
-- The unpivoted grain of the fact table: an offer snapshot is keyed by (offer_id, ingested_at, offer_type),
-- and the fact is fanned out across every tariff tier active at valid_from. The expected tier count is NOT
-- hardcoded — it is derived from dim_electricity_tariff_tiers as of each snapshot's valid_from, so the test
-- survives EVN tariff regime changes (new blocks / new tariff structure) without manual updates.
-- If any group's count diverges from the table-derived expected count, the test returns those rows.

with row_counts as (
    select
        offer_id,
        valid_from,
        offer_type,
        count(*) as tier_count
    from {{ ref('fct_compute_offers') }}
    group by offer_id, valid_from, offer_type
)

-- Note: the outer row_counts.valid_from is passed QUALIFIED so the scalar subquery in the macro
-- correlates against the outer group's valid_from instead of shadowing over the inner tiers table.
select *
from row_counts
where tier_count != {{ expected_tariff_tier_count('row_counts.valid_from') }}
