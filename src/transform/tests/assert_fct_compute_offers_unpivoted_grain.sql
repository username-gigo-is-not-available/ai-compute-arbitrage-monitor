-- Test: fct_compute_offers must have one row per (machine, valid_from, ACTIVE tariff tier).
-- The unpivoted grain of the fact table: a census collapses to one row per machine (ADR-020),
-- and the fact is fanned out across every tariff tier active at valid_from. The expected tier count is NOT
-- hardcoded — it is derived from dim_electricity_tariff_tiers as of each snapshot's valid_from, so the test
-- survives EVN tariff regime changes (new blocks / new tariff structure) without manual updates.
-- If any group's count diverges from the table-derived expected count, the test returns those rows.

with row_counts as (
    select
        machine_id,
        valid_from,
        count(*) as tier_count
    from {{ ref('fct_compute_offers') }}
    group by machine_id, valid_from
)

-- Note: the outer row_counts.valid_from is passed QUALIFIED so the scalar subquery in the macro
-- correlates against the outer group's valid_from instead of shadowing over the inner tiers table.
select *
from row_counts
where tier_count != {{ expected_tariff_tier_count('row_counts.valid_from') }}
