-- Test: mart_arbitrage_opportunities must have one row per (machine snapshot, tariff tier) (ADR-020)
-- This test verifies the offer×tier grain of the mart. The expected tier count is NOT hardcoded —
-- it is derived from dim_electricity_tariff_tiers as of each group's valid_from, so EVN tariff
-- restructures (new blocks / new tariff sets) update the expectation automatically.
-- If any group's count != expected, the test returns those rows.

with row_counts as (
    select
        machine_id,
        valid_from,
        count(*) as tier_count
    from {{ ref('mart_arbitrage_opportunities') }}
    group by machine_id, valid_from
)

-- Note: the outer row_counts.valid_from is passed QUALIFIED so the scalar subquery in the macro
-- correlates against the outer group's valid_from instead of shadowing over the inner tiers table.
select *
from row_counts
where tier_count != {{ expected_tariff_tier_count('row_counts.valid_from') }}