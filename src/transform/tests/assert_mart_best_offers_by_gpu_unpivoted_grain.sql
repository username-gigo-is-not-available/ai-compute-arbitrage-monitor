-- Test: mart_best_offers_by_gpu must have one row per (GPU variant, tariff tier) at a given valid_from
-- This test verifies the offer×tier grain of the mart. The expected tier count is NOT hardcoded —
-- it is derived from dim_electricity_tariff_tiers as of each group's valid_from, so EVM block/tariff
-- structural changes update the test automatically.
-- If any group's count != the expected count, the test returns those rows.

with row_counts as (
    select
        offer_type,
        gpu_architecture,
        gpu_model_name,
        gpu_memory_gb,
        valid_from,
        count(*) as tier_count
    from {{ ref('mart_best_offers_by_gpu') }}
    group by offer_type, gpu_architecture, gpu_model_name, gpu_memory_gb, valid_from
)

-- Note: the outer row_counts.valid_from is passed QUALIFIED so the scalar subquery in the macro
-- correlates against the outer group's valid_from instead of shadowing over the inner tiers table.
select *
from row_counts
where tier_count != {{ expected_tariff_tier_count('row_counts.valid_from') }}