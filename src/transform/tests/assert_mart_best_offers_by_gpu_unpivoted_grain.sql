-- mart_best_offers_by_gpu must have one row per (GPU variant, tariff tier) at a given valid_from
-- The expected tier count is NOT hardcoded — it is derived from dim_electricity_tariff_tiers as of each group's
-- valid_from, so EVN block/tariff structural changes update the test automatically.

with row_counts as (
    select
        gpu_architecture,
        gpu_model_name,
        gpu_memory_gb,
        valid_from,
        count(*) as tier_count
    from {{ ref('mart_best_offers_by_gpu') }}
    group by gpu_architecture, gpu_model_name, gpu_memory_gb, valid_from
)

-- Note: the outer row_counts.valid_from is passed QUALIFIED so the scalar subquery in the macro
-- correlates against the outer group's valid_from instead of shadowing over the inner tiers table.
select *
from row_counts
where tier_count != {{ expected_tariff_tier_count('row_counts.valid_from') }}
