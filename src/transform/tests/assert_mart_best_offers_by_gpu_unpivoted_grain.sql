-- Test: mart_best_offers_by_gpu should have exactly 7 rows per GPU variant (one per active tariff tier)
-- This test verifies the offer×tier grain of the mart.
-- It counts the number of rows per (offer_type, gpu_architecture, gpu_model_name, gpu_memory_gb) and asserts that each group has exactly 7 rows.
-- If any group has a count != 7, the test will return those rows, indicating a failure.

with row_counts as (
    select
        offer_type,
        gpu_architecture,
        gpu_model_name,
        gpu_memory_gb,
        count(*) as tier_count
    from {{ ref('mart_best_offers_by_gpu') }}
    group by offer_type, gpu_architecture, gpu_model_name, gpu_memory_gb
)

select *
from row_counts
where tier_count != 7