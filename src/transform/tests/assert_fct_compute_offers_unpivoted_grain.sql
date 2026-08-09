-- Test: fct_compute_offers should have exactly 7 rows per offer snapshot (one per active tariff tier)
-- This test verifies the unpivoted grain of the fact table.
-- It counts the number of rows per (offer_id, valid_from) and asserts that each group has exactly 7 rows.
-- If any group has a count != 7, the test will return those rows, indicating a failure.

with row_counts as (
    select
        offer_id,
        valid_from,
        count(*) as tier_count
    from {{ ref('fct_compute_offers') }}
    group by offer_id, valid_from
)

select *
from row_counts
where tier_count != 7
