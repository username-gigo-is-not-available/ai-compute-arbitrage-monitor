-- Test: fct_compute_offers should have exactly 7 rows per (offer snapshot, offer type) (one per active tariff tier)
-- This test verifies the unpivoted grain of the fact table.
-- An offer snapshot is keyed by (offer_id, ingested_at, offer_type): the same Vast.ai offer_id can be
-- listed under multiple offer_type values (on_demand/bid/reserved) with different prices at one timestamp.
-- It counts the number of rows per (offer_id, valid_from, offer_type) and asserts each group has exactly 7 rows.
-- If any group has a count != 7, the test will return those rows, indicating a failure.

with row_counts as (
    select
        offer_id,
        valid_from,
        offer_type,
        count(*) as tier_count
    from {{ ref('fct_compute_offers') }}
    group by offer_id, valid_from, offer_type
)

select *
from row_counts
where tier_count != 7
