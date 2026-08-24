-- Test: each (offer_id, ingested_at) snapshot must carry between 1 and 3 distinct offer_types.
--
-- The Vast.ai API is queried separately per offer_type and can return the same offer_id under
-- 2-3 offer_type values at a single ingested_at with different prices (verified empirically:
-- 236 such keys in bronze across 11 ingests). With offer_type part of the snapshot grain, a
-- (offer_id, ingested_at) key legitimately has 1-3 distinct offer_types.
--
-- At least 1: every snapshot has an offer_type (also enforced by not_null on offer_type).
-- At most 3: OfferType only defines on_demand / bid / reserved, so a count > 3 means a stray
-- value from the API that the accepted_values [on_demand, bid, reserved] test will also flag.
--
-- Returns any (offer_id, ingested_at) whose distinct-type count is outside [1, 3].

with type_counts as (
    select
        offer_id,
        ingested_at,
        count(distinct offer_type) as n_offer_types
    from {{ ref('stg_compute_offers') }}
    group by offer_id, ingested_at
)

select *
from type_counts
where n_offer_types not between 1 and 3