-- Test: each (offer_id, ingested_at) snapshot must carry between 1 and N distinct offer_types,
-- where N is the cardinality of the OfferType enum (on_demand / bid / reserved).
--
-- The Vast.ai API is queried separately per offer_type and can return the same offer_id under
-- multiple offer_type values at a single ingested_at with different prices (verified empirically:
-- 236 such keys in bronze across 11 ingests). With offer_type part of the snapshot grain, a
-- (offer_id, ingested_at) key legitimately has 1-N distinct offer_types.
--
-- The range is derived from the offer_types() macro (which mirrors src/common/enums.py
-- OfferType enum) — NOT a hardcoded 1-3. The macro returns the canonical enum list and the
-- upper bound is its cardinality, so if Vast.ai ever adds a fourth offer_type, this test
-- updates automatically and the accepted_values test in the schemas stays in sync.
--
-- Returns any (offer_id, ingested_at) whose distinct-type count is outside [1, N].

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
where n_offer_types < 1
   or n_offer_types > {{ offer_types() | length }}