# ADR 003: Unpivot `fct_compute_offers` by Tariff Tier

## Status

Accepted — **Amended by ADR-008**

> **Note (ADR-008):** The fact-table grain documented here as
> `(offer_id, valid_from, tariff_tier_skey)` is **superseded**. Empirical data shows the same
> Vast.ai `offer_id` can be listed under 2–3 `offer_type`s at one `ingested_at` with different
> prices, so `offer_type` is part of the offer snapshot identity. The current grain is
> **`(offer_id, valid_from, offer_type, tariff_tier_skey)`** and the dbt `unique_key` is
> `['offer_id', 'valid_from', 'offer_type', 'tariff_tier_skey']`. See ADR-008 for context.

## Context

The `fct_compute_offers` fact table previously stored electricity cost and profit metrics in a "pivoted" format, with separate columns for each of the 7 EVN tariff scenarios (e.g., `cost_household_1_high_usd_per_hr`, `profit_business_low_usd_per_hr`). This resulted in a very wide table with over 20 redundant cost/profit columns, making it difficult to query for aggregate analysis across tiers and hard to maintain if new tariff tiers were introduced.

## Decision

We will unpivot the `fct_compute_offers` table so that each row represents a single (offer snapshot, tariff tier) combination. This means:

- The grain of the fact table changes from `(offer_id, valid_from)` to `(offer_id, valid_from, tariff_tier_skey)`.
- The 14+ pivoted cost/profit columns are replaced with generic columns: `cost_usd_per_hr`, `profit_usd_per_hr`, `cost_per_tflop_usd`, and `profit_per_tflop_usd`.
- A foreign key `tariff_tier_skey` links to the `dim_electricity_tariff_tiers` dimension, which provides the context for the specific tariff tier (e.g., `consumer_category`, `tariff_label`, `tariff_value`).
- The `unique_key` in the dbt model configuration is updated to `['offer_id', 'valid_from', 'tariff_tier_skey']`.

This change will result in 7 rows per offer snapshot (one for each active tariff tier), making the table deeper but significantly narrower and easier to analyze.

## Consequences

- **Positive**:
  - Simplified querying for cross-tier analysis (e.g., finding the most profitable tier for a given offer).
  - Easier to extend if new tariff tiers are added in the future.
  - Reduced column count, improving schema clarity.
- **Negative**:
  - The row count of the fact table will increase by approximately 7x.
  - Downstream models (e.g., `mart_arbitrage_opportunities`) will need to be updated to handle the unpivoted structure.
  - BigQuery partitioning and clustering strategies remain effective, but query costs may increase slightly due to the larger row count.
