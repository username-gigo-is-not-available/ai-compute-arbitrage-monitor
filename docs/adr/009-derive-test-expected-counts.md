# ADR 009: Derive test expected counts from the source tables (no magic numbers)

The assertion tests hardcoded several counts that are really properties of EVN's tariff structure, so any EVN change
new tariff blocks, new tier sets, or a new offer_type would silently break the tests and require manual triple-edits.
We decided that every count that is *derivable at compile time* from a table should be derived from that table via a
dbt macro, used consistently in both the singular `assert_*.sql` tests and the `schema.yml` expression/accepted-values
tests, so a single source of truth (the seeded dimension tables and enum) drives every expectation.

- **Tariff tier count** — `expected_tariff_tier_count(as_of)` macro returns a correlated scalar subquery over
  `dim_electricity_tariff_tiers` counting tiers active as of a per-row `as_of` date (SCD `valid_from <= as_of < valid_to`).
  Used by `assert_fct_compute_offers_unpivoted_grain`, `assert_mart_arbitrage_opportunities`, and
  `assert_mart_best_offers_by_gpu_unpivoted_grain` in place of the hardcoded `!= 7`. As-of-date semantics keep the
  expectation valid for historical offer snapshots even after EVN publishes a new annual tariff set.
- **Tariff block set** — `expected_tariff_block_numbers(as_of_expr, subject_alias='t')` macro mirrors the tier-count
  macro: it returns a correlated `EXISTS` subquery over `dim_electricity_tariff_blocks` filtered by the same
  SCD range predicates (`valid_from <= as_of < valid_to`), so a tier's block number is required to exist *as of the
  tier's own `valid_from`* — not merely in the current block set. This avoids a temporally misaligned check when EVN
  changes block structure across regimes. The former hardcoded `tariff_block_number in (1,2,3,4)` / `<= 4` expression
  tests were removed from `staging/schema.yaml`, `intermediate/schema.yml`, and `dwh/schema.yml`, and replaced by the
  singular test `assert_tariff_tiers_blocks_consistency`, which uses `NOT EXISTS {{ expected_tariff_block_numbers('t.valid_from') }}`
  (a correlated `NOT EXISTS` rather than a derived-table join because BigQuery cannot correlate a FROM-table to an outer
  row). The `tariff_blocks_limit` config in `src/config/apis/evn.py` is the only remaining literal and is now annotated
  to note it is the scrape cap whose runtime truth is the blocks dimension.
- **Offer-type cardinality** — `offer_types()` macro returns the canonical enum list `['on_demand','bid','reserved']`
  (mirroring `src/common/enums.py OfferType`). It sources the `accepted_values` in all three schema layers and the
  upper bound in `assert_stg_compute_offers_offer_type_snapshot_grain` (via `{{ offer_types() | length }}`), replacing
  the hardcoded `1..3`.

For the two table-backed counts the derivation is always computed from the same tables being asserted against, so the
tests self-heal when EVN's published structure changes. The `offer_type` enum has no warehouse table, so its canonical
list lives in the macro and is kept in sync with the Python enum.