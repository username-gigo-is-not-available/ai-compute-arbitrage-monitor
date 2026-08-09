# Spec: Refactor compute-offers grain and tariff-window semantics

## Problem Statement

The dbt transform layer has two correctness issues that need to be resolved before the
`refactor/evn-seeds` branch can be merged:

1. **`offer_type` was incorrectly added to the grain** of `stg_compute_offers` and
   `fct_compute_offers`. A Vast.ai `offer_id` is unique per `(offer_id, ingested_at)`
   snapshot — confirmed by querying the source and finding zero duplicate rows. Adding
   `offer_type` to the `unique_key` and the SCD Type 2 post-hook correlation was a
   mistaken generalization that would split a single offer's history across type
   partitions and break the SCD close-out logic.

2. **`mart_profitability_trends` has a dead join and a stale column name.** The model
   was refactored to remove the `scheduled_tariff_window` column (the schedule's
   window type) and instead filter to hours where the tariff tier's window matches the
   EVN time-of-use schedule. However, the `left join` to
   `dim_electricity_tariff_window_schedule` was left in place as a no-op (no `ts.*`
   column is selected, and a LEFT join doesn't filter), and the column was renamed
   `tariff_window_type` in the output — which is ambiguous because that name is also
   used in `fct_compute_offers` for the tier's *own* window, not the *effective*
   window.

## Solution

1. **Revert the `offer_type` grain** in `stg_compute_offers.sql` and
   `fct_compute_offers.sql` — restore `unique_key` to `(offer_id, ingested_at)` and
   `(offer_id, valid_from, tariff_tier_skey)` respectively, and restore the post-hook
   to correlate on `offer_id` only. Keep the `NOT EXISTS` pattern (it's more robust
   than `NOT IN` for NULL handling) but drop the `offer_type` correlation.

2. **Fix `mart_profitability_trends`** — convert the `left join` to an `inner join`
   so that only offers whose tariff tier's window matches the EVN schedule at that
   hour are included (Intent A: "live" information). Rename the output column
   `tariff_window_type` → `scheduled_tariff_window_type` to distinguish it from the
   tier's own window in the fact table.

3. **Update `schema.yml`** — fix the `mart_profitability_trends` description, rename
   the column in the schema definition, fix the uniqueness test to include all grain
   columns, and remove `severity: warn` from `tariff_window_type` accepted-values
   tests across all four marts (so invalid values fail, not warn).

4. **Remove stale `target/` build artifacts** that still reference the old
   `scheduled_tariff_window` column.

5. **Create `CONTEXT.md`** at the repo root to pin down the domain vocabulary
   (`offer`, `offer_type`, `tariff_window_type`, `scheduled_tariff_window_type`,
   `tariff_tier_skey`, `valid_from`/`valid_to`).

## User Stories

1. As a data engineer, I want `stg_compute_offers` to use `(offer_id, ingested_at)` as
   its unique key, so that each offer snapshot is deduplicated correctly without
   splitting by offer type.
2. As a data engineer, I want `fct_compute_offers` to use `(offer_id, valid_from,
   tariff_tier_skey)` as its unique key, so that the SCD Type 2 grain matches the
   documented grain in ADR-003.
3. As a data engineer, I want the `fct_compute_offers` post-hook to close out
   SCD rows using `NOT EXISTS` correlated on `offer_id` only, so that offers that
   disappear from a scrape are correctly expired without false positives from
   `NOT IN` NULL semantics.
4. As an analyst, I want `mart_profitability_trends` to only include offer rows where
   the tariff tier's window type matches the EVN schedule at that hour, so that the
   trend chart shows only "live" tariff-window-aligned profitability.
5. As an analyst, I want the effective tariff window column in
   `mart_profitability_trends` to be named `scheduled_tariff_window_type`, so that I
   can distinguish it from the tier's own `tariff_window_type` in the fact table.
6. As a data engineer, I want the `unique_combination_of_columns` test on
   `mart_profitability_trends` to include all grain columns (`consumer_category`,
   `scheduled_tariff_window_type`, `tariff_block_number`), so that duplicate rows are
   caught.
7. As a data engineer, I want `tariff_window_type` accepted-values tests to fail
   (not warn) on invalid values across all marts, so that data quality issues are
   surfaced as errors.
8. As a developer, I want a `CONTEXT.md` glossary, so that domain terms like
   `tariff_window_type` vs `scheduled_tariff_window_type` are unambiguous.
9. As a data engineer, I want stale `target/` build artifacts removed, so that
   compiled tests referencing the old `scheduled_tariff_window` column don't cause
   confusion in logs.
10. As a data engineer, I want the `mart_profitability_trends.sql` file to end with a
    newline, so that git diffs are clean.

## Implementation Decisions

- **`offer_type` is not part of the offer identity.** An `offer_id` identifies a
  (host, machine) pair. The `offer_type` (on_demand, bid, reserved) is a property of
  the snapshot, not a key component. The grain reverts to what ADR-003 documents.
- **Post-hook keeps `NOT EXISTS` but drops `offer_type` correlation.** The `NOT
  EXISTS` pattern is retained as a strict improvement over `NOT IN` (NULL-safe), but
  the correlation is on `offer_id` only, matching the reverted grain.
- **`mart_profitability_trends` uses `inner join` to the schedule.** The join now
  filters to hours where the tier's `tariff_window_type` matches the EVN schedule's
  `tariff_window_type`. This is Intent A — the chart shows only "live"
  tariff-window-aligned data.
- **`scheduled_tariff_window_type` is a mart-only concept.** The rename applies only
  to `mart_profitability_trends`. The other marts (`mart_arbitrage_opportunities`,
  `mart_best_offers_by_gpu`, `mart_gpu_model_summary`) keep `tariff_window_type`
  because they show the tier's own window without schedule filtering.
- **`tariff_tier_skey` is retained in all marts.** The user initially considered
  removing it but decided against it to preserve price-version separation in
  `mart_profitability_trends` (which aggregates historical hours and could otherwise
  merge rows across tariff price changes).
- **ADR-004** is created to record the `scheduled_tariff_window_type` naming
  decision.
- **`CONTEXT.md`** is created at the repo root with the resolved domain vocabulary.

## Testing Decisions

- **Existing seam**: dbt schema tests (`unique_combination_of_columns`, `not_null`,
  `accepted_values`, `dbt_utils.expression_is_true`) in `schema.yml` files. These are
  the primary testing seam for the transform layer.
- **New test**: `accepted_values` on `scheduled_tariff_window_type` in
  `mart_profitability_trends` schema — values `[low, high]`, error severity.
- **Updated test**: `unique_combination_of_columns` on `mart_profitability_trends`
  now includes `consumer_category`, `scheduled_tariff_window_type`, and
  `tariff_block_number` in addition to the existing columns.
- **Updated test**: `accepted_values` on `tariff_window_type` in all four marts now
  uses error severity (removed `severity: warn`).
- **Validation**: `dbt run` + `dbt test` on the full DAG. The `fct_compute_offers`
  post-hook SCD close-out behavior can be validated by checking that
  `valid_to` is correctly set to the latest `valid_from` for offers that disappear
  from a scrape.
- **Prior art**: The existing `dbt_utils.unique_combination_of_columns` tests on
  `mart_arbitrage_opportunities` and `mart_best_offers_by_gpu` (which already include
  `offer_type`) serve as the pattern for the updated uniqueness tests.

## Out of Scope

- Removing `tariff_tier_skey` from the marts (deferred — would collapse price
  versions in `mart_profitability_trends`).
- Renaming `tariff_window_type` in marts other than `mart_profitability_trends`.
- Any changes to the ingestion layer (`src/ingest/`, `src/refine/`).
- Any changes to the Airflow / Terraform infrastructure.
- Adding new GPU spec columns to `mart_gpu_model_summary` (only
  `mart_best_offers_by_gpu` received the bandwidth/CUDA additions).

## Further Notes

- The `round_gpu_mb` macro change (adding `cast(t as int64)` inside the subquery) and
  the `int_compute_offers` simplification (removing the redundant nested cast) are
  correct and require no further changes.
- The `mart_best_offers_by_gpu` additions of `gpu_bandwidth_gbytes_per_sec` and
  `gpu_max_cuda_version_supported` to the SELECT are correct and the schema entries
  are in place.
- ADR-003 (unpivot by tariff tier) remains accurate — the grain reverts to
  `(offer_id, valid_from, tariff_tier_skey)` as documented there.
