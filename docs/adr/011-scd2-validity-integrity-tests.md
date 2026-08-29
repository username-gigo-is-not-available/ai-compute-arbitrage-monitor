# ADR 011: SCD Type 2 Validity Integrity Tests (`assert_scd2_*`)

## Status

Accepted

## Context

Six SCD Type 2 tables live in `dwh` (`fct_compute_offers`, `dim_exchange_rates`,
`dim_electricity_tariff_tiers`, `dim_electricity_tariff_fees`,
`dim_electricity_tariff_blocks`, `dim_electricity_tariff_window_schedule`). The
existing schema-level tests for these tables only assert:

- natural key + `valid_from` uniqueness (`dbt_utils.unique_combination_of_columns`),
- `valid_from < valid_to` (`dbt_utils.expression_is_true`).

They cannot detect the two SCD Type 2 integrity violations that actually break
as-of lookups and "current version" semantics downstream:

1. **Double active** — more than one row sharing the same natural key whose
   `valid_to` is the active sentinel (`date '9999-12-31'` for the DATE dims,
   `timestamp '9999-12-31'` for `fct_compute_offers`).
2. **Overlapping ranges** — any two rows for the same natural key whose
   `[valid_from, valid_to)` intervals overlap, active or not.

We wanted one reusable, macro-based mechanism parameterized by natural key
columns (applied via `schema.yml` per model) rather than six near-duplicate
SQL files, so it stays consistent as tables are added.

## Decision

Add two generic dbt tests built from parametrized macros, applied at the model
level in `models/dwh/schema.yml` for every SCD table:

- `assert_scd2_double_active(model, natural_key_columns, active_value, ...)`
  — returns the offending **pairs** of active rows per natural key. A row is
  active iff `valid_to = active_value` (the table's own sentinel literal, passed
  explicitly per table so DATE vs TIMESTAMP stays type-correct).
- `assert_scd2_no_overlapping_ranges(model, natural_key_columns, ...)`
  — returns the offending **pairs** of rows per natural key whose half-open
  intervals overlap.

Both return pairs (`a`/`b` with `a` strictly before `b` by `valid_from`) shaped
as the natural key + both `(valid_from, valid_to)` pairs, so the run log shows
the real shape of every violation. Natural key comparison uses
`is not distinct from` so nullable key members (`tariff_block_number` in tiers)
are handled.

**Interval semantics — half-open `[valid_from, valid_to)`.** Two rows overlap
iff `a.valid_from < b.valid_to AND b.valid_from < a.valid_to` (the SQL standard's
`OVERLAPS`). Adjacent ranges (one ends exactly where the next starts) are legal.
This matches the pipeline's own as-of joins (`valid_from <= date < valid_to` in
`expected_tariff_tier_count`, `expected_tariff_block_numbers`, and the ADR-010
fee/block joins) and how `valid_to` is derived (`valid_to = next valid_from - 1
day` in `valid_to.sql` for the dims; `fct`'s post-hook closes with
`valid_to = next snapshot's valid_from`).

**`fct_compute_offers` natural key is `(offer_id, offer_type, tariff_tier_skey)`,
not `offer_id` alone.** Per ADR-008, the fact grain is
`(offer_id, valid_from, offer_type, tariff_tier_skey)`, and a single offer can be
legitimately present under multiple offer types and multiple active tariff tiers
at the same instant. Keying the SCD check on `offer_id` alone would flag that
legitimate simultaneity as a violation.

**Conventions.** Two macro files, two macros (repo convention: one macro per
file), named `test_assert_scd2_*` so the YAML keys are `assert_scd2_*` (the
repo's existing assertion-test naming style).

## Consequences

- **Positive**:
  - One reusable mechanism covers every SCD table; adding a new table = two
    `schema.yml` entries (natural key columns + sentinel literal), zero SQL.
  - Detection is per-pair and self-describing: a failure returns the offending
    natural key with both validity intervals, so the shape of the violation is
    visible without extra queries.
  - Catches the two classes that the existing uniqueness/ordering tests cannot.
- **Negative**:
  - The tests are self-join based (pair enumeration). On large tables
    (`fct_compute_offers` already ~1M rows) this is a pairwise scan within each
    natural-key partition; partition sizes here are small, so the cost is
    bounded by the per-key version count, not the table size.
- **Initial run (2026-08-28) — detection only, no fixes**: all five dimensions
  pass both tests. `fct_compute_offers` fails both: 14,546 double-active pairs
  across 399 offers (1,211 keys with 2 open rows, 1,078 with 3, 1,036 with 4,
  up to 7), and 19,642 overlapping pairs (14,546 are the same double-active
  pairs; 5,096 involve closed rows whose `valid_to` overruns a later row's
  `valid_from`). The shape indicates the fact's incremental post-hook does not
  close superseded open rows when an offer persists across snapshot ingests —
  diagnosing and fixing that is a separate follow-up.