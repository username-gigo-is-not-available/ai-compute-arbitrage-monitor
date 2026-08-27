-- Test: every tariff tier's non-null tariff_block_number must be DEFINED in dim_electricity_tariff_blocks
-- as of the SAME point in time the tier applies (SCD as-of semantics), not merely in today's block set.
--
-- This replaces the former hardcoded `tariff_block_number in (1, 2, 3, 4)` expression tests in the
-- schema.yml files. Both the tiers and the blocks are SCD Type 2 dimensions, so the correct check is
-- temporal: a tier's block number is valid iff a block row exists with the same
-- (consumer_category, tariff_window_type, tariff_block_number) whose [valid_from, valid_to) range
-- also contains the tier's own valid_from. If EVN changes the block structure across regimes, this
-- test adapts automatically because the expected set is derived from the blocks dimension via the
-- expected_tariff_block_numbers(as_of) macro, anchored at each tier's valid_from.
--
-- NOT EXISTS is used (rather than a derived-table join) because BigQuery cannot correlate a FROM-table
-- to an outer row; a correlated EXISTS performs the as-of membership check per tier cleanly.

select *
from {{ ref('dim_electricity_tariff_tiers') }} as t
where t.tariff_block_number is not null
  and not exists {{ expected_tariff_block_numbers('t.valid_from') }}