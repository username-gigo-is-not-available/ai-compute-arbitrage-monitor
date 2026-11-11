-- Every tariff tier's non-null tariff_block_number must exist in dim_electricity_tariff_blocks as of the tier's own
-- valid_from (both are SCD Type 2), not merely in today's block set, so a change of EVN's block structure needs no
-- update here.
-- NOT EXISTS because BigQuery cannot correlate a FROM-table to an outer row.

select *
from {{ ref('dim_electricity_tariff_tiers') }} as t
where
    t.tariff_block_number is not null
    and not exists {{ expected_tariff_block_numbers('t.valid_from') }}
