## What to build

Integrate the new fees and blocks dimensions into the fact table.

## Details

- Update fct_compute_offers to join to dim_electricity_tariff_fees (distribution and access fees)
- Update fct_compute_offers to join to dim_electricity_tariff_blocks (lower_bound_kwh and upper_bound_kwh)
- Update cost_usd_per_hr calculation to: (tariff_value + distribution_fee) * total_system_kwh_per_hr / usd_to_mkd_rate + access_fee / 730 / usd_to_mkd_rate
- Keep tariff_block_number sourced from dim_electricity_tariff_tiers (no circular dependency)
- Update schema.yml for fct_compute_offers with new columns and tests

## Scope

Part of spec #12 (Electricity Tariff Fees and Blocks Modeling). This is a vertical slice focused on the fact table integration.