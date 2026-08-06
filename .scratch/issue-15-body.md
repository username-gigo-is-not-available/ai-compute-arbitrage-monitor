## What to build

Implement the electricity tariff blocks dimension as a separate SCD Type 2 table.

## Details

- Uncomment electricity_tariff_blocks in src/transform/models/staging/sources.yaml
- Add stg_electricity_tariff_blocks staging model with typing and validation
- Add int_electricity_tariff_blocks intermediate model
- Add dim_electricity_tariff_blocks DWH model with SCD Type 2 logic (grain: consumer_category, tariff_window_type, tariff_block_number, valid_from)
- Columns: skey, consumer_category, tariff_window_type, tariff_block_number, lower_bound_kwh, upper_bound_kwh, valid_from, valid_to, is_latest
- Add schema tests (unique_combination_of_columns, not_null checks)

## Scope

Part of spec #12 (Electricity Tariff Fees and Blocks Modeling). This is a vertical slice focused on the blocks dimension only.