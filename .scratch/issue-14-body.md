## What to build

Implement the electricity tariff fees dimension as a separate SCD Type 2 table.

## Details

- Uncomment electricity_tariff_fees in src/transform/models/staging/sources.yaml
- Add stg_electricity_tariff_fees staging model with typing and validation
- Add int_electricity_tariff_fees intermediate model with fee_type (distribution/access) assignment via extract_fee_type macro
- Add dim_electricity_tariff_fees DWH model with SCD Type 2 logic (grain: consumer_category, fee_type, valid_from)
- Long format: one row per fee per consumer category
- Add schema tests (unique_combination_of_columns, accepted_values on fee_type, not_null checks)

## Scope

Part of spec #12 (Electricity Tariff Fees and Blocks Modeling). This is a vertical slice focused on the fees dimension only.