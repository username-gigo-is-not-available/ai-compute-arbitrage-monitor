# ADR 006: Tariff Type Classification and Block Null-Tolerance

## Status

Accepted

## Context

The `int_electricity_tariff_tiers` model derives a `tariff_type` column from the Macedonian `label` via the `extract_tariff_type` macro, which returns `'energy'` when the label is 'Активна електрична енергија' (active electrical energy) and `NULL` for all other components. This column was produced by the SQL but was never declared in the schemas, carried into `dim_electricity_tariff_tiers`, or referenced by `fct_compute_offers` — yet the fact table's join expected `tt.tariff_type`, which was a runtime-breaking bug (the dimension did not have the column).

Separately, `tariff_block_number` is legitimately `NULL` for low tariff (HT) and undifferentiated high tariff (business BT) tiers, which have no consumption-block distinction — only household high tariff (BT1–BT4) has blocks. The schemas nevertheless asserted strict `accepted_values: [1,2,3,4]`, which fails on the legitimate `NULL` rows.

The question was two-fold: (a) should `tariff_type` be a first-class column carried through the modeling chain, and (b) should `tariff_block_number` tests tolerate `NULL`?

## Decision

**`tariff_type` is carried through the full chain** (intermediate → dimension → fact) as a first-class classification column:

- `tariff_type` is declared in the `int_electricity_tariff_tiers` and `dim_electricity_tariff_tiers` schemas.
- `dim_electricity_tariff_tiers` selects it from the intermediate layer.
- `fct_compute_offers` exposes it (renamed from the previous `tariff_label` naming) for consistency with its source.
- Semantics: `'energy'` for active electrical energy consumption, `NULL` for components not classified. Tests are NULL-tolerant (`tariff_type is null or tariff_type = 'energy'`) because `NULL` is a meaningful state, not an anomaly.

**`tariff_block_number` tests are NULL-tolerant** in the intermediate tiers, dimension tiers, and fact schemas: `tariff_block_number is null or tariff_block_number in (1, 2, 3, 4)`. NULL is a legitimate, documented state for low/business tiers with no block distinction; when non-NULL, the value must be 1–4.

## Consequences

- **Positive**:
  - Fixes the runtime bug in `fct_compute_offers` (the `tt.tariff_type` reference now resolves).
  - Declares `tariff_type` end-to-end, so the derived classification is documented, tested, and available to downstream marts.
  - The NULL-tolerant `tariff_block_number` tests now reflect the real data semantics instead of failing on legitimate NULL rows.
  - Consistent naming: `tariff_type` in intermediate, dimension, and fact (the previous `tariff_label` name is gone, avoiding confusion with `label`, which is the Macedonian component name).

- **Negative**:
  - `dim_electricity_tariff_tiers` and `fct_compute_offers` carry one additional column.
  - The `tariff_type`/`tariff_block_number` NULL-tolerance semantics now span three layers, so reversing them later touches multiple schemas.