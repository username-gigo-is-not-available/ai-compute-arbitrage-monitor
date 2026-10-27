# ADR 025: One cost: what running the GPU adds to the bill

## Status

Accepted. Supersedes the marginal/average split of ADR-021 (and of ADR-005 through it). The
tariff-block model of ADR-021 is unchanged.

## Context

ADR-021 gave `fct_compute_offers` and the market marts two cost sets:

- `marginal_*`: kWh x (tariff + distribution fee), plus VAT for households.
- `average_*`: marginal plus the monthly access fee spread over 730 hours.

Ranking, best-offer selection and the forecast already used marginal only, because the access
fee is paid whether or not the GPU runs, so it cannot change the decision to run it. The average
set was carried for context and doubled every cost and profit column (19 columns across the fact
and five marts, plus their tests).

The difference is small. On the 2026-10-21 census the household access fee is 200 MKD/month,
0.27 MKD/hr, about $0.006/hr with VAT; the business access fee is 0. The median household
machine costs $0.122/hr to run.

## Decision

- One cost set, named without a prefix: `cost_usd_per_hr`, `profit_usd_per_hr`,
  `cost_per_tflop_usd`, `profit_per_tflop_usd`, `cost_per_gpu_usd_per_hr`,
  `profit_per_gpu_usd_per_hr`, `pct_profitable`, and in `mart_profitability_trends`
  `avg/min/max_profit_usd_per_hr` and `avg/min/max_profit_per_tflop_usd`.
- Cost is the former marginal cost: kWh x (tariff + distribution fee), VAT for households,
  converted from MKD. The `cost_usd_per_hr` macro no longer takes an access fee.
- `fct_compute_offers` drops `access_fee`. `dim_electricity_tariff_fees` keeps it.

## Consequences

- **Positive**:
  - Half the cost and profit columns; the name says what the number is.
  - Every mart answers one question: does running the GPU pay?
- **Negative**:
  - A host who wants the whole bill (to compare with a hosting fee, say) adds the access fee from
    `dim_electricity_tariff_fees` themselves.
  - The column renames need `dbt build --full-refresh -s fct_compute_offers+`; ADR-024 makes that
    safe for the history.
