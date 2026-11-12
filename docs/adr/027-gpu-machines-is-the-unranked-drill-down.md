# ADR 027: The machines mart is an unranked drill-down

## Status

Accepted. Replaces `mart_arbitrage_opportunities` with `mart_gpu_machines`. Follows ADR-026.

## Context

`mart_arbitrage_opportunities` ranked single listings: one row per machine and tariff tier, available and
verified machines only, ranked by profit per TFLOP. On the 2026-11-03 census its #1 was an RTX 4090 asking
$40/hr and its #2 an RTX 3070 at $3.99/hr, about ten times the usual ask. Ranking single listings by profit
floats unsold overpriced asks to the top, the same flaw ADR-026 removed from `mart_best_offers_by_gpu`.

It answered a renter's question (which offer is the best deal), while the project's actor is the host.
`mart_gpu_profitability` now answers the host's question per GPU variant.

## Decision

- `mart_gpu_machines`: one row per current machine, the machines behind each `mart_gpu_profitability` row,
  so a host can see what machines like theirs (GPU, reliability, CPU, network, country) ask.
- Every current machine counts: available and taken, verified, unverified and deverified, as in every other
  mart.
- No tier and no cost or profit: nothing about a machine depends on the tariff, and the electricity cost is
  cents next to the ask. Money per tier is in `mart_gpu_profitability`.
- No rank.
- The ask is named `ask_usd_per_hr` and `ask_per_gpu_usd_per_hr`, like the marts' `market_ask_*`.

## Consequences

- **Positive**:
  - A host can compare their own machine with machines like it, which no per-GPU mart can show.
  - One row per machine instead of seven.
- **Negative**:
  - Profit per machine is no longer in a mart; it stays in `fct_compute_offers`.
  - The old BigQuery table `mart_arbitrage_opportunities` is not removed by dbt and must be dropped by hand.
