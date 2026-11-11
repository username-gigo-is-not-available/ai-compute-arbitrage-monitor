# ADR 026: GPU profitability is the typical machine, not the best listing

## Status

Accepted. Replaces `mart_best_offers_by_gpu` with `mart_gpu_profitability`.

## Context

The marts answer one question for a host: which GPU is worth hosting. `mart_best_offers_by_gpu`
answered it with one machine per GPU variant and tier: the newest, then most profitable, of the
available, verified machines. On 2026-10-21 that put a 4090 at $40/hr on top, a listing nobody
rents.

On the 2026-11-03 census, 35 of 123 GPU variants rest on fewer than 5 machines, and 3 of them are
in the top 10 by median profit per GPU; #1 is a single RTX 4090D 48GB at $12.92 per GPU.

## Decision

- `mart_gpu_profitability`: one row per GPU variant (model + memory) and tariff tier over the
  latest census, with medians across its machines: price (all, available, taken), cost, profit per
  GPU, profit per TFLOP, TFLOPS and TDP.
- Every current machine counts: available and taken, verified, unverified and deverified. A new
  host starts unverified, and both sides of the market set the price.
- `number_of_machines` sits beside the medians.
- No rank in the mart. Ordering, and how to treat a median from few machines (a minimum count,
  shrinkage toward the overall median, a confidence bound), is left to the reader: the API.
- A machine with 0 W or a $0 ask is dropped in `int_compute_offers`; a 0 TFLOPS is no value in the
  TFLOPS median, but the machine still counts for price and profit.

## Consequences

- **Positive**:
  - The price is what the variant's machines typically ask, so one overpriced listing cannot set it.
  - The split shows the gap between what the competition asks and what taken machines charge.
- **Negative**:
  - Sorted naively by median profit, a variant with one machine can still come first; readers must
    use `number_of_machines`.
  - The old BigQuery table `mart_best_offers_by_gpu` is not removed by dbt and must be dropped by
    hand.
