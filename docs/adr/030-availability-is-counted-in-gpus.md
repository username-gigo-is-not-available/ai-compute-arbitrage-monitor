# ADR 030: Availability is counted in GPUs

## Status

Accepted. Changes the meaning of Available / Taken in CONTEXT.md. Follows ADR-020 and ADR-026. Closes #133.

## Context

`int_compute_offers` collapses a machine's offers into one row (ADR-020) and set
`rentable_flag = logical_or(rentable_flag)`: a machine was available when any of its offers could be rented now.
A partly rented machine (an 8× 4090 whose 1-GPU slice is rented while the 4-GPU half is free) therefore counted as
one whole available machine.

Measured on the 6 censuses 2026-10-05 to 11-15, over the machines that survive `single_type_machines`:

- 181–643 machines per census were partly rented: 25–29% of the machines counted as available.
- A partly rented machine had on average 36–39% of its GPUs free.
- Every offer in Silver carries `gpu_ids`. On ~95% of machines the offers' ids cover every GPU; on ~5% some GPUs
  are listed in no offer.

On the 11-15 census, counting machines instead of GPUs overstated `pct_available` by 1–12 points (RTX 5060 Ti
41.2% vs 28.7%, RTX 3060 28.6% vs 19.2%). Moving partly rented machines into the taken median as well moved it by
at most ~4% (RTX 5070 Ti $0.206 → $0.214 per GPU).

## Decision

- A **GPU** is free when an offer that can be rented now lists it in `gpu_ids`. `int_compute_offers` replaces
  `rentable_flag` with `number_of_free_gpus`, the distinct ids of the machine's rentable offers.
- GPUs that no offer lists count as taken: nobody can rent them now. The denominator is `number_of_machine_gpus`.
- A machine is available when `number_of_free_gpus > 0` and taken when `number_of_free_gpus <
  number_of_machine_gpus`. A partly rented machine counts in **both** medians: its free GPUs compete at its price,
  and its taken GPUs are paid that price. Every offer of a machine has the same per-GPU price (ADR-020), so its price
  is evidence for both.
- `pct_available` = `sum(number_of_free_gpus) / sum(number_of_machine_gpus)`.
- `mart_gpu_machines` shows `number_of_free_gpus` ("3 of 8 free").

## Considered Options

- **Keep `logical_or` and document it.** Cheapest; the medians barely move. Rejected: `pct_available` was off by
  up to 12 points, and "available" on a mostly rented machine misleads a host reading `mart_gpu_machines`.
- **Fix only `pct_available`, keep the machine-level flag for the medians.** Smaller change. Rejected for one
  definition of availability across all marts.
- **Keep `rentable_flag` next to the count.** Rejected: two columns that can disagree, and `true` would still mean
  "at least one GPU free".
- **Leave unlisted GPUs out of the denominator.** Rejected: a machine listing 4 of its 8 GPUs, all free, would
  look 100% available.

## Consequences

- **Positive**:
  - `pct_available` is the share of GPUs a renter could take now.
  - A partly rented machine's price informs both the competition and the price being paid.
- **Negative**:
  - The available and taken medians no longer split the machines: a machine can be in both.
  - Adding `gpu_ids` to the incremental `stg_compute_offers` needed a one-time
    `dbt build --full-refresh --select stg_compute_offers+`, or old censuses would read 0 free GPUs.
