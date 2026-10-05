# ADR 020: Compute offers come from an on-demand, range-partitioned market census

## Status

Accepted; not yet implemented. Supersedes ADR-008. Amends ADR-019 (retry rule for
`compute_offers`).

## Context

Vast.ai changed `/bundles` after the pipeline was built. About two months ago one
unfiltered request per offer type returned the whole market (~11k rows per hourly ingest
across three types). Probes on 2026-10-01 and 2026-10-03 found:

- **512 rows per request, silently.** `limit` above 512 returns 512, with no error
  (`limit: 10000` in `settings.yaml`). Vast.ai support confirmed on Discord that the cap is
  by design: the API returns "a set of offers matching your search filters rather than a
  list of all available offers".
- **The 512 are a random sample.** Two identical on-demand requests shared 159 of 512 offers.
  `order` sorts the sample, not the market, so sorted (keyset) pagination cannot enumerate it.
- **A daily quota of 20,000 returned rows per key** (`429 search_quota_exceeded`, not in the
  docs). The current hourly ingest asks for 3 × 512 × 24 = 36,864 rows/day, so it fails
  every day.
- **The three offer types repeat the same offers.** For the same offer, the bid row's price
  equals the on-demand row's `min_bid`, and the reserved row's price equals on-demand (no
  `duration` is sent). Bid and reserved requests add rows, not information.
- **One machine is listed as several overlapping slices** (e.g. 1, 2 and 5 GPUs of the same
  5-GPU machine, each with its own `offer_id`), and a request returns a varying subset of
  slices even under the cap. Across 483 multi-slice machines the per-GPU base price was
  identical on every slice (0 exceptions), and `num_gpus / gpu_frac` gave the same machine
  size on every slice of 500 machines (0 exceptions).
- **82% of returned offers were not rentable** (`rentable = false`); `rented` was false on
  all rows (it refers to the caller's own rentals).
- Filters on `ask_contract_id` and `dph_total` ranges work; the `id` filter does not.

## Decision

- **A snapshot is a census**: every machine on the on-demand market, not a sample. Range
  filters make this possible: a request that returns fewer than 512 rows is complete for its
  range, so the market is fetched in `ask_contract_id` ranges, split until each returns under
  the cap.
- **On-demand only.** The bid price is kept from `min_bid`; reserved is not collected.
  `offer_type` leaves the key (supersedes ADR-008).
- **Bronze and Silver stay at offer level**, keyed `(offer_id, snapshot_at)`, and keep
  `gpu_ids` and `gpu_frac` so slices can be re-derived without re-fetching.
- **dbt collapses to one row per machine**: per-GPU price, machine size from
  `num_gpus / gpu_frac`, and whether it is available or taken. No free-GPU count: which
  slices come back is random, so it cannot be known reliably.
- **Available and taken machines are both kept** and marked; the marts may report price by
  availability and the share of each GPU model that is taken.
- **All or nothing** (amends ADR-019): each range is stored as it is fetched; a retry fetches
  only the missing ranges; a snapshot is published to Silver only once every range is in.
  If it cannot finish before the next scheduled census, it is skipped.
- **Frequency comes from the measured census cost**: the smallest interval dividing 24 h for
  which scheduled censuses use at most ~half the daily quota, leaving room for retries and
  development. Expected 1–2 censuses per day. A higher quota is to be requested from Vast.ai
  support, as its rate-limit docs invite.
  _Measured 2026-10-05_: the first complete census found 13,069 offers on 7,357 machines and
  used 13,581 rows (65 requests). That exceeds half the quota, so `compute_offers` runs
  `@daily`, at 00:00 UTC when the quota resets. Across the full market, no machine's offers
  disagreed on machine size or per-GPU price (3,294 machines with several slices); 28% of
  machines had an available offer.

## Considered options

- **Keep hourly, accept a sample**: every count and share in the marts would carry sampling
  noise, and snapshot-to-snapshot changes would be partly noise.
- **Partition by GPU model**: complete only if every model is known in advance, which the
  sampled API cannot guarantee.
- **Census over all three offer types**: about 3× the rows for no extra prices; with an ~11k
  market it fits the quota at most once a day with no room to retry.
- **Several API keys to multiply the quota**: rejected as quota evasion.

## Consequences

- Offer snapshots are no longer hourly. Hourly variation still comes from the EVN tariff
  schedule; `mart_profitability_trends` becomes profit by hour of day at the latest market
  price, as `mart_gpu_forecast` already does.
- `fct_compute_offers` and the marts move from offer grain to machine grain;
  `offer_count`-style metrics now count machines (or GPUs), not overlapping slices.
- The per-GPU-price and machine-size findings come from samples; the census should flag any
  machine whose slices disagree, instead of assuming they never do.
- Probing the API spends the same daily quota as the pipeline (a probe session on 2026-10-01
  exhausted it for ~11 h).
