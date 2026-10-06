# AI Compute Arbitrage Monitor

A dbt project that ingests Vast.ai GPU compute offers, joins them with Macedonian EVN electricity tariff tiers and time-of-use schedules, and produces mart-level profitability analytics for arbitrage opportunities.

## Language

**Machine**:
A physical GPU server listed on Vast.ai, identified by `machine_id`. The unit of market-benchmarking: every offer on a machine has the same per-GPU price, and its **machine size** (total GPU count, rented or not) is known from any one of its offers (ADR-020).
_Avoid_: Node, instance, box

**Offer**:
A single Vast.ai rental listing — one slice of a machine's GPUs, at the machine's price. Identified by `offer_id`. A machine is usually listed as several overlapping offers (e.g. 1, 2 and all of its GPUs), so offers count listings, not GPUs.
_Avoid_: Instance, node, machine (these conflate the listing with the physical server)

**Slice**:
The specific set of a machine's GPUs that one offer rents. Slices of the same machine overlap; renting one makes the overlapping ones disappear.
_Avoid_: Bundle, partition

**Offer type**:
The pricing modality of an offer — `on_demand`, `bid`, or `reserved`. Only on-demand offers are collected: the bid price is the on-demand offer's minimum bid, and reserved prices equal on-demand prices unless a rental duration is given (ADR-020).
_Avoid_: Pricing mode, plan

**Census**:
One complete sweep of the Vast.ai on-demand market in which every listed machine appears, as opposed to a sample. A census is all-or-nothing: an incomplete one is never published (ADR-020).
_Avoid_: Scrape, pull, fetch, sample

**Offer snapshot**:
The market as of a scheduled hour, built from one complete census, one row per offer keyed by `(offer_id, snapshot_at)`. `snapshot_at` is the ingest run's scheduled time floored to the hour (ADR-019), not the fetch moment — that is `ingested_at`. `int_compute_offers` maps `snapshot_at` to `valid_from`.
_Avoid_: Record, row, version

**Available / Taken**:
Whether an offer can be rented right now (Vast.ai's `rentable` flag). **Taken** means not rentable now — usually rented by someone, possibly switched off by its owner; the two cannot be told apart.
_Avoid_: Rented (Vast.ai's `rented` flag means rented by the API caller, not by anyone)

**Tariff tier**:
A specific EVN electricity price configuration — a combination of `consumer_category`, `tariff_window_type`, and `tariff_block_number` — valid for a date range. Versioned as SCD Type 2 with `tariff_tier_skey`.
_Avoid_: Tariff, rate, price tier

**Tariff window type**:
The low/high time-of-use window of a tariff tier (`low` or `high`). This is a property of the tier itself, not of the time of day.
_Avoid_: Scheduled window, TOU window

**Scheduled tariff window type**:
The effective tariff window at a specific hour, determined by the intersection of a tariff tier's `tariff_window_type` and the EVN time-of-use schedule (`dim_electricity_tariff_window_schedule`). Only present in `mart_profitability_trends`, where the join filters to hours where the tier's window matches the schedule.
_Avoid_: Scheduled window, effective window, live window

**Tariff tier skey**:
Surrogate key for a tariff tier version in `dim_electricity_tariff_tiers`. Used as a foreign key in `fct_compute_offers` and as a grain column in the marts.
_Avoid_: Tier id, tier key

**Tariff fee**:
A charge component in the EVN electricity bill. Two types: `distribution` (per-kWh surcharge) and `access` (fixed monthly charge). Stored in `dim_electricity_tariff_fees` with `fee_type` column.
_Avoid_: Tariff charge, fee

**Tariff block**:
A consumption boundary range (in kWh/month) that determines which per-kWh price tier applies. Stored in `dim_electricity_tariff_blocks` with `lower_bound_kwh` and `upper_bound_kwh`.
_Avoid_: Consumption tier, usage band

**Valid from / Valid to**:
SCD Type 2 validity endpoints. Provenance is per-entity: for the four EVN tariff
tables `valid_from` is a date parsed from the source text during refine
(`refine/assets/extraction.py:extract_valid_from_date`, `dd.mm.yyyy` →
`to_date`); it is not the snapshot or ingest timestamp. `valid_to` is
`9999-12-31` for the current version. Validity ranges are half-open
`[valid_from, valid_to)` per the **Validity range** entry.
_Avoid_: Effective date, expiry, as-of

**Validity range**:
A version's half-open interval `[valid_from, valid_to)` — it covers `valid_from <= t < valid_to`. Adjacent
versions never overlap and the boundary instant belongs to exactly one version. The SCD Type 2 integrity
invariant is contiguity per natural key: at most one active version (no double active) and no overlapping
ranges, enforced by the `assert_scd2_double_active` and `assert_scd2_no_overlapping_ranges` tests.
_Avoid_: Closed interval, active period, effective window

**Host**:
The economic actor this project models — someone who owns GPUs, places them in Macedonia at EVN electricity rates, and rents them out on Vast.ai at the global market price. The cost model is the host's electricity; the revenue is the rental price.
_Avoid_: Renter, consumer, buyer

**Market-benchmarking**:
The practice of treating competitors' Vast.ai machines as a proxy for what the host can charge for a similar GPU. Taken machines show prices that are being paid; available machines show the competition. The marts compare machines to answer "which GPU config is worth hosting," not to model the host's own listings.
_Avoid_: Own-inventory, confirmed demand (a taken machine may be offline, not rented)

**Electricity-only cost model**:
The deliberate scope of the cost model — it covers only the electricity to run the GPU (TDP-based), not hardware capex, maintenance, or placement fees. The host is assumed to already own the GPU; this is not a break-even calculator.
_Avoid_: Total-cost, ROI, break-even

**Forecast**:
A forward-looking, per-machine, tariff-window-aware projection of electricity cost and profit over a specific future time window (e.g., "my GTX 3080 over this weekend"). Distinct from market-scanning, which ranks current offers.
_Avoid_: Scenario, projection, estimate
