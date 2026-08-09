# AI Compute Arbitrage Monitor

A dbt project that ingests Vast.ai GPU compute offers, joins them with Macedonian EVN electricity tariff tiers and time-of-use schedules, and produces mart-level profitability analytics for arbitrage opportunities.

## Language

**Offer**:
A Vast.ai GPU compute listing — a (host, machine) pair that can be rented. Identified by `offer_id`.
_Avoid_: Instance, node, machine (these conflate the listing with the physical host)

**Offer type**:
The pricing modality of an offer — `on_demand`, `bid`, or `reserved`. Not part of the offer's identity; an offer has one type per snapshot.
_Avoid_: Pricing mode, plan

**Offer snapshot**:
A point-in-time capture of an offer's specs and pricing, keyed by `(offer_id, ingested_at)`. Each ingest produces one snapshot per offer.
_Avoid_: Record, row, version

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
SCD Type 2 timestamps. `valid_from` is the snapshot/ingest timestamp; `valid_to` is `9999-12-31` for the current version.
_Avoid_: Effective date, expiry, as-of

**Host**:
The economic actor this project models — someone who owns GPUs, places them in Macedonia at EVN electricity rates, and rents them out on Vast.ai at the global market price. The cost model is the host's electricity; the revenue is the rental price.
_Avoid_: Renter, consumer, buyer

**Market-benchmarking**:
The practice of treating competitors' Vast.ai ask prices as a proxy for what the host can charge for a similar GPU. The marts rank market offers to answer "which GPU config is worth hosting," not to model the host's own listings.
_Avoid_: Own-inventory, validated-demand

**Electricity-only cost model**:
The deliberate scope of the cost model — it covers only the electricity to run the GPU (TDP-based), not hardware capex, maintenance, or placement fees. The host is assumed to already own the GPU; this is not a break-even calculator.
_Avoid_: Total-cost, ROI, break-even

**Forecast**:
A forward-looking, per-machine, tariff-window-aware projection of electricity cost and profit over a specific future time window (e.g., "my GTX 3080 over this weekend"). Distinct from market-scanning, which ranks current offers.
_Avoid_: Scenario, projection, estimate
