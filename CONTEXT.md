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

**Valid from / Valid to**:
SCD Type 2 timestamps. `valid_from` is the snapshot/ingest timestamp; `valid_to` is `9999-12-31` for the current version.
_Avoid_: Effective date, expiry, as-of