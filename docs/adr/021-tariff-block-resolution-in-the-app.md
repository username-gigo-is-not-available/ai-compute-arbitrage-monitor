# ADR 021: Tariff blocks are resolved per user in the app; marts carry every block

## Status

Accepted. The dbt part is implemented in #42; the app part (Track B) is not yet built. Amends ADR-007 (forecast inputs, grain and framing) and the cost
model of ADR-005 (marginal vs. average cost, VAT). Restores the intent of ADR-005's
`kwh_consumed_so_far` input without restoring `mart_user_profitability_scenario`.

## Context

`mart_gpu_forecast` always priced high-tariff hours at block 1
(`qualify row_number() ... order by tariff_block_number nulls first`), and it took the consumer
category and the host's ask price as dbt vars. A table built with vars serves one value per build,
so the planned hero view, whose input is the host's own kWh, had no data behind it. The
`mart_user_profitability_scenario` that ADR-005 built for that input was removed by ADR-007 as
"renter framing". That was a mistake: the host pays their own household EVN bill, so their
consumption is a host-side input.

How EVN prices household electricity ([tariff system](https://www.evn.mk/AboutInvoices/TariffSystem.aspx?lang=en-gb),
[invoices](https://evn.mk/Invoices.aspx?lang=en-gb), checked 2026-10-06/07):

- **Blocks are progressive**, like tax brackets: each high-tariff kWh is priced by the block it
  falls into. Only household high-tariff kWh count toward blocks. Low-tariff and business tiers
  have no blocks.
- **Blocks follow the household's meter-reading period, not the calendar month.** Reading dates
  differ per household. The published bounds (0–210, 211–630, 631–1050, 1051+ kWh) are for
  30 days and scale with the period's length (7 / 21 / 35 kWh per day).
- **Block 4 costs about 4× block 1.** A host running GPUs 24/7 reaches it, so defaulting to block 1
  understates cost the most for exactly the host the project models.
- **Published prices exclude VAT.** VAT is added on the bill. The household rate is 18% (checked
  on a real bill).
- **Two charges don't depend on consumption:** the access fee, and the municipal public-lighting
  tax (a flat per-meter amount equal to 25 kWh a month for households, under the Law on Communal
  Fees).

## Decision

- **Resolve the block in the app (Django), per request, not in dbt.** The user enters:
  - their high-tariff kWh since the last reading (the **billing-period consumption**)
  - the last and next reading dates
  - their GPU count (default 1)
  - optionally, their own ask price

  The app scales the block bounds to the period's length and finds the block. That block gives the
  **marginal tariff rate**.
- **The marts carry every block, and the app filters.**
  - `mart_gpu_forecast` drops all three per-user vars (the block-1 filter, `forecast_consumer_category`
    and `forecast_ask_price_usd_per_hr`).
  - Its grain becomes GPU model × hour × consumer category × tariff tier, with per-GPU values.
  - `forecast_days` becomes 32, to cover a 30-day window plus the gap between rebuilds.
  - The live market view filters `mart_arbitrage_opportunities` on the `tariff_tier_skey` that the
    current hour's tariff window and the user's block resolve to.
- **The forecast moves through blocks as the GPU consumes.**
  - The window is at most today + 30 days, so it crosses at most one meter reading.
  - The app keeps a running total that starts at the billing-period consumption and adds only the
    GPU's high-tariff kWh, hour by hour. Each hour is priced at the block reached at the start of
    that hour.
  - At the next reading date the total resets to 0, and the following period uses 30-day bounds.
- **Hosts are shown marginal cost.** Replace `cost_usd_per_hr` and the profit and per-TFLOP columns
  derived from it with two explicit sets:
  - **`marginal_*`:** (tariff + distribution fee) × kWh, plus VAT for households.
  - **`average_*`:** marginal plus the access fee spread over 730 hours, with VAT on the access fee too
    (VAT applies to the whole bill).

  `fct_compute_offers` and the market marts carry both. Market ranking uses marginal profit per
  TFLOP. The forecast carries marginal only.
- **VAT (18%) applies to household costs only**, to every charge it covers on the bill (energy,
  distribution and access fee). A VAT-registered business reclaims it.
- **The forecast's hours are converted to `Europe/Skopje`** before joining the EVN time-of-use
  schedule. The schedule's hours are local, and the spine was UTC.

## Considered Options

- **Build the block (or kWh) as a dbt var.** Rejected: one build serves one user.
- **Bring back `mart_user_profitability_scenario`.** Rejected for the same reason. Its input was
  also a build-time var.
- **A blended monthly rate from expected monthly kWh.** Rejected: hosts decide whether to run the
  GPU now, which is a marginal decision. A blended rate also cannot be expressed as one
  `tariff_tier_skey`.
- **Assume the billing period follows the calendar month.** Rejected: reading dates differ per
  household.

## Consequences

Known simplifications, accepted for the thesis:

- **Household usage other than the GPU isn't counted** in the running total, so block crossings
  come later than in reality. Asking for a household baseline is future work.
- **100% utilization:** full TDP every hour, as if always rented.
- **Tariffs are frozen at today's values** for the whole window (`is_latest`).
- **Business users get the household time-of-use schedule**, which has no consumer category.
- **Blocks are crossed on hour boundaries,** not mid-hour.

The forecast table grows by about 5× in rows (2 categories × up to 4 blocks), which is still small.
Profitability numbers fall for heavy users, which is the point.
