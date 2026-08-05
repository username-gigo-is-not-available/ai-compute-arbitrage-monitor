# 02 — Fix mart_profitability_trends: inner join, column rename, schema updates

**What to build:** The `mart_profitability_trends` model currently has a dead `left join`
to `dim_electricity_tariff_window_schedule` — no `ts.*` column is selected, so the join
has zero effect on the output. Convert it to an `inner join` so that only offer rows
whose tariff tier's `tariff_window_type` matches the EVN schedule at that hour are
included (Intent A: "live" information). Rename the output column
`tariff_window_type` → `scheduled_tariff_window_type` to distinguish it from the tier's
own window in `fct_compute_offers`. Update `schema.yml` accordingly: fix the description,
rename the column definition, expand the uniqueness test to include all grain columns
(`consumer_category`, `scheduled_tariff_window_type`, `tariff_block_number`), and ensure
the `accepted_values` test on the renamed column uses error severity.

**Blocked by:** 01 — Revert offer_type grain in stg/fct compute_offers

**Status:** ready-for-agent

- [ ] `left join` → `inner join` on `dim_electricity_tariff_window_schedule`
- [ ] Output column renamed `tariff_window_type` → `scheduled_tariff_window_type`
- [ ] `schema.yml` description updated (no stale "scheduled tariff window" references)
- [ ] `schema.yml` column definition renamed to `scheduled_tariff_window_type`
- [ ] `schema.yml` uniqueness test includes `consumer_category`, `scheduled_tariff_window_type`, `tariff_block_number`
- [ ] `schema.yml` `accepted_values` on `scheduled_tariff_window_type` uses error severity
- [ ] Trailing newline added to `mart_profitability_trends.sql`
- [ ] `dbt run` + `dbt test` pass on `mart_profitability_trends`
