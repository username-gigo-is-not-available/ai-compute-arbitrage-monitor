# 05 — Create ADR-004: scheduled_tariff_window_type naming

**What to build:** Create an ADR in `docs/adr/004-scheduled-tariff-window-type-naming.md`
to record the decision to rename `tariff_window_type` → `scheduled_tariff_window_type` in
`mart_profitability_trends` only. The rename distinguishes the tier's own window
(`tariff_window_type` in `fct_compute_offers` and the other marts) from the effective
window at a given hour (the tier's window intersected with the EVN schedule, present only
in `mart_profitability_trends` after the inner join).

**Blocked by:** 02 — Fix mart_profitability_trends: inner join, column rename, schema updates

**Status:** ready-for-agent

- [ ] ADR-004 created in `docs/adr/`
- [ ] Context, decision, and consequences documented
- [ ] ADR references the `CONTEXT.md` glossary terms
