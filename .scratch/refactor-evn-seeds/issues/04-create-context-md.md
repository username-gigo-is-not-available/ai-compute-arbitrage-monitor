# 04 — Create CONTEXT.md domain glossary

**What to build:** Create a `CONTEXT.md` at the repo root to pin down the domain
vocabulary that was previously ambiguous. Key terms to define: `offer` (a Vast.ai GPU
compute listing identified by `offer_id`), `offer_type` (on_demand, bid, reserved — a
property of the snapshot, not the identity), `tariff_window_type` (the tier's own
low/high window from `dim_electricity_tariff_tiers`), `scheduled_tariff_window_type`
(the effective window at a given hour, where the tier's window matches the EVN schedule),
`tariff_tier_skey` (SCD Type 2 surrogate key), and `valid_from`/`valid_to` (SCD
timestamps).

**Blocked by:** None — can start immediately

**Status:** ready-for-agent

- [ ] `CONTEXT.md` created at repo root
- [ ] Terms defined: offer, offer_type, tariff_window_type, scheduled_tariff_window_type, tariff_tier_skey, valid_from/valid_to
- [ ] `_Avoid_` synonyms listed for each term
