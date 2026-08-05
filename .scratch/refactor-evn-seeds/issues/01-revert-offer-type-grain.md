# 01 — Revert offer_type grain in stg/fct compute_offers

**What to build:** Restore the correct SCD Type 2 grain for compute offers. The `offer_id`
identifies a (host, machine) pair and is unique per `(offer_id, ingested_at)` snapshot —
confirmed by querying the source (0 duplicate rows). The `offer_type` (on_demand, bid,
reserved) is a property of the snapshot, not part of the identity. Revert the `unique_key`
config in `stg_compute_offers` back to `['offer_id', 'ingested_at']` and in
`fct_compute_offers` back to `['offer_id', 'valid_from', 'tariff_tier_skey']`. In the
`fct_compute_offers` post-hook, keep the `NOT EXISTS` pattern (NULL-safe improvement over
`NOT IN`) but drop the `offer_type` correlation so it matches on `offer_id` only.

**Blocked by:** None — can start immediately

**Status:** ready-for-agent

- [ ] `stg_compute_offers.sql` `unique_key` reverted to `['offer_id', 'ingested_at']`
- [ ] `fct_compute_offers.sql` `unique_key` reverted to `['offer_id', 'valid_from', 'tariff_tier_skey']`
- [ ] `fct_compute_offers.sql` post-hook uses `NOT EXISTS` correlated on `offer_id` only
- [ ] `dbt run` succeeds on `stg_compute_offers` and `fct_compute_offers`
- [ ] `dbt test` passes on both models (schema tests already match the reverted grain)
