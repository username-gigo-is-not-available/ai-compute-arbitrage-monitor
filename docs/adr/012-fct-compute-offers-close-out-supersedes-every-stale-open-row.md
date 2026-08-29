# ADR 012: `fct_compute_offers` close-out supersedes every stale open row

## Status

Accepted — amends ADR-008.

## Context

ADR-008 specified that the SCD close-out post-hook "correlates on
`(offer_id, offer_type)`, so each offer_type's history expires independently when
that modality drops out of the latest scrape." The implemented post-hook
therefore only closed an open row when the offer was **absent** from the latest
snapshot (`not exists (...)` against `int_compute_offers` at the latest
`valid_from`).

That guard is inverted for an SCD Type 2 dimension. A version is superseded when
a **newer version of the same entity arrives** — not only when the entity
disappears. Because `fct_compute_offers` ingests a full offer snapshot every
hour, every offer that persists across snapshots accumulated a new open row while
its previous open rows were never closed.

Detection (2026-08-28, `assert_scd2_*` tests, ADR-011) confirmed both violation
classes from this single mechanism:

- **Double active** — 14,546 pairs across 399 offers. distribution of open rows
  per natural key: 2→1,211 keys, 3→1,078, 4→1,036, 5→63, 6→21, 7→140, which
  exactly matches the combination counts of a per-hour stack
  (`1211·1 + 1078·3 + 1036·6 + 63·10 + 21·15 + 140·21 = 14,546`). All 399
  double-active offers were present in the latest snapshot — the closer skipped
  every one by design.
- **Overlapping ranges** — 19,642 pairs; 14,546 are the same double-active pairs
  (both `valid_to = 9999-12-31`) and 5,096 are previously stacked rows that were
  bulk-closed at the same drop timestamp, so their `[valid_from, drop_ts)`
  intervals overlap each other.

## Decision

Change the close-out so it fires on **supersession or absence**: on each
incremental build, close **every** open row older than the newest snapshot's
`valid_from`, whether or not the offer is still listed.

```sql
update {{ this }} as fct
set valid_to = '{{ latest_ts }}'
where valid_to = timestamp '9999-12-31'
  and valid_from < '{{ latest_ts }}';
```

The `not exists` guard is removed. "Present in the new snapshot" (a new open row
supersedes the old) and "absent from the new snapshot" (the row is closed at the
instant it was observed to be gone) are both covered by the same predicate.

## Consequences

- **Positive**:
  - Exactly one open row per `(offer_id, offer_type, tariff_tier_skey)` after
    each incremental build; half-open adjacency `[prev_from, new_from)` holds,
    and genuine market departures produce an absent-gap.
  - Both `assert_scd2_double_active` and `assert_scd2_no_overlapping_ranges`
    pass on rows built with the new logic.
- **Negative**:
  - A transient absence (offer not returned by a single API scrape) closes and
    reopens the offer, creating a small gap in its history. The immediate fix
    tolerance is acceptable; a grace-period variant (close only after N
    consecutive absences) can be adopted later if scrape hiccups prove noisy.
  - **Historical data is not repaired by this change alone.** Rows already in
    `fct_compute_offers` (as of 2026-08-27: 51,072 rows, 18,424 open) still
    contain stacked and bulk-closed rows. They need a one-time backfill that
    re-derives `valid_to` per natural key as `lead(valid_from)` over
    `(offer_id, offer_type, tariff_tier_skey)` ordered by `valid_from` so legacy
    rows satisfy the SCD invariant. That backfill is tracked separately.
    _Resolved 2026-08-29_: the one-time backfill was applied to the dev warehouse;
    it re-derived `valid_to` for 13,601 legacy rows, after which all 12
    `assert_scd2_*` tests pass.
  - Full-refresh of `fct_compute_offers` remains unsafe for history: a rebuild
    inserts every historical snapshot with `valid_to = 9999`, and the post-hook
    would then close them all at the same timestamp. Full refresh should continue
    to be avoided in favour of incremental builds.