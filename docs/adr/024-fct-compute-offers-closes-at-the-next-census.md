# ADR 024: `fct_compute_offers` closes an open row at the next census, not the newest

## Status

Accepted. Amends ADR-012: the close-out still fires on supersession or absence, but closes at
the next census date in the table instead of the newest.

## Context

ADR-012's post-hook set `valid_to = max(valid_from)` on every older open row. That is the next
census only when a dbt run adds one census, because then the newest census is also the next one.

The first `dbt build` since 2026-10-05 (on 2026-10-25) added the 10-12 and 10-21 censuses in one
run. The post-hook closed both the 10-05 and the 10-12 rows at 10-21, so the 10-05 rows claimed
`[10-05, 10-21)` and overlapped the 10-12 rows: `assert_scd2_no_overlapping_ranges` returned
43,281 rows. Any run that catches up after a missed run does the same, and so does a full
refresh, which ADR-012 therefore called unsafe for history.

## Decision

The post-hook closes every open row at the next census date in the table:

```sql
update {{ this }} as fct
set valid_to = census.next_valid_from
from (
    select valid_from, lead(valid_from) over (order by valid_from) as next_valid_from
    from (select distinct valid_from from {{ this }})
) as census
where fct.valid_from = census.valid_from
  and fct.valid_to = timestamp '9999-12-31'
  and census.next_valid_from is not null;
```

"Next" is the next census in the whole table, not the machine's own next row: a machine missing
from the next census (delisted, or not returned) is closed at that census, as under ADR-012. A
per-machine `lead()` would leave a delisted machine open forever.

## Consequences

- **Positive**:
  - A run that adds several censuses, and a full refresh, close every census at the right date.
    Verified 2026-10-25 by a full refresh: 10-05 closes at 10-12, 10-12 at 10-21, 10-21 open;
    `assert_scd2_double_active` and `assert_scd2_no_overlapping_ranges` pass.
  - Full refresh is safe again; it rebuilds the history from Silver.
- **Negative**:
  - The post-hook scans the distinct `valid_from` of the whole table on every run. With one
    census a day that is a few hundred values.
