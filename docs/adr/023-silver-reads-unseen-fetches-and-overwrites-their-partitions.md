# ADR 023: Silver reads the fetches it has not seen and overwrites the partitions they touch

## Status

Accepted. Supersedes the Silver write strategies of ADR-016 and ADR-017, and the Silver
watermark of ADR-019 (`snapshot_at`). Bronze is unchanged: every source and seed is true
append, partitioned by the day the data is about where Bronze has that day (`snapshot_at`,
`timestamp`), else by `ingested_at` (seeds, whose `valid_from` is still raw text in Bronze).

## Context

Silver had two load strategies:

- `AppendByDay(snapshot_at)` for `compute_offers`: read Bronze rows with
  `snapshot_at > max(snapshot_at)` in Silver and append them.
- `OverwriteByPartition` for `exchange_rates` and the four tariff seeds: read all of Bronze,
  keep the latest fetch per key and overwrite the partitions written.

Both left one copy per day after an ordinary retry, but they disagreed on a day that changes
after Silver has it. A manual re-run of the 10-21 census after Silver had run lands in Bronze
with the same `snapshot_at`, so `snapshot_at > 10-21` is false and the re-run never reaches
Silver. A rebuild of Silver from Bronze would read both censuses and keep the later one. So
the live table depended on when Silver ran, not only on what Bronze holds, although the reason
Bronze is append-only is that Silver can be rebuilt from it.

`snapshot_at` cannot serve as the watermark: it is deterministic (ADR-019), so every fetch of a
day has the same value. Only `ingested_at`, set at fetch time, grows with every fetch.

## Decision

One strategy for every Silver table (`refine.load_strategy.OverwriteByPartition`):

- **Read** the Bronze rows with `ingested_at > max(ingested_at)` in Silver, or all of Bronze
  when Silver is empty. The watermark is always `ingested_at`, which every Bronze and Silver
  table carries.
- **Transform** as before; each pipeline's dedup keeps the latest `ingested_at` per key.
- **Write** with `overwritePartitions()`: every partition present in the new rows is replaced,
  including new ones; partitions absent from them are left alone.

The watermark column decides what is read; the partition column decides what is replaced:

| Table | Watermark | Partition |
|---|---|---|
| `compute_offers` | `ingested_at` | `day(snapshot_at)` |
| `exchange_rates` | `ingested_at` | `timestamp` (identity) |
| tariff seeds | `ingested_at` | `valid_from` (identity) |

Partition names stay as before (`snapshot_at_day`, `timestamp`, `valid_from`), so existing
tables keep their spec.

**Assumption: every fetch delivers whole partitions.** Overwriting a partition from one fetch
is only correct if that fetch holds all of the partition's rows. It holds for every table today:
a census is written to Bronze only once every range is in (ADR-020), a rate is one row per
`timestamp`, and a tariff scrape carries each version it lists completely.

## Consequences

- Silver is what a rebuild from Bronze gives: a same-day re-run replaces that day, whether it
  arrives before or after Silver ran.
- Each run reads only the new fetches, about one census a day, not all of Bronze. A run with
  no new fetches stops after the read and writes nothing (`Pipeline.run`); before, the read
  was never empty, and the window schedule's `generate` failed on zero versions.
- A tariff version EVN drops from its page stays in Silver: no new rows touch its partition,
  and it is still needed for as-of joins on older offers.
- A source that could deliver part of a partition (a partial census, a paginated seed) would
  shrink that partition to the part. Such a source needs a different strategy.
- A change to a Silver transform applies only to new fetches. To re-apply it to old days,
  empty the Silver table; the next run then reads all of Bronze.
- `AppendByDay` and the `LoadStrategy` base class are removed from `refine`; Bronze keeps its
  own `ingest.write_strategy.AppendByDay`.
