# Compute offers: from Vast.ai to the marts

A plain-English walk through the `compute_offers` pipeline, following one machine from Vast.ai to the
reports. The design decisions behind it are in [ADR-020](adr/020-compute-offers-market-census.md); the
terms (Offer, Slice, Machine, Census, Available / Taken) are defined in [CONTEXT.md](../CONTEXT.md).

**The example:** machine **#15489**, an RTX 4090 box with **5 GPUs** at **$0.40 per GPU per hour**. Vast.ai
lists it as several overlapping offers (slices): rent 1 GPU, rent 2, rent 4, and so on. The numbers are
rounded from a probe on 2026-10-03 and are illustrative.

Every night at **00:00 UTC**, when Vast.ai's daily quota resets, Airflow runs three stages in order:
**download → clean → report**.

```
Vast.ai ──(download, 00:00 UTC)──▶ BRONZE: raw offers, as Vast.ai sent them
        ──(clean, Spark)──────────▶ SILVER: clean offers
        ──(dbt)──▶ stg (offers) ──▶ int (MACHINES) ──▶ fct (machines × tariffs, cost + profit) ──▶ marts
```

Bronze and Silver keep what Vast.ai said (offers). Gold reshapes it into what is real (machines). If the
way machines are built ever changes, Gold can be rebuilt from Silver without downloading anything again.

---

## Stage 1: Download → Bronze (raw data)

*Code: `src/ingest/sources/compute_offers.py` · Table: `bronze_sources.compute_offers`*

**Goal:** every offer on the Vast.ai on-demand market, stored exactly as Vast.ai sent it.

1. **Label the snapshot.** The run gets the hour it was scheduled for, e.g. `2026-10-06 00:00` UTC.
   Everything from this run carries that label (`snapshot_at`). The real fetch time is `ingested_at`.
2. **Download the whole market in pieces.** Vast.ai returns at most 512 offers per request, a random
   sample when more match, so:
   - Ask for every offer ID → 512 come back → too many, so it is a sample. Use the sample's IDs to cut the
     ID range into 128 pieces (crowded ID stretches get narrower pieces).
   - Ask for each piece → each returns fewer than 500 → that piece is complete.
   - A piece that still comes back full is cut again the same way (`cover()`).
   - About 130 requests at one per second, using ~13–14k of the 20k rows/day quota.
3. **All or nothing.** If any piece fails, the run stops and nothing is saved. If Vast.ai answers that the
   quota is used up (a `Retry-After` of hours), it stops at once instead of retrying.
4. **Save.** Each offer becomes one row of ~40 columns: prices, GPU model, CPU, disk, location,
   `rentable_flag`, and the slice columns `gpu_fraction_of_machine` and `gpu_ids`.

**Our machine in Bronze**: one row per slice Vast.ai returned:

| offer_id | machine_id | GPUs | price/hr | gpu_fraction_of_machine | rentable |
|---|---|---|---|---|---|
| 8936322 | 15489 | 1 | $0.40 | 0.2 | yes |
| 8936325 | 15489 | 2 | $0.80 | 0.4 | yes |
| 8936323 | 15489 | 4 | $1.60 | 0.8 | yes |

Bronze only grows; nothing in it is changed or deleted. It is the raw history.

---

## Stage 2: Clean → Silver

*Code: `src/refine/sources/compute_offers.py` (Spark) · Table: `silver_sources.compute_offers`*

**Goal:** the same rows, cleaned and reliable.

1. **Read only what is new.** Bronze rows with a `snapshot_at` newer than anything already in Silver, so
   running it twice does not duplicate data.
2. **Clean the text:** remove non-ASCII characters, trim spaces, turn empty strings into nulls, shorten CPU
   names (`"AMD EPYC 7B13 64-Core Processor"` → `"AMD EPYC 7B13"`).
3. **Deduplicate.** If a download was retried, keep only the latest attempt for that snapshot.
4. **Fix the types** to the Silver schema and append to Silver.

**Our machine in Silver:** the same 3 rows, clean. Silver is still one row per offer: the trustworthy copy
of what Vast.ai said.

---

## Stage 3: Report → Gold (dbt on BigQuery)

*Code: `src/transform/models/`*

### 3a. `stg_compute_offers`: staging

Renames and types the columns. Still one row per offer.

### 3b. `int_compute_offers`: one row per machine

1. **Group the slices by machine.** Our 3 rows form one group: #15489.
2. **Machine size from any slice:** GPUs ÷ fraction = 1 ÷ 0.2 = **5 GPUs**, even though the biggest slice
   returned was 4. Every slice gives the same answer; the test
   `assert_stg_compute_offers_machine_slices_agree` checks it on every census.
3. **Specs from the biggest slice** (CPU, RAM, disk).
4. **Price for the whole machine:** $0.40 per GPU × 5 = **$2.00/hr**. The per-GPU price is the same on
   every slice; the same test checks it.
5. **Available or taken:** available if any slice can be rented now, otherwise taken (rented, or switched
   off by its owner; the data cannot tell these apart).

| machine_id | GPU | GPUs | price/hr | available |
|---|---|---|---|---|
| 15489 | RTX 4090 | 5 | $2.00 | yes |

On 2026-10-05: 13,069 offers → **7,357 machines**, 23,436 GPUs.

### 3c. `fct_compute_offers`: cost and profit

**Goal:** for each machine, its profit if it ran in Macedonia, under every EVN tariff.

1. **One row per machine per tariff tier.** There are 7 tiers (consumer category × low/high window ×
   block), so 7,357 machines × 7 = 51,499 rows.
2. **Look up that day's values:** USD→MKD exchange rate, the tier's price per kWh, distribution and
   access fees.
3. **Calculate:**
   - power: TDP × GPUs, e.g. 450 W × 5 = 2.25 kWh per hour
   - electricity cost per hour: kWh × (tariff + distribution fee) ÷ exchange rate, plus the monthly access
     fee spread over 730 hours
   - revenue: the machine's price, $2.00/hr
   - profit: revenue − cost, also per TFLOP
4. **History:** when the next census arrives, the previous rows get an end (`valid_to`). Current rows have
   `valid_to = 9999-12-31`.

### 3d. The marts: the answers

All read the current fact rows.

| Mart | Question it answers |
|---|---|
| `mart_arbitrage_opportunities` | Which available machines are most profitable per TFLOP, ranked per tariff? |
| `mart_best_offers_by_gpu` | For each GPU model, which available machine is best per GPU? |
| `mart_gpu_model_summary` | Per GPU model: machines, GPUs, % available, % profitable, price per GPU of available vs taken machines |
| `mart_gpu_forecast` | "My GPU over the next hours": median market price per GPU × my GPU count, against the hourly EVN schedule |
| `mart_profitability_trends` | Profit by hour; with one census a day it only covers 00:00 until issue #40 |

In the first census, taken machines were cheaper per GPU than available ones (RTX 4090: ~$0.44 vs ~$0.51),
so the taken price is the more realistic expectation for a host.
