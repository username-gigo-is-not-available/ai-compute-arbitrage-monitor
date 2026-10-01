# 0D — Spark ↔ GCS-backed Iceberg through the Lakehouse REST catalog

Spike: confirm Spark can create/append/read a GCS-backed Iceberg table through the
GCP Lakehouse runtime catalog (REST catalog) — locally (impersonated ADC) and as a
Dataproc Serverless batch — and confirm cross-engine interop with the 0A table.

Disposable. Nothing under `src/` was modified. The 0A throwaway bucket/catalog were
reused; the shared bucket was not touched.

## Verdicts

| Step | Result | Evidence |
|---|---|---|
| Local create + append + read-back | **PASS** | spark 4.1.1, 5 rows 101–105, values match |
| Interop A — Spark reads 0A pyiceberg table | **PASS** | `spike_table_15ac3e` n=15, 1..15, read-only |
| Interop B — pyiceberg reads Spark table | **PASS** | pyiceberg 0.12.0, ids 101–105, `PyArrowFileIO` |
| BigQuery catalog federation | **PASS** | `SELECT ... FROM \`...spike_table_spark\`` → 5 rows, matches Spark |
| Dataproc Serverless batch (2.2, as spike-sa) | **PASS** | batch `a2b5a8bd` (after 5 failed/aborted attempts, see below): ref table n=15, `spike_table_spark` 5 → 7 rows, ids 106–107, `DATAPROC BATCH read+write: PASS` |
| BigQuery federation after the batch | **PASS** | re-query returned all 7 rows (101–105 + 106–107) — independent confirmation of the Dataproc write |
| Interop B re-run after the batch | **PASS** | pyiceberg 0.12.0 read all 7 rows incl. the batch-written ones |

All six checks green, on one table, across three engines (local Spark 4.1.1,
Dataproc Spark 3.5.3, pyiceberg 0.12.0) plus BigQuery federation. Reaching the batch
verdict took **eight** submissions; every one is listed below with its exact error,
because the failures are the reusable part and two of them were my own errors.

### Batch leg — every submission, with the error that actually occurred

| # | Batch | Iceberg jars requested | Outcome | Decisive log line |
|---|---|---|---|---|
| 1 | `a47475a3` | `-3.5_2.12:1.11.0` + `iceberg-gcp` + `iceberg-gcp-bundle` | FAILED | `java.lang.ClassNotFoundException: scala.Serializable` |
| 2 | `fc4f76ce` | *(none — `spark.jars.packages` mangled to `^spark.jars.packages`)* | FAILED | `Ignoring non-Spark config property: ^spark.jars.packages` |
| 3 | `aaa48d2c` | *(as #2, caret fixed)* | FAILED | `ClassNotFoundException: org.apache.iceberg.spark.SparkCatalog` — packages set in the builder are inert in a launched JVM |
| 4 | `10824119` | `-3.5_2.13:1.10.2` + `iceberg-gcp` + `iceberg-gcp-bundle` | FAILED | reads passed (`n=15`, `rows before append: 5`), then write died: `NoSuchMethodError: 'org.apache.avro.LogicalTypes$TimestampNanos org.apache.avro.LogicalTypes.timestampNanos()'` / `NoClassDefFoundError: Could not initialize class org.apache.iceberg.GenericDataFile` |
| 5 | `5e2b729f` | diagnostic script (classpath probe only) | SUCCEEDED | probe printed only `MISSING` lines — no read, no write (see the false-negative note) |
| 6 | `a52aee80` | `-3.5_2.13:1.10.2` **alone** | FAILED | read died: `NoClassDefFoundError: com/google/auth/oauth2/GoogleCredentials` |
| 7 | `33e5936a` | `-3.5_2.13:1.10.2` + `iceberg-gcp-bundle` | SUCCEEDED | `n=15`, `rows before append: 5`, `rows after append: 7` — first functional pass, but see the row-pollution warning below |
| 8 | `55d7bfd8` | as #7, re-run | SUCCEEDED | `rows before append: 7` → `rows after append: 9` — **appended a duplicate pair** (see below) |
| 9 | `6390895a` | as #7 | FAILED (pre-submit) | `Custom Service Account 'spike-sa@…' is missing required permissions: [dataproc.agents.create, …, dataproc.tasks.reportStatus]` |
| 10 | `a2b5a8bd` | as #7 | SUCCEEDED | `n=15`, `rows before append: 5` → `rows after append: 7`, `DATAPROC BATCH read+write: PASS` — **the verdict batch** |

Attempt 4 is the informative failure: it proves auth, REST-catalog access, `GCSFileIO`
and reads of both tables all work on Dataproc, and isolates the write failure to
classpath poisoning by plain `iceberg-gcp` (finding 3). Attempts 1–4 all left the table
committed and unchanged, i.e. Iceberg aborted the write cleanly.

### Row pollution from re-runs (and why the verdict batch is #10, not #7)

`spike_table_spark` is append-only, so a successful re-run appends again. Attempt 7 took
it 5 → 7; attempt 8 then took it 7 → 9, i.e. ids 106–107 existed twice. A teardown +
`reset` (drop/recreate to the canonical 5 rows) ran between attempts 8 and 10, so the
**final** validated state is exactly 7 rows — confirmed independently by BigQuery
federation and by pyiceberg, both of which returned ids 101–107 with no duplicates.

This is a spike-hygiene trap, not a product bug: any re-run of a batch that appends must
either reset the table first or tolerate a growing count. The `rows before append` line
in `spike_0d_batch.py` exists for exactly this reason.

### The classpath probe is a false negative — do not trust it

Every batch printed four `[classpath] MISSING …` lines, including the batch that then
succeeded:

```
  [classpath] MISSING org.apache.iceberg.spark.SparkCatalog (An error occurred while calling z:java.lang.Class.forName.)
: java.lang.ClassNotFoundException: org.apache.iceberg.spark.SparkCatalog
  [classpath] MISSING org.apache.iceberg.gcp.auth.GoogleAuthManager …
  [classpath] MISSING org.apache.iceberg.gcp.gcs.GCSFileIO …
  [classpath] MISSING org.apache.iceberg.avro.TypeToSchema …
  spike_table_15ac3e: {'n': 15, 'lo': 1, 'hi': 15} (expected 15 rows, 1..15)
  rows before append: 5
  rows after append: 7
DATAPROC BATCH read+write: PASS
```

`java.lang.Class.forName` via py4j resolves against the driver's own classloader, while
Spark loads user jars (`spark.jars.packages`) in a **child** classloader, so the lookup
cannot see them. The probe is retained in the script only because it usefully proved
resolution happened at all (`found … in gcs-maven-mirror`); it must not be used to decide
whether a jar is present. Functional results are the only reliable signal.

## Versions

| | Version |
|---|---|
| Local PySpark | **4.1.1** (Scala 2.13.17, Avro 1.12.1, Java 17) |
| Local Iceberg runtime jar | `iceberg-spark-runtime-4.1_2.13:1.11.0` (+ `iceberg-gcp:1.11.0`, `iceberg-gcp-bundle:1.11.0`) |
| Dataproc Serverless runtime | **2.2** (image 2.2.88) = **Spark 3.5.3, Scala 2.13, Avro 1.11.4** |
| Batch Iceberg runtime jar | `iceberg-spark-runtime-3.5_2.13:1.10.2` + `iceberg-gcp-bundle:1.10.2` |
| pyiceberg (interop B) | 0.12.0 |

**The 1.10.2 vs 1.11.0 version split is a deliberate pin, not a mirror limitation.**
Both legs were *not* locked to 1.11.0: local uses `4.1_2.13:1.11.0`, batch uses
`3.5_2.13:1.10.2`, because Iceberg 1.11.0's Spark-3.5 runtime jar references unshaded
`org.apache.avro.LogicalTypes.timestampNanos()` (Avro ≥ 1.12) which Spark 3.5.3's
Avro 1.11.4 does not provide (finding 2 below). Verified after the fact:
`org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.11.0` **is published and resolvable on
Maven Central** (direct POM fetch returns HTTP 200), so the batch could be moved to 1.11.0
if Avro 1.12 were also supplied — but as things stand on runtime 2.2, 1.10.2 is the correct
pin. (`gcs-maven-mirror` served the requested 1.10.2 without incident; no mirror lag was
involved in the choice.)

`iceberg-spark-runtime-4.1_2.13` **does exist** (1.11.0, 2026-05-15) — the plan's
"may not be published" hedge is obsolete. `4.0_2.13` also exists (1.10.0 → 1.11.0).
The untested mismatch path (`4.0_2.13` jar on a 4.1.x runtime) was **deliberately not
exercised** (decision A): `4.1_2.13:1.11.0` is an exact artifact-to-runtime match, so
the open question is whether Iceberg 1.11 genuinely supports Spark 4.1 or merely
publishes the artifact. That remains unanswered by choice, not by accident.

## Custom image: UNTESTED (0D used the default runtime)

0D proved the Iceberg/Spark/Lakehouse path works, but **only on the default `2.2` runtime**.
Since `refine_strategy.py` pins `runtime_config.container_image` to
`settings.yaml:gcp.dataproc.image_tag`, production batches will *not* use that default, so
the custom-image path is a genuine gap between what 0D proved and what B1/B2 will run.

To test it, the following must exist first (none of it created by this spike):

1. **Enable `artifactregistry.googleapis.com`** — currently *disabled* in this project
   (`gcloud artifacts repositories list` errors out). It *is* in `apis.tf`
   (`artifact_registry` key), so `terraform apply` would create it, but until then no repo
   can exist.
2. **Create the Artifact Registry repo** `ai-compute-arbitrage-monitor-repository` in
   `europe-west3` (docker format) — defined in `repository.tf` as
   `google_artifact_registry_repository.image_repository`; part of the 46-resource plan.
3. **Build and push** `infra/docker/Dockerfile.dataproc` as
   `ai-compute-arbitrage-monitor-dataproc-image:latest` into that repo (needs a CI job or a
   local `docker build`/`gcloud artifacts images push`).
4. **Re-run the 0D batch** with `runtimeConfig.environmentConfig.executionConfig.containerImage`
   set to that URI, confirming both the read of the 0A reference table and the append still
   succeed — and decide whether the Iceberg jars move into the image or stay in
   `spark.jars.packages` (see B2 impact below; keeping the property is the lower-risk choice
   since it is the path 0D actually proved).
5. Confirm the image's Spark version (`dataproc_2.2` base = Spark 3.5.x) matches the
   `3.5_2.13` jar flavour used for the batch.

## Jar-selection findings (all measured on runtime 2.2.88, none assumed)

1. **Scala suffix — the runtime is 2.13, not 2.12.** The Dataproc tutorial's
   `...-3.5_2.12` coordinate fails with
   `java.lang.ClassNotFoundException: scala.Serializable`, proven by jar inspection
   to be a flavour error: `scala-library-2.13.17.jar` contains **zero**
   `scala/Serializable*` entries, while `scala-library-2.12.20.jar` contains
   `scala/Serializable.class`. Iceberg's own `_2.13` jar was correct.
2. **Iceberg version — use 1.10.x on Spark 3.5.3, not 1.11.0.** `1.11.0`'s Spark-3.5
   runtime jar references **unshaded** `org.apache.avro.LogicalTypes.timestampNanos()`
   (Avro ≥ 1.12), which Spark 3.5.3's Avro 1.11.4 does not have. `1.10.2`'s
   `TypeToSchema` is fully shaded to `org.apache.iceberg.shaded.org.apache.avro.*`
   (verified with `javap`) and is self-contained.
3. **Request the runtime jar + `iceberg-gcp-bundle`, never plain `iceberg-gcp`.**
   - The shaded runtime jar contains Iceberg's GCS classes
     (`.../gcp/auth/GoogleAuthManager`, `.../gcp/gcs/GCSFileIO`) but not Google's SDK:
     with the runtime jar alone the *read* dies with
     `ClassNotFoundException: com.google.auth.oauth2.GoogleCredentials`.
   - `iceberg-gcp-bundle` declares **zero** dependencies and packages the shaded Google
     SDK (`com/google/auth/oauth2/GoogleCredentials`), and contains **0** unshaded
     `org/apache/avro/*` and **0** unshaded `iceberg-core` classes — safe to add.
   - Plain `iceberg-gcp` declares `iceberg-core` transitively, so naming it pulled
     unshaded `iceberg-core` + `avro 1.12.1`; the runtime's own `avro-1.11.4.jar` won
     classpath ordering, producing at parquet write time:
     `NoClassDefFoundError: Could not initialize class org.apache.iceberg.GenericDataFile`,
     `Caused by: NoSuchMethodError: 'org.apache.avro.LogicalTypes$TimestampNanos org.apache.avro.LogicalTypes.timestampNanos()'`.
4. **Maven egress was never needed.** Dataproc resolved packages through Google's GCS
   Maven mirror (`... in gcs-maven-mirror`, `maven-central-eu.storage-download.googleapis.com`),
   not Maven Central. The plan's NAT-to-Maven-Central premise was moot.

## Required catalog / FileIO settings (verified, not guessed)

Local (impersonated, token-based) and batch (ADC) differ only in auth:

- `spark.sql.catalog.<c>.type = rest`
- `spark.sql.catalog.<c>.uri = https://biglake.googleapis.com/iceberg/v1/restcatalog`
- `spark.sql.catalog.<c>.warehouse = bl://projects/<project>/catalogs/<catalog_id>`
- `spark.sql.catalog.<c>.header.x-goog-user-project = <project>` — required (billing/quota header)
- `spark.sql.catalog.<c>.io-impl = org.apache.iceberg.gcp.gcs.GCSFileIO`
- Batch auth: `spark.sql.catalog.<c>.rest.auth.type = org.apache.iceberg.gcp.auth.GoogleAuthManager` (ADC)
- Local auth: `spark.sql.catalog.<c>.token` + `gcs.oauth2.token` / `gcs.oauth2.token.expires-at`
  (the impersonated-token escape hatch; see deviation below)

## IAM — every grant made (and every one revoked)

Final observed state of project-level bindings for `spike-sa`, and where each came from:

| Grant | Scope | Origin | Status |
|---|---|---|---|
| `roles/iam.serviceAccountTokenCreator` user → `spike-sa` | on spike-sa | 0C (pre-existing) | untouched by 0D — not re-granted, not revoked |
| `roles/biglake.editor` | project | 0A/0C (pre-existing) | untouched by 0D |
| `roles/bigquery.jobUser` | project | 0A/0C (pre-existing) | untouched by 0D |
| `roles/storage.objectAdmin` | project | 0A/0C (pre-existing) | untouched by 0D |
| **`roles/dataproc.worker`** | **project** | **GRANTED BY 0D** | **REVOKED at teardown ✓** |

So exactly **one** new grant was required: **`roles/dataproc.worker`** on the batch
identity (spike-sa), which Dataproc Serverless needs to run a batch as that SA. No new
`storage.objectAdmin` was needed — spike-sa already held it project-level from 0A/0C,
so bucket read/write on the throwaway bucket required nothing new.

### Catalog-scoped IAM: not exercised, and not needed here

The agreed decision was catalog-scoped-first with a hard stop on 403. What was
actually observed: `gcloud alpha biglake iceberg catalogs get-iam-policy
spike_iceberg_15ac3e23` returns an **empty policy** (`{"etag": "ACAB"}`) — no binding
was ever placed on the catalog, and no catalog-scoped 403 was ever recorded. Every
read and write succeeded on the **project-level** `biglake.editor` that 0A/0C had
already granted spike-sa.

The honest finding is therefore narrower than "catalog-scoping does not work":
catalog-level IAM was **never required and never tested** on this path. What is
proven is that project-level `biglake.editor` on the batch identity is sufficient.
Whether a catalog-scoped-only binding would also suffice is left open.

Note the identity decision: **the plan's premise was false** — the project contains
**no `dataproc-sa`, no VPC, no subnet, no Dataproc API**, i.e. the `infra/terraform/envs/gcp`
stack was never applied. The batch therefore ran as the 0C-proven **spike-sa** instead of
a hand-provisioned `dataproc-sa` (decision B), isolating the new variable (Spark/Iceberg)
from the identity variable. For B2 the honest datapoint is therefore: *the batch identity
needs exactly the 0C triple (`biglake.editor` + `bigquery.jobUser` + `storage.objectAdmin`),
and `dataproc-sa` does not exist today because iam.tf is unapplied.*

### Note on `iam.tf`

`iam.tf` in `infra/terraform/envs/gcp` is **unapplied** (no tfstate), so nothing it
declares exists in this project. Its current contents would also not be sufficient for
a Spark-on-Dataproc leg: it grants no `biglake.editor` at all (0C's finding) and no
`dataproc.worker` to the batch identity. Both are needed for this path — see the grant
table above.

## Infra created for the batch (and torn down)

**Correction (post-0D review): the batches ran on the DEFAULT runtime, not a custom
image.** `gcloud dataproc batches list --format='...containerImage...'` returns empty for
every 0D batch, and `gcloud dataproc batches describe` shows only
`runtimeConfig.version = 2.2` with no `environmentConfig.executionConfig.containerImage`.
So `Dockerfile.dataproc` was **not** exercised by 0D, and nothing in this spec should be read
as evidence about it. This also means the earlier claim that serverless "never consumes"
`Dockerfile.dataproc` was **wrong**: `infra/airflow/dags/refine_strategy.py` does set
`runtimeConfig.containerImage` from `settings.yaml`'s `gcp.dataproc.image_tag`, so
production serverless batches do run a custom image. See "Custom image: untested" below.

Because no VPC/subnet existed, minimal throwaway networking was created and fully removed:

**Gap found in `apis.tf` during post-0D review:** `biglake.googleapis.com` is **not** in the
`enabled_services` map, yet the whole path depends on the Lakehouse Iceberg REST catalog at
`https://biglake.googleapis.com/iceberg/v1/restcatalog` and on `roles/biglake.editor`
(now added to three SAs in `iam.tf`). The spike enabled the API by hand, so a fresh
`terraform apply` would create SAs holding a BigLake role with the BigLake API disabled.
Not fixed here (out of scope for this pass) — it needs `biglake = "biglake.googleapis.com"`
in `locals.services` in `apis.tf`. Related: the plan also creates **no GCS bucket and no
Iceberg catalog**, so B2 still needs both provisioned.

- Enabled APIs: `compute.googleapis.com`, `dataproc.googleapis.com`, `managedspark.googleapis.com`
- `default` network in `europe-west3`: Private Google Access enabled on the subnet
- Cloud Router `spike-nat-router` + NAT `spike-nat` (auto-allocated IPs) — for Maven egress,
  which turned out to be unnecessary (see finding 4)
- Batch staging used the existing throwaway bucket `spike-iceberg-test-15ac3e23`

## Submit-path findings (Dataproc Serverless batching)

All three submit mechanisms were attempted; record the winner so B2 does not repeat this:

1. `gcloud dataproc batches submit --properties` mangles `spark.jars.packages` unless
   commas are escaped with the `^` delimiter form (`^|^<key>=<value>`); a stray caret
   yields `Ignoring non-Spark config property: ^spark.jars.packages`.
2. The `google-cloud-dataproc` Python client returns
   `400 Request contains an invalid argument` when `staging_bucket` carries a `gs://`
   scheme (the API wants the bare bucket name). Not diagnosed further.
3. **Winner: the Dataproc REST API** (`POST .../locations/<r>/batches`) with an explicit
   JSON body — fully repeatable, no shell escaping, no scheme ambiguity.
   Script: `.spratch/_0d_submit_rest.ps1`.

Also proven: `spark.jars.packages` set inside `SparkSession.builder` in an
already-launched JVM is **inert** (it is a spark-submit launch-time property). It must
arrive via the batch's `runtime_config.properties`.


## The Hadoop GCS connector is NOT required on this path

The plan asked whether the connector jar and `spark.hadoop.fs.gs.impl` are needed before
carrying them forward as defaults. Answer, measured: **not needed, and not set.**
`GCSFileIO` talks to GCS through Google's client library, so no Hadoop filesystem
delegation is involved. Neither `spike_0d_local.py` nor `spike_0d_batch.py` sets
`spark.hadoop.fs.gs.impl`, `fs.AbstractFileSystem.gs.impl`, or any `fs.gs.*` key — and
both legs read *and* wrote successfully (local: 5 rows written and read; batch: appended
to 7 and read back). The only GCS credential settings required are `GCSFileIO`'s own
(`gcs.oauth2.token` / `gcs.oauth2.token.expires-at` locally; `GoogleAuthManager`/ADC on
the batch).

This is precisely where `initialize_spark()` diverges: its LOCAL branch configures those
`fs.gs.*` keys with `USER_CREDENTIALS`, a path that both presumes non-impersonated
credentials and is unnecessary once `io-impl=GCSFileIO` is set.

## Deliberately kept (0A state, per plan)

- bucket `gs://spike-iceberg-test-15ac3e23`
- catalog `spike_iceberg_15ac3e23`
- reference table `spike_ns_15ac3e.spike_table_15ac3e` (15 rows) — **untouched**

## B2 impact

- **`Dockerfile.dataproc`**: **no change forced by the version split, but the file itself is
  UNVALIDATED for serverless.** The split (`4.1_2.13` local vs `3.5_2.13` batch) is a
  jar-coordinate concern, not a Dockerfile one. However 0D never ran this image (all
  batches used the default runtime), and `refine_strategy.py` *does* point serverless at it
  via `gcp.dataproc.image_tag`, so the image is on the B1/B2 critical path untested. Its
  `FROM us-central1-docker.pkg.dev/cloud-dataproc/spark/dataproc_2.2` is also a
  **us-central1** artifact being used in **europe-west3** — a cross-region concern worth
  confirming, and it does not obviously conflict with serverless 2.2 (Spark 3.5.x).
- **Iceberg jars on a custom image**: 0D proved the jars must be on the classpath at
  *launch* time, and proved `spark.jars.packages` inside a `SparkSession.builder` is inert
  in an already-launched JVM. With a custom image you have two valid options — keep
  `runtime_config.properties`/`spark.jars.packages` (simplest, already proven), or bake the
  two jars into the image and drop the property. Baking them in is *not* required, and if
  done, both `iceberg-spark-runtime-3.5_2.13:1.10.2` and `iceberg-gcp-bundle:1.10.2` must
  be included — the runtime jar alone fails on `com.google.auth.oauth2.GoogleCredentials`.
  Note the base image is Spark 3.5.x, so the image's jar must be the `3.5_2.13` build; a
  `4.1_2.13` jar baked into that image would fail exactly as the local/Batch parity gap
  suggests. This remains **untested**.
- **Jars on the custom image are also a Python-env question**: `Dockerfile.dataproc` creates
  a conda env (`spark_env`) and sets `PYSPARK_PYTHON`, but installs no Iceberg/Spark
  packages there — the Python side needs none for this path.
- **`pyproject.toml`**: no change forced. Local Spark 4.1.1 is already the declared floor.
  The extra artifact pair (`iceberg-gcp-bundle`) is a runtime-supply concern for the batch,
  not a Python dependency.
- **`initialize_spark()`** would need, at minimum:
  1. the REST-catalog config block (type/uri/warehouse/`x-goog-user-project`/`io-impl`) for
     the GCP branch, which today sets nothing (relies on image defaults);
  2. `rest.auth.type = org.apache.iceberg.gcp.auth.GoogleAuthManager` for the GCP branch;
  3. for LOCAL, the current `USER_CREDENTIALS` client-id/secret/refresh-token path is
     incompatible with impersonated ADC — it needs either a `GCSFileIO` + impersonated-token
     path (as in `spike_0d_local.py`'s `mint_sa_token()`) or an explicit documented
     limitation. `spark.jars.packages` must be launch-time (spark-submit/runtime config),
     never a `SparkSession.builder` call — proven inert in a launched JVM.

## Teardown commands actually used

```powershell
# 1. drop the spike's own table (the 0A tables stay)
$env:PYSPARK_PYTHON = ".\.venv\Scripts\python.exe"
.\.venv\Scripts\python.exe .spratch\spike_0d_teardown.py   # DROP TABLE IF EXISTS spike_ns_15ac3e.spike_table_spark
#    -> _0d_teardown.log: "[after] tables in spike_ns_15ac3e: [('spike_ns_15ac3e', 'spike_table_15ac3e')]"
#       "TEARDOWN: PASS - spike table gone, 0A reference table intact"

# 2. purge the dropped table's orphaned data dir (DROP TABLE does not delete files)
gcloud storage rm -r gs://spike-iceberg-test-15ac3e23/spike_ns_15ac3e/spike_table_spark/**

# 3. remove the batch script staged in the throwaway bucket
gcloud storage rm -r gs://spike-iceberg-test-15ac3e23/spike

# 4. revoke the one IAM grant 0D added
gcloud projects remove-iam-policy-binding graphic-mission-505412-j7 `
  --member="serviceAccount:spike-sa@graphic-mission-505412-j7.iam.gserviceaccount.com" `
  --role="roles/dataproc.worker"

# 5. remove the throwaway networking created for the batch (no VPC existed)
gcloud compute routers nats delete spike-nat --router=spike-nat-router --region=europe-west3 --quiet
gcloud compute routers delete spike-nat-router --region=europe-west3 --quiet
gcloud compute networks subnets update default --region=europe-west3 --no-enable-private-ip-google-access
```

Verified after teardown: `privateIpGoogleAccess: False`, no routers, no NAT, bucket root
back to `spike_ns_15ac3e/spike_table_15ac3e/` only, `dataproc.worker` no longer bound
(spike-sa back to exactly the 0A/0C triple), `spike_table_spark` returns
`Not found: Table ...was not found`, and the 0A reference table still reads 15 rows. The
0A bucket `spike-iceberg-test-15ac3e23`, its `spike_ns_15ac3e/spike_table_15ac3e/` data,
and catalog `spike_iceberg_15ac3e23` were left untouched.

## Raw logs

`.spratch/_0d_local.log`, `_0d_pyiceberg.log`, `_0d_pyiceberg2.log`, `_0d_batch.log`,
`_0d_submit_rest.ps1` (winner submit path), `spike_0d_local.py`, `spike_0d_pyiceberg_read.py`,
`spike_0d_batch.py`. Dataproc driver output lives under
`gs://spike-iceberg-test-15ac3e23/google-cloud-dataproc-metainfo/<uid>/jobs/<batch-id>/driveroutput.*`.

