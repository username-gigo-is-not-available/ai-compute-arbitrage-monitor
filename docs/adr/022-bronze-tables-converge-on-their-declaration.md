# ADR 022: Bronze tables converge on their declared schema and partition spec

## Status

Accepted. Amends ADR-016's note that a table created before a partition change keeps its old
spec until it is dropped and recreated. Implements the setup decision of #41.
_Amended 2026-10-12_: Silver uses the same `ensure_table`. Its Spark schema (plus the meta
columns) is converted to an Iceberg schema with every column optional, as in Bronze, and synced
before Spark opens the table; Spark then only appends or overwrites partitions. This replaces
Spark creating Silver tables on first write and `SilverWriteStrategy._add_missing_columns` (now `LoadStrategy`).
Spark still casts to the declared nullability; `not_null` is enforced by the dbt tests.

## Context

A Bronze table's shape is declared in code: the schema in `src/ingest/schemas/` and the
partition spec from its `BronzeWriteStrategy`. `ensure_table` only created missing tables
and added missing columns. Every other change (a dropped or renamed column, a widened type,
the hour → day partition change for the `@daily` sources) meant dropping and recreating the
table by hand, although Iceberg supports all of them as metadata changes.

## Decision

Every ingest run makes the table look like its declaration (`GCPLakehouseConfig.ensure_table`):

- **Renames are not supported.** A missing column plus a new one could be a rename or a drop
  plus an add, and field ids cannot tell them apart, because the catalog assigns its own ids
  when it creates a table. So a renamed column is synced as a drop plus an add: the new column
  starts empty. Bronze names follow the source API and friendlier names belong in Silver or
  dbt, so the rule is: **never rename a Bronze column in place; add the new one and keep the
  old one.** A declared-rename mapping was built and removed as unused.
- **New columns and wider types** come from `union_by_name`. A narrowing or incompatible
  type change, or a new required column on an existing table, fails the run.
- **Columns not in the schema are dropped**, with a warning in the log. The data stays in
  older snapshots (time travel) until they are expired.
- **Column order** follows the schema.
- **The partition spec** is compared as a list of (source column name, transform); when it
  differs, every current field is removed and every declared one added in one spec update.
  Fields that stay the same keep their id.
- **Files written under an older spec are rewritten** into the current one by overwriting
  the table with its own contents. This reads the whole table into memory, which the small
  Bronze tables allow. It checks the files, not whether the spec changed in this run, so a
  rewrite interrupted by a failed run is finished by the next one.

A table that already matches its declaration gets no new schema, spec or snapshot.

## Consequences

- Schema and partition changes ship as code changes; no manual migration or table drop.
- Deleting a column from a schema file deletes it from the table on the next run.
- The rewrite replaces Bronze files with new ones holding the same rows, including their
  original `ingested_at`. ADR-017's "every ingest attempt is recorded" still holds; the
  rows are moved, not changed.
- Renaming a column in a schema file loses that column's values from the current table
  (they stay in older snapshots until expired); only the log warning shows it.
- Dropping a column that the current partition spec uses, in the same change that moves the
  spec off it, fails: columns are dropped before the spec is updated.
