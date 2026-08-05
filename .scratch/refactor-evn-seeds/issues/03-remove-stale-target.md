# 03 — Remove stale target/ build artifacts

**What to build:** Delete the `src/transform/target/` directory. It contains stale
compiled tests and run artifacts that still reference the old `scheduled_tariff_window`
column, causing confusion in dbt logs. The directory is already in `.gitignore` so this
is a local-only cleanup.

**Blocked by:** None — can start immediately

**Status:** ready-for-agent

- [ ] `src/transform/target/` directory removed
- [ ] `git status` no longer shows stale compiled artifacts
