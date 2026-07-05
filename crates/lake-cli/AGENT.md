# lake-cli

The `lake` binary: end-to-end self-check (ingest parquet -> commit
manifest -> SQL query). Run via `mise run e2e`.

## Invariants

- Application boundary: the ONLY crate allowed to use `anyhow`. Domain
  crates use per-crate snafu enums.
- The self-check must keep proving the whole path in one command on a
  laptop (goal.md signal). New user-visible behavior should extend it.

## Layout

- `main.rs` — the self-check. Data lands in `./data` (gitignored).
