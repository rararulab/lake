# lake-cli

The all-in-one `lake` binary: run the tier servers, query, and administer
tables. clap derive, thin `main.rs`, one handler module per command.

## Invariants

- Application boundary: the ONLY crate allowed `anyhow`. Domain crates use
  per-crate snafu enums.
- `main.rs` stays thin — parse, build `Context`, dispatch. Command logic
  lives in `commands/`, split by subcommand.
- Agent-friendly: results to stdout, deterministic exit codes. (Structured
  `--format json` output is a planned addition.)
- `selftest` must keep proving the whole path (create → ingest → SQL) in one
  command on a laptop — the goal.md working signal.

## Storage modes (`commands/mod.rs::Context`)

- **local** (default) — RocksDB metastore + local-FS Lance under `--data-dir`.
- **cloud** — set `LAKE_S3_BUCKET` → `DynamoMeta` + Lance on S3. Config:
  `LAKE_DYNAMODB_ENDPOINT`/`LAKE_DYNAMODB_TABLE`, `LAKE_S3_ENDPOINT`, `AWS_*`,
  and `LAKE_S3_PROXY_EXCLUDES` (bypass an ambient `PROXY_URL` for the endpoint;
  behind a proxy also set the standard `NO_PROXY` so the drop path's direct
  object_store client bypasses it too).

## Layout

- `main.rs` — clap `Cli` + dispatch
- `commands/mod.rs` — `Context` (local/cloud storage wiring: metastore + engine
  + metasrv)
- `commands/{selftest,sql,ingest,table,serve}.rs` — one per subcommand group
  (`table` covers create/list/drop; `ingest` loads a Parquet file)
