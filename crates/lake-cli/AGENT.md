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

## Layout

- `main.rs` — clap `Cli` + dispatch
- `commands/mod.rs` — `Context` (metastore + engine + metasrv wiring)
- `commands/{selftest,sql,table,serve}.rs` — one per subcommand group
