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
- Non-loopback serving fails without inbound TLS + bearer authentication unless
  `LAKE_ALLOW_INSECURE=true` explicitly declares a trusted terminating proxy.

## Storage modes (`commands/mod.rs::Context`)

- **local** (default) — RocksDB metastore + local-FS Lance under `--data-dir`.
- **cloud** — set `LAKE_S3_BUCKET` → `DynamoMeta` + Lance on S3. Config:
  `LAKE_DYNAMODB_ENDPOINT`/`LAKE_DYNAMODB_TABLE`, `LAKE_S3_ENDPOINT`,
  `LAKE_TABLE_PREFIX`, `LAKE_MANAGED_OBJECT_PREFIX`, `AWS_*`, and
  `LAKE_S3_PROXY_EXCLUDES` (bypass
  an ambient `PROXY_URL` for the endpoint;
  behind a proxy also set the standard `NO_PROXY` so the drop path's direct
  object_store client bypasses it too).

## Flight security

- Inbound: `LAKE_AUTH_PRINCIPALS_FILE` (protected multi-tenant map) or the
  backward-compatible `LAKE_AUTH_TOKEN_FILE`, plus `LAKE_TLS_CERT_FILE` and
  `LAKE_TLS_KEY_FILE`.
- Query/admin→Metasrv: `LAKE_METADATA_AUTH_TOKEN_FILE`,
  `LAKE_METADATA_CA_FILE`, `LAKE_METADATA_SERVER_NAME`.
- Metasrv follower→leader: `LAKE_PEER_AUTH_TOKEN_FILE`, `LAKE_PEER_CA_FILE`,
  `LAKE_PEER_SERVER_NAME`.
- Credential files may end in a newline; values are trimmed at that boundary
  and never logged or accepted as command-line flags.

## Query admission

- `LAKE_QUERY_MAX_CONCURRENT` (default 64)
- `LAKE_QUERY_QUEUE_TIMEOUT_MS` (default 100)
- `LAKE_QUERY_EXECUTION_TIMEOUT_MS` (default 1800000)
- `LAKE_QUERY_MAX_SQL_BYTES` (default 1048576)
- `LAKE_QUERY_MAX_DISCOVERY_ROWS` (default 10000)
- `LAKE_QUERY_DISCOVERY_BATCH_ROWS` (default 256; at most the row maximum)

All values are positive integers parsed once before serving.

## Lance maintenance policy

- `LAKE_LANCE_RETAIN_VERSIONS` (default 10, range 1..=10000)

The policy is parsed before local or cloud storage construction and remains
immutable for the process lifetime.

## Append operation policy

- `LAKE_APPEND_OPERATION_RETENTION_SECS` (default 604800)
- `LAKE_APPEND_OPERATION_GC_PAGE_SIZE` (default 128, maximum 10000)
- `LAKE_APPEND_MAX_CONCURRENT` (default 8)
- `LAKE_APPEND_QUEUE_TIMEOUT_MS` (default 100)
- `LAKE_APPEND_MAX_STREAM_BYTES` (default 67108864)
- `LAKE_APPEND_MAX_BUFFERED_BYTES` (default 268435456)

## Layout

- `main.rs` — clap `Cli` + dispatch
- `commands/mod.rs` — `Context` (local/cloud storage wiring: metastore + engine
  + metasrv)
- `commands/{selftest,sql,ingest,table,serve}.rs` — one per subcommand group
  (`table` covers create/list/drop; `ingest` loads a Parquet file)
- `lake query --metadata-addr <URI>` — metadata target for typed FILE append
  forwarding; SDK clients still receive only the query endpoint
