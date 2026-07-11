# CLI Standards

`lake` is the all-in-one operator and agent interface for the project. It is
not just the e2e self-check binary.

## Shape

- Use `clap` derive (`Parser`, `Subcommand`, `Args`, `ValueEnum`) for all
  command parsing.
- The binary owns subcommands. Expected top-level direction:
  - `lake self-check` — local ingest -> commit -> SQL smoke test.
  - `lake serve ...` — run lake services, including future Flight SQL /
    control-plane services.
  - `lake client ...` — client/operator commands that talk to a running server.
  - `lake admin ...` — explicit maintenance/debug operations.
- New user-visible behavior lands as a subcommand or subcommand group; do not
  grow one huge flag bag on the root command.

## Agent-Friendly Output

- Errors and progress go to stderr. Data goes to stdout.
- Commands that produce data must support a machine-readable mode before they
  are considered agent-friendly. Prefer `--format json` for structured output.
- JSON output must be stable enough for scripts: no decorative text, no tables,
  no ANSI color.
- Human table output is fine, but it is a presentation mode, not the contract.
- Exit code `0` means success; non-zero means the requested operation did not
  complete. Do not hide partial failures behind printed warnings.

## Configuration

- Precedence is CLI args > env vars > config file > defaults.
- Config defaults live at the application boundary. Domain crates do not hide
  operational defaults.
- All path/endpoint flags use explicit names such as `--data-dir`,
  `--catalog-url`, or `--endpoint`; avoid ambiguous positional arguments for
  operational commands.

Query Flight SQL discovery is bounded by
`LAKE_QUERY_MAX_DISCOVERY_ROWS` (default `10000`) and
`LAKE_QUERY_DISCOVERY_BATCH_ROWS` (default `256`). Both must be positive and
the batch size cannot exceed the row maximum. Schema/table `DoGet` requests
share `LAKE_QUERY_MAX_CONCURRENT` admission; queue saturation or a response
that exceeds the matching-row maximum returns gRPC `ResourceExhausted`.

Metasrv FILE append admission uses `LAKE_APPEND_MAX_CONCURRENT` (default `8`),
`LAKE_APPEND_QUEUE_TIMEOUT_MS` (default `100`),
`LAKE_APPEND_MAX_STREAM_BYTES` (default `67108864`), and
`LAKE_APPEND_MAX_BUFFERED_BYTES` (default `268435456`). All must be positive;
the process buffer must hold at least one maximum-sized stream and both byte
values must fit weighted semaphore permits. Each request reserves its complete
per-stream maximum until forwarding or local commit finishes, so saturation
returns gRPC `ResourceExhausted` before payload polling.

`LAKE_SHUTDOWN_GRACE_MS` (default `30000`) is the total Metasrv shutdown
budget, beginning when SIGINT or SIGTERM is received. It covers Flight
connection drain plus maintenance, leadership-campaign, and health-readiness
cleanup; unfinished owned background tasks are aborted at the deadline and the
process returns a typed error instead of waiting indefinitely.

Leader table maintenance uses `LAKE_MAINTENANCE_INTERVAL_SECS` (default `60`)
and `LAKE_MAINTENANCE_TABLE_PAGE_SIZE` (default `128`, maximum `10000`). Each
tick reads at most one registry page and resumes from a process-local cursor;
invalid or zero values fail before the Metasrv listener binds.

## gRPC health checks

Query and Metasrv register the standard `grpc.health.v1.Health` service on the
same port as Flight. Probes therefore use the same TLS trust roots and
`authorization: Bearer <token>` metadata as every other RPC; there is no
anonymous probe endpoint.

- Check service `""` for process liveness.
- Check service `"arrow.flight.protocol.FlightService"` for traffic readiness.
- Query readiness becomes `SERVING` only after the initial catalog refresh.
- Metasrv readiness is `SERVING` only with a live local lease or a known remote
  leader that can receive forwarded writes.

Use any generated gRPC Health client to call `Check` for polling or `Watch` for
streaming transitions. During graceful shutdown both service names publish
`NOT_SERVING` before Tonic begins connection drain, so a watcher can remove the
node before the process exits.

## Process logging

The binary installs its tracing subscriber before command parsing, storage
opening, or listener binding. Logs always use stderr so stdout remains the
deterministic data channel.

- `LAKE_LOG_FORMAT=json|pretty` selects newline-delimited JSON (the default)
  or human-readable output. ANSI is disabled in both modes.
- `RUST_LOG` supplies a standard tracing filter. Without it, lake's binary,
  query, metasrv, and catalog targets log at INFO while dependencies remain
  quiet.
- Invalid explicit values fail startup. Startup records contain the package
  version only; argv, SQL, paths, environment values, credentials, and tokens
  are not logging fields.

## Async Runtime

- The CLI is async-first. Use `#[tokio::main]` at the binary boundary and keep
  I/O operations async through command handlers.
- Blocking local filesystem or CPU-heavy work must be isolated; do not hold
  async locks across `.await`.
- DataFusion APIs are async at the query boundary (`SessionContext::sql`,
  `DataFrame::collect`), so CLI query paths should remain async instead of
  wrapping them in sync helper APIs.

## References

- clap derive subcommands: <https://docs.rs/clap/latest/clap/_derive/_tutorial/index.html>
- Tokio runtime: <https://docs.rs/tokio>
- DataFusion SQL API: <https://datafusion.apache.org/library-user-guide/using-the-sql-api.html>
