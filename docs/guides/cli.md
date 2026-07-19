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

### External Iceberg REST catalog

`lake query` may expose one read-only external Iceberg REST catalog as the SQL
catalog `iceberg`. Configure the Query process with all three of the following
variables, or none of them:

```bash
LAKE_ICEBERG_REST_ENDPOINT=https://catalog.example.com \
LAKE_ICEBERG_WAREHOUSE=s3://embodied-warehouse \
LAKE_ICEBERG_NAMESPACES=analytics,models \
LAKE_ICEBERG_REST_TIMEOUT_MS=10000 \
lake query --metadata-addr https://metasrv.example.com:50052
```

`LAKE_ICEBERG_REST_ENDPOINT` must be a credential-free HTTPS URL. Plain HTTP
is only valid for a numeric IP loopback development endpoint (`127.0.0.0/8` or
`::1`); hostnames, including `localhost`, are not a loopback exception.
`LAKE_ICEBERG_WAREHOUSE` is passed to the REST catalog, and
`LAKE_ICEBERG_NAMESPACES` is a comma-separated finite allowlist. Any partial
or invalid triple fails before Query binds. REST credentials and object-store
credentials are supplied by the deployment/runtime, never through CLI flags,
Lake metadata, or Flight tickets.

`LAKE_ICEBERG_REST_TIMEOUT_MS` is optional and defaults to `10000`; it must
be in `1..=60000`. Lake applies the resulting total and connect deadline to
each external REST configuration handshake, namespace point check, exact table
lookup, and OAuth exchange. The override without the base catalog triple, or a
malformed/out-of-range value, fails before Query binds.

An external catalog may be unauthenticated, or use exactly one of the following
runtime-only REST authentication modes:

| Variables | Mode |
|---|---|
| `LAKE_ICEBERG_REST_TOKEN` | Static bearer token |
| `LAKE_ICEBERG_REST_CREDENTIAL` | OAuth client credentials in `client-id:client-secret` form |
| `LAKE_ICEBERG_REST_OAUTH2_SERVER_URI` | Optional credential-free OAuth token endpoint; requires `..._CREDENTIAL` |
| `LAKE_ICEBERG_REST_OAUTH_SCOPE`, `LAKE_ICEBERG_REST_OAUTH_AUDIENCE`, `LAKE_ICEBERG_REST_OAUTH_RESOURCE` | Optional standard OAuth token parameters; each requires `..._CREDENTIAL` |

`LAKE_ICEBERG_REST_TOKEN` and `LAKE_ICEBERG_REST_CREDENTIAL` are mutually
exclusive. Auth variables without the base catalog triple, malformed OAuth
endpoint URLs, remote plaintext OAuth token endpoints, and empty/invalid values
fail before the listener binds. The overridden OAuth token endpoint follows the
same HTTPS-or-numeric-loopback rule as the catalog endpoint. Inject secrets
from the deployment secret manager as environment variables; do not put them in
endpoint URLs, CLI flags, config files committed to source control, or
diagnostic output.

When an OAuth client-credential session fails on a bounded namespace check or
exact table lookup, Query renews its in-memory token once and retries only that
read. Concurrent failures share one renewal. Static bearer tokens do not
refresh, and this is neither background refresh scheduling nor secret rotation.
If renewal or the one retry fails, Query returns the failure.

Query accepts `SELECT ... FROM iceberg.<namespace>.<table>` only for a
configured namespace. It does not enumerate external namespaces or tables in
Flight discovery; clients must use a full table name. The external catalog owns
its snapshots and commits. Iceberg DDL/DML is rejected, and Flight tickets pin
the selected Iceberg snapshot through `DoGet`.

`LAKE_LANCE_RETAIN_VERSIONS` controls the recent untagged dataset versions
preserved by engine maintenance. It defaults to `10` and must be within
`1..=10000`. Configuration is parsed before local or cloud storage opens;
malformed, zero, and larger values make startup exit non-zero. Lance tags and
referenced branches remain retained independently of this recent-version
window.

Query Flight SQL discovery is bounded by
`LAKE_QUERY_MAX_DISCOVERY_ROWS` (default `10000`) and
`LAKE_QUERY_DISCOVERY_BATCH_ROWS` (default `256`). Both must be positive and
the batch size cannot exceed the row maximum. Schema/table `DoGet` requests
share `LAKE_QUERY_MAX_CONCURRENT` admission. Each authenticated tenant is
additionally capped by `LAKE_QUERY_MAX_CONCURRENT_PER_TENANT` (default `8`),
with at most `LAKE_QUERY_MAX_TRACKED_TENANTS` (default `4096`, maximum `65536`)
live gates. The tenant cap cannot exceed the global cap. One total
`LAKE_QUERY_QUEUE_TIMEOUT_MS` covers tenant-first and global acquisition;
queue/tracker saturation or a response beyond the matching-row maximum returns
gRPC `ResourceExhausted`. The policy is per Query replica and does not claim a
cluster-global quota.

Durable async execution has a separate bounded worker scheduler.
`LAKE_ASYNC_WORKER_CONCURRENCY` defaults to `4` and must be within `1..=64`;
`LAKE_ASYNC_WORKER_CONCURRENCY_PER_TENANT` defaults to `1` and cannot exceed
the worker total. `LAKE_ASYNC_EXECUTION_TIMEOUT_MS` defaults to `1800000` and
must be within `1..=86400000`. Eligible tenants are selected round-robin from
each bounded state page, and saturated tenants do not occupy worker slots while
waiting. Deadline expiry cancels execution and lease renewal and records a
stable terminal failure. All three values are per Query replica.

Set `LAKE_ASYNC_GLOBAL_WORKER_CONCURRENCY` and
`LAKE_ASYNC_GLOBAL_WORKER_CONCURRENCY_PER_TENANT` together to opt into one
shared execution ceiling across replicas that use the same dedicated async
state store. Both values must be in `1..=64`, and the tenant value cannot
exceed the total. An absent pair leaves cluster coordination disabled; a
partial, malformed, zero, excessive, or contradictory pair fails before Query
binds. This limit controls only running background executions: local bounded
page selection remains process-local, so it does not promise a global queue or
strict dispatch order.

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

`lake catalog-finalize --acknowledge-writer-rollout
--acknowledge-write-quiescence` monotonically enables generation-gated Query
catalog refresh. Run it only after every registry writer is on compatible code
and metadata write admission is paused. The command is idempotent and supports
`--json`; there is no routine unfinalize command. Old Query processes remain
safe, but an old writer must never run after this acknowledgement.

Leader table maintenance uses `LAKE_MAINTENANCE_INTERVAL_SECS` (default `60`)
and `LAKE_MAINTENANCE_TABLE_PAGE_SIZE` (default `128`, maximum `10000`). Each
tick reads at most one registry page and resumes from a process-local cursor;
invalid or zero values fail before the Metasrv listener binds. Dataset cleanup
uses the immutable `LAKE_LANCE_RETAIN_VERSIONS` policy captured at process
startup, then reconciles external manifest history only after cleanup succeeds.

Append-operation cleanup uses `LAKE_APPEND_OPERATION_GC_PAGE_SIZE` (default
`128`) per physical metadata page and
`LAKE_MAINTENANCE_OPERATION_GC_MAX_PAGES` (default `16`, maximum `10000`) per
tick. `LAKE_MAINTENANCE_OPERATION_GC_MAX_MS` adds a whole-stage wall-clock
deadline (default `10000`, maximum `60000`). A tick follows its process-local
cursor until it reaches end-of-scan or either budget; it advances the cursor
only after a complete page and never wraps to reread the beginning in the same
tick. Deadline cancellation retains the durable operation record, so exact
cleanup retries safely on a later pass.
The defaults raise the scan ceiling from 128 to 2048 records per minute
(roughly 34 records/second before reconciliation cost) while keeping drop and
table maintenance schedulable. Size the product of page size and page budget
above the sustained append rate times the maintenance interval. A sustained
positive rate of `lake_metasrv_maintenance_items_total{stage="append_operations",outcome=~"budget_exhausted|time_exhausted"}`
means a configured ceiling is continuously consumed; compare the `deleted`
rate with append throughput before raising page size, page count, or duration.

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

## Prometheus metrics

Set `LAKE_METRICS_ADDR` on `lake query` or `lake meta` to enable the
Prometheus text endpoint at `GET /metrics`:

```bash
LAKE_METRICS_ADDR=127.0.0.1:9090 lake query \
  --addr 127.0.0.1:50051 \
  --metadata-addr http://127.0.0.1:50052
```

The value must be an IP loopback socket. Hostnames, wildcard addresses, and
non-loopback IPs fail startup before the Flight listener binds. In a production
pod, use a localhost Prometheus sidecar or node agent to scrape and forward the
endpoint; Lake does not create an anonymous network-wide telemetry surface.
Only `GET /metrics` succeeds; HEAD, other methods, and other paths are rejected.

Core series are:

- `lake_process_info` with fixed `service` and build `version` labels.
- `lake_query_admission_total`, `lake_query_inflight_requests`,
  `lake_query_rejections_total`, `lake_query_catalog_refresh_total`, and
  `lake_query_ready`.
- `lake_iceberg_snapshot_resolution_total`,
  `lake_iceberg_catalog_operation_total`, and
  `lake_iceberg_oauth_refresh_total` when the Query replica has an external
  Iceberg catalog configured.
- `lake_metasrv_append_admission_total`, `lake_metasrv_inflight_appends`,
  `lake_metasrv_reserved_append_bytes`, `lake_metasrv_campaign_total`,
  `lake_metasrv_write_ready`, `lake_metasrv_maintenance_pages_total`, and
  `lake_metasrv_maintenance_items_total`.
- `lake_dynamo_v2_authoritative`, `lake_dynamo_finalize_barrier_held`,
  `lake_dynamo_prefix_requests_total`, and `lake_dynamo_prefix_items_total`
  when Dynamo is the metastore.

Labels are finite state-machine values such as `success`, `error`, `leader`,
or `saturated`. SQL, tenant, namespace, table, operation ID, URI, and credential
values are never metric labels. The listener and exporter upkeep future share
the server future directly: normal shutdown joins it, while dropping the outer
server future drops the listener without leaving detached work.

After a v2 migration, alert on runtime targets selected by
`max by (service, instance) (lake_dynamo_v2_authoritative) == 0`, and require the rate of
`lake_dynamo_prefix_requests_total{layout="v1"}` to fall to zero. Prefix-read
amplification is the ratio of evaluated to returned
`lake_dynamo_prefix_items_total` rates after `sum by (layout, api, service,
instance)` removes the differing `kind` label. Physical Query fan-out uses the
successful request rate divided by the returned-item rate; no prefix label is
needed. Detect a missing series per pod by reconciling scrape inventory:
`max by (service, instance) (up{job=~"lake-query|lake-metasrv"}) unless on
(service, instance) max by (service, instance)
(lake_dynamo_v2_authoritative)`. The scrape configuration must relabel `up`
with the same `service` and `instance` identity, and the alert rule should use
`for: 5m`. A held finalization barrier together with a v1-authoritative
pod means the post-finalize restart is incomplete and metadata write admission
must remain paused. See the full bounded-label contract in
[`dynamo-prefix-metrics.md`](../design/dynamo-prefix-metrics.md).

## OTLP distributed tracing

Trace export is disabled unless `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` (preferred)
or `OTEL_EXPORTER_OTLP_ENDPOINT` is set to an HTTP(S) collector origin:

```bash
OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://127.0.0.1:4317 \
OTEL_SERVICE_NAME=lake-query \
lake query --addr 127.0.0.1:50051 \
  --metadata-addr http://127.0.0.1:50052
```

The endpoint must not contain credentials, a path, query, or fragment. Lake
uses OTLP/gRPC, a fixed 2,048-span queue and 256-span export batch. Set
`LAKE_OTLP_SHUTDOWN_TIMEOUT_MS` to `1..=30000` milliseconds (default `5000`)
to bound the final export, exporter shutdown, and worker join. Collector
unavailability drops telemetry within those bounds; it does not fail a running
Query or Metasrv command.

Flight calls propagate only W3C `traceparent` and `tracestate`. Server spans
use fixed RPC names and the bounded fields `rpc.system`, `rpc.service`,
`rpc.method`, and `rpc.outcome`. SQL, tenant, principal, namespace, table,
object URI/path, credentials, media type, action bodies, and operation IDs are
neither span attributes nor propagated baggage. Existing JSON/pretty logs stay
enabled when OTLP is active.

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
