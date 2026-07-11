spec: task
name: "query-admission-control"
inherits: project
tags: [query, flight, limits, concurrency, resilience]
---

## Intent

Keep each stateless Query replica responsive under bursty fleet fan-out by
bounding concurrent execution, queue wait, execution duration, and SQL/ticket
size. Admission remains process-local and never adds per-query metadata load.

## Decisions

- `QueryLimits` is immutable server configuration with finite defaults and
  rejects zero/invalid values at startup.
- Planning and execution acquire owned semaphore permits. A DoGet permit stays
  alive for the result stream and releases on completion, deadline, or drop.
- Queue timeout returns `ResourceExhausted`; execution timeout returns
  `DeadlineExceeded` and drops the underlying DataFusion stream.
- SQL and raw-SQL ticket byte limits are checked before planning.
- CLI environment values are parsed once at startup and passed through
  `QueryServerConfig`; no request reads process environment.

## Boundaries

### Allowed Changes
- `crates/lake-query/**`
- `crates/lake-cli/**`
- `README.md`
- `docs/architecture.md`
- `docs/design/**`
- `docs/plans/**`
- `specs/**`
- `verification/**`
- `**/.github/**`
- `**/AGENT.md`
- `**/CLAUDE.md`
- `**/docs/guides/mise-ci.md`
- `**/docs/guides/workflow.md`
- `**/mise.toml`

The final six patterns account for shared-checkout history from merged issue
12 that the repository-wide worktree verifier still reports. This workspace
does not edit those paths.

### Forbidden
- Per-query calls to Metasrv or the durable metastore for admission
- Releasing a concurrency permit when DoGet returns but before its stream ends
- Silently ending a timed-out result stream without `DeadlineExceeded`
- Unbounded or zero production defaults
- Retrying rejected requests inside Query

## Completion Criteria

Scenario: saturated Query rejects queued execution and releases on stream drop
  Test:
    Package: lake-query
    Filter: query_admission_rejects_when_saturated_and_releases_on_drop
  Given one admitted slow DoGet stream and concurrency limit one
  When a second query waits beyond the queue deadline
  Then it receives ResourceExhausted, and dropping the first stream lets the
  next query acquire the released permit

Scenario: query execution deadline terminates a slow result stream
  Test:
    Package: lake-query
    Filter: query_execution_deadline_terminates_slow_stream
  Given a streaming query that stalls after its first batch
  When its configured execution duration elapses
  Then the stream returns DeadlineExceeded and releases its permit

Scenario: oversized SQL and tickets fail before planning
  Test:
    Package: lake-query
    Filter: oversized_sql_and_ticket_are_rejected_before_planning
  Given a small configured SQL byte limit
  When GetFlightInfo SQL or a DoGet statement handle exceeds it
  Then Query returns ResourceExhausted without touching the catalog

Scenario: valid streaming remains incremental under admission control
  Test:
    Package: lake-query
    Filter: do_get_returns_before_the_input_stream_finishes
  Given a query whose later batch is delayed
  When DoGet is called within limits
  Then it still returns the result stream before the producer finishes

Scenario: CLI query limits reject invalid environment values
  Test:
    Package: lake-cli
    Filter: query_limit_values_are_validated_before_serving
  Given zero, malformed, or valid query-limit values
  When CLI builds QueryLimits at startup
  Then invalid values fail with actionable errors and valid values produce the
  exact immutable limits

## Out of Scope

- Tenant-specific quotas or fair queuing
- Distributed/global admission across Query replicas
- DataFusion memory-pool/spill tuning
- Client retry/backoff policy
- Durable async query state and cancellation RPCs
