spec: task
name: "iceberg-async"
inherits: project
tags: [iceberg, query, flight, async, snapshots]
---

## Intent

Lake's durable Flight SQL path currently accepts native `lake.*` reads but
rejects every `iceberg.*` reference with `FailedPrecondition: asynchronous
Iceberg queries are not supported`. This leaves the long-running external
scans that benefit most from durable async execution on a foreground stream.

Reproducer: configure the existing read-only Iceberg catalog, submit
`SELECT * FROM iceberg.analytics.episodes` through `PollFlightInfo`, and
observe the explicit rejection before the encrypted job is persisted. A large
Iceberg scan therefore cannot use Lake's bounded worker, result-part, polling,
or cancellation lifecycle.

This advances `goal.md`'s requirement that stateless Query replicas absorb
bursty concurrent read fan-out while reading storage directly. It preserves
Iceberg as the external metadata authority, Lake federation as read-only, and
the existing exact-snapshot contract: a durable job must execute only the
Iceberg snapshot selected at submission, never the catalog's current head.

## Decisions

- Reuse the existing encrypted `StatementTicket` Iceberg snapshot entries; do
  not add a second async ticket format or persist endpoint, warehouse,
  credentials, object URLs, or raw SQL in async coordination records.
- The worker reconstructs each exact Iceberg provider by snapshot ID through
  `QueryEngine::resolve_iceberg_snapshot_at`, matching synchronous `DoGet`.
  If the external catalog no longer retains that ID, the job fails closed;
  it must never fall forward to the current snapshot.
- Retain all existing async admission, tenant quotas, leases, result bounds,
  deadlines, polling, cancellation, and read-only SQL rules. The change only
  makes the already-pinned external snapshot executable by a durable worker.
- Document that async SQL supports the same configured read-only Iceberg
  tables as synchronous SQL; Iceberg DDL/DML and catalog enumeration remain
  unsupported.

## Boundaries

### Allowed Changes
crates/lake-query/src/async_query.rs
crates/lake-query/src/flight.rs
README.md
docs/design/iceberg-federation.md
specs/issue-212-iceberg-async.spec.md
verification/issue-212-iceberg-async.md

### Forbidden
crates/lake-meta/**
crates/lake-metasrv/**
crates/lake-catalog/**
crates/lake-iceberg/**
crates/lake-sdk/**
crates/lake-objects/**
Cargo.toml
Cargo.lock
Lake registry, async-record, or statement-ticket schema changes
Iceberg write, DDL, DML, commit, catalog mutation, or catalog enumeration
latest-snapshot fallback when a pinned Iceberg snapshot is unavailable
new async quotas, worker scheduler behavior, retry loops, or configuration
credentials, endpoint URLs, object URLs, Arrow batches, or SQL in async state

## Completion Criteria

Rule: durable-async-iceberg — configured external scans use the standard
  durable Flight lifecycle without weakening snapshot pinning

Scenario: An external Iceberg scan is submitted and materialized asynchronously
  Test:
    Package: lake-query
    Filter: async_iceberg_submission_executes_pinned_snapshot
  Given an authenticated principal allowed to read a configured Iceberg
    namespace and an async Query coordinator
  When it submits `SELECT * FROM iceberg.analytics.episodes` through
    `PollFlightInfo` and a worker claims the durable job
  Then the job completes, its standard poll result can be read, and it returns
    the external table's rows without a native Lake table or registry lookup

Scenario: A queued external scan cannot fall forward after the catalog advances
  Test:
    Package: lake-query
    Filter: async_iceberg_submission_executes_pinned_snapshot
  Given a durable Iceberg job whose selected snapshot contains one episode
  When the external catalog commits another episode before the worker executes
  Then the worker result contains only the originally selected snapshot and no
    current-snapshot fallback occurs

Scenario: A no-longer-retained external snapshot fails closed
  Test:
    Package: lake-query
    Filter: async_iceberg_worker_rejects_unretained_snapshot
  Given a durable job whose encrypted statement names an Iceberg snapshot ID
    absent from the configured external table
  When a worker claims that job
  Then execution fails without publishing a result and never substitutes the
    table's current snapshot

Scenario: Existing async native-table behavior remains covered
  Test:
    Package: lake-query
    Filter: poll_flight_info_submits_identity_bound_pinned_job
  Given an authenticated native Lake table query
  When it is submitted, claimed, and read through the async Flight lifecycle
  Then its identity-bound handle, durable state, and bounded result endpoint
    remain usable across Query replicas

## Out of Scope

- Additional Iceberg catalog types, multiple configured catalogs, write
  federation, or external catalog discovery.
- Async Iceberg schema evolution behavior beyond the already selected immutable
  snapshot.
- Changes to the shared async state schema, result storage format, scheduling,
  resource policies, or SDK API shape.
