spec: task
name: "catalog-stale-while-revalidate"
inherits: project
tags: [catalog, query, cache, availability]
---

## Intent

The query replica already owns an immutable last-good catalog snapshot, but
the first SQL planner after its TTL expires waits for a full metadata scan and
all concurrent planners queue behind the same lock. A slow or unavailable
metadata authority therefore stops reads that could safely use the cached
generation. Runtime refresh must move off the request path without weakening
startup validation or publishing partial snapshots.

## Decisions

- A never-warmed catalog refreshes synchronously and propagates authority
  failure; the replica must not claim readiness with an empty accidental view.
- Once warmed, stale checks return immediately with the last-good generation
  and atomically start at most one detached revalidation.
- Build the replacement privately and publish it only after the complete scan
  succeeds. Failure preserves the prior snapshot and records bounded local
  health state.
- The server background loop may force refresh on its own schedule, but SQL
  planning never waits for runtime authority I/O.
- Cancellation and shutdown behavior remain owned by the Query server task;
  no durable state or cross-replica cache protocol is introduced.

## Boundaries

### Allowed Changes
Cargo.lock
crates/lake-catalog/**
crates/lake-query/**
docs/architecture.md
docs/plans/2026-07-12-catalog-swr.md
specs/issue-47-catalog-swr.spec.md
verification/issue-47-catalog-swr.md

### Forbidden
crates/lake-engine-lance/**
crates/lake-meta/**
crates/lake-metasrv/**
crates/lake-sdk/**

## Completion Criteria

Scenario: Initial warm remains synchronous and fail-closed
  Test:
    Package: lake-catalog
    Filter: initial_refresh_waits_and_propagates_failure
  Given a catalog has never published a registry generation
  When its first stale check reaches an unavailable authority
  Then the caller waits for that attempt and receives the failure

Scenario: Stale planners use last-good without waiting
  Test:
    Package: lake-catalog
    Filter: stale_checks_return_while_one_refresh_runs
  Given a warmed catalog whose refresh age has expired
  When many callers check freshness while the next scan is paused
  Then every caller returns with the last-good snapshot and exactly one refresh runs

Scenario: Refresh failure preserves service and health state
  Test:
    Package: lake-catalog
    Filter: failed_revalidation_preserves_last_good_snapshot
  Given a warmed catalog and a failing runtime revalidation
  When SQL continues reading the catalog
  Then the prior immutable generation remains available and health records the failure

Scenario: Recovery atomically publishes a new generation
  Test:
    Package: lake-catalog
    Filter: successful_revalidation_publishes_recovered_generation
  Given a failed revalidation left the old snapshot intact
  When the authority recovers and the next revalidation completes
  Then readers observe the complete new generation and failure health clears

Scenario: SQL planning does not await runtime refresh
  Test:
    Package: lake-query
    Filter: warm_sql_planning_continues_during_slow_catalog_refresh
  Given a query engine has a warmed catalog and a slow runtime authority scan
  When a SQL statement is planned after the refresh age expires
  Then planning completes from last-good before the authority scan is released

Scenario: Query shutdown owns detached revalidation
  Test:
    Package: lake-catalog
    Filter: shutdown_aborts_inflight_revalidation
  Given a request-triggered revalidation is stuck in authority I/O
  When the query replica shuts down its catalog
  Then the in-flight task is aborted and joined without waiting for that I/O

Scenario: Fallible startup precedes background task creation
  Test:
    Package: lake-query
    Filter: startup_configuration_failure_does_not_leak_refresher
  Given query server address or security setup is invalid
  When startup returns that configuration error
  Then no catalog refresher retains the query engine

Scenario: Cancelling the serve future stops all refresh tasks
  Test:
    Package: lake-query
    Filter: aborting_server_future_releases_refresh_tasks
  Given scheduled refresh and request-triggered revalidation are owned by a query server
  When its serve future is aborted instead of gracefully shut down
  Then a drop guard cancels both task paths and releases their engine state

## Out of Scope

- Serving before the initial catalog warm succeeds.
- Changing the registry TTL or cross-replica consistency window.
- Persisting health or cache state.
- Retrying metadata requests inside the catalog.
