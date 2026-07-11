spec: task
name: "graceful-flight-shutdown"
inherits: project
tags: [runtime, shutdown, flight, query, metasrv, ha]
---

## Intent

Make Query and Metasrv terminate deterministically during rolling deployment:
stop accepting new Flight connections, bound the drain window for in-flight
RPCs, cancel and join owned background loops, and have a departing metadata
leader release its lease immediately.

## Decisions

- Shutdown-aware serve entry points accept an injected future; existing serve
  helpers preserve their forever-running embedding API by using a pending
  future.
- One serve invocation owns one cancellation token and every background task
  it spawns. Shutdown always joins those tasks before returning.
- Tonic begins graceful connection drain when shutdown fires. A finite
  `shutdown_grace` bounds drain; exceeding it closes the server and returns a
  typed error.
- Query catalog refresh selects between its interval and cancellation.
- Metasrv campaign cancellation best-effort resigns the lease, clears local
  leadership, and exits; maintenance cancellation exits without starting a
  new sweep.
- CLI SIGINT/SIGTERM handling feeds the injectable serve API. Signals are not
  hidden inside domain crates.

## Boundaries

### Allowed Changes
- `Cargo.toml`
- `Cargo.lock`
- `crates/lake-query/**`
- `crates/lake-metasrv/**`
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
- Detached server-owned refresh, campaign, or maintenance tasks
- Waiting for the full metadata lease TTL after an orderly leader shutdown
- Treating drain timeout as a successful clean shutdown
- Catching process signals inside reusable Query or Metasrv libraries
- Bypassing TLS/authentication during shutdown or readiness transitions

## Completion Criteria

Scenario: Query shutdown releases listener and joins catalog refresher
  Test:
    Package: lake-query
    Filter: query_shutdown_releases_listener_and_joins_refresher
  Given a live Query server and owned catalog refresh task
  When its injected shutdown trigger fires
  Then serve returns, the listen address can be rebound, and no refresh scan
  occurs after the task join

Scenario: Query gives an in-flight stream its bounded drain window
  Test:
    Package: lake-query
    Filter: query_shutdown_drains_inflight_stream_within_grace
  Given a DoGet stream blocked after its first batch
  When shutdown fires and the producer finishes inside the configured grace
  Then the client receives completion and the server serve future returns Ok

Scenario: Query reports drain timeout for a stuck stream
  Test:
    Package: lake-query
    Filter: query_shutdown_reports_drain_timeout
  Given an in-flight DoGet that does not finish
  When shutdown grace expires
  Then serve returns the typed drain-timeout error and releases the listener

Scenario: Metasrv campaign shutdown resigns and clears leadership
  Test:
    Package: lake-metasrv
    Filter: campaign_shutdown_resigns_and_clears_leadership
  Given a node holding a valid metadata lease
  When campaign cancellation fires
  Then the lease is removed immediately, local writes are no longer authorized,
  and the campaign task returns

Scenario: Metasrv server joins campaign and maintenance on shutdown
  Test:
    Package: lake-metasrv
    Filter: metasrv_shutdown_releases_listener_and_background_tasks
  Given a serving Metasrv node with owned background loops
  When its injected shutdown trigger fires
  Then serve returns only after both loops exit and the listen address is free

Scenario: CLI exposes one cross-platform shutdown future
  Test:
    Package: lake-cli
    Filter: server_commands_use_injected_shutdown_path
  Given Query and Meta server commands
  When their command handlers are inspected and compiled
  Then both route SIGINT/SIGTERM through shutdown-aware library entry points
  rather than calling forever-running serve helpers

## Out of Scope

- HTTP/gRPC health and readiness endpoints
- Distributed tracing and metrics export
- Draining traffic at the external load balancer
- Persisting in-flight query state for restart/resume
- Graceful SDK upload checkpointing
