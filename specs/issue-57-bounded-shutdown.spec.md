spec: task
name: "bounded-metasrv-shutdown"
inherits: project
tags: [metasrv, shutdown, maintenance, leadership, lifecycle]
---

## Intent

The configured Metasrv shutdown grace must bound the complete owned process
lifecycle, not only tonic connection drain. Today maintenance and leadership
campaign tasks are joined without a deadline after Flight stops. A stuck engine
maintenance call or metadata operation can therefore prevent a pod from
terminating indefinitely after SIGTERM.

## Decisions

- Start one total shutdown deadline when the shutdown future resolves; Flight
  draining and all background-task cleanup share the same budget.
- Maintenance observes cancellation before waiting for a table lock and before
  beginning every later table. It may finish the current per-table operation
  within the remaining grace period.
- Join maintenance and leadership campaign concurrently so one stuck task does
  not prevent the other from resigning or completing.
- At the deadline, abort every unfinished owned background task, await each
  aborted handle, release process resources, and return a typed background
  drain timeout unless an earlier Flight drain error already takes precedence.
- A server that exits without an explicit shutdown signal still gets one grace
  interval for owned-task cleanup.
- Keep crash-simulation behavior and durable mutation protocols unchanged.

## Boundaries

### Allowed Changes
crates/lake-metasrv/**
docs/architecture.md
docs/guides/cli.md
docs/plans/2026-07-12-bounded-metasrv-shutdown.md
specs/issue-57-bounded-shutdown.spec.md
verification/issue-57-bounded-shutdown.md

### Forbidden
crates/lake-query/**
crates/lake-meta/**
crates/lake-engine*/**
crates/lake-sdk/**
durable metadata formats
Flight wire protocols
lease duration or election semantics

## Completion Criteria

Scenario: Total deadline aborts and joins stuck owned tasks
  Test:
    Package: lake-metasrv
    Filter: background_shutdown_aborts_owned_tasks_at_total_deadline
  Given maintenance and campaign tasks that never finish and retain shared resources
  When their cleanup reaches the total shutdown deadline
  Then both tasks are aborted and joined, their resources are released, and a typed timeout is returned

Scenario: Cancelled maintenance does not begin another table
  Test:
    Package: lake-metasrv
    Filter: maintenance_shutdown_stops_before_next_table
  Given a sweep with two tables whose first maintenance operation is paused
  When shutdown is cancelled and the first operation is allowed to finish
  Then the sweep returns without invoking maintenance for the second table

## Out of Scope

- Interrupting a durable engine transaction before the total deadline.
- Changing maintenance frequency, pagination, or table prioritization.
- Query-server shutdown behavior or deployment manifests.
- Changing leader lease acquisition, renewal, or fencing rules.
