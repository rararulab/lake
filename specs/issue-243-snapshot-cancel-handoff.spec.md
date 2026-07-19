spec: task
name: "iceberg-snapshot-cancel-handoff"
inherits: project
tags: [iceberg, snapshot, cancellation, concurrency, resilience]
---

## Intent

External Iceberg snapshot resolution is single-flight per exact table. If its
leader request is cancelled, a follower that has already joined the load must
observe that the in-flight sender closed, become the replacement loader, and
complete without needing another caller to repair the state.

## Decisions

- Exercise the public `IcebergCatalog::resolve_snapshot` API through the
  existing gated in-memory catalog double. The gate makes the first exact
  external load deterministic without adding production test hooks.
- Start the follower before cancelling the leader. Release one permit only
  after cancellation, then require that follower to finish under a bounded
  timeout.
- Preserve all cache and error semantics. This is a regression lock for the
  existing drop cleanup path, not a retry policy, timeout, refresh, or REST
  behavior change.

## Boundaries

### Allowed Changes
crates/lake-iceberg/tests/catalog.rs
docs/design/iceberg-federation.md
specs/issue-243-snapshot-cancel-handoff.spec.md
verification/issue-243-snapshot-cancel-handoff.md

### Forbidden
crates/lake-iceberg/src/**
crates/lake-cli/**
crates/lake-query/**
crates/lake-meta/**
crates/lake-metasrv/**
Cargo.toml
Cargo.lock
Iceberg write, DDL, DML, commit, catalog mutation, or catalog enumeration
background refresh, timer, retry/backoff policy, circuit breaker, or negative cache
REST protocol, auth, TLS, proxy, DNS, credential, or object-storage behavior

## Completion Criteria

Rule: iceberg-snapshot-cancel-handoff — a cancelled snapshot-load leader does
  not strand followers already sharing its exact-table load

Scenario: A waiting Iceberg snapshot follower takes over after leader cancellation
  Test:
    Package: lake-iceberg
    Filter: cancelled_snapshot_leader_releases_existing_follower
    Level: integration
    Test Double: gated in-memory Iceberg catalog
  Given one immediate-refresh exact-table load is held at the external catalog
  And a second resolver has joined that in-flight load
  When the leading resolver is cancelled and the catalog allows one replacement load
  Then the existing follower returns the table snapshot under a bounded wait
  And exactly two exact external table loads have started

## Out of Scope

- Retrying an external failure, changing stale-if-error behavior, or changing
  successful snapshot caching.
- OAuth behavior, catalog configuration, or read-only federation policy.
