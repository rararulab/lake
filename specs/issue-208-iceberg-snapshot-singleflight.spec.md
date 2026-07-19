spec: task
name: "iceberg-snapshot-singleflight"
inherits: project
tags: [iceberg, rest, cache, concurrency, query]
---

## Intent

Protect the Query tier's external Iceberg REST catalog from fleet-read
fan-out. A cold or expired snapshot-cache entry currently lets every
concurrent planner point-load the same external table. One cache miss can
therefore amplify to one REST table lookup per reader, despite all callers
needing the same immutable snapshot.

This advances the `goal.md` requirement that the stateless Query layer absorbs
read fan-out and shields bounded metadata authorities. It preserves the
external catalog as Iceberg's metadata authority, Lake's read-only federation
boundary, and snapshot pinning in Flight tickets.

## Decisions

- Single-flight is per configured Iceberg namespace/table key. A cold load and
  a freshness refresh share one exact bounded external lookup; unrelated table
  keys continue independently.
- Waiters receive the leader's selected snapshot or its existing
  stale-if-error fallback. External failures are never retained as cache
  entries, and a cancelled leader leaves later callers able to establish a new
  load.
- Cache coordination never holds a synchronous or asynchronous lock over an
  external catalog request. The existing 10,000-entry snapshot capacity,
  5-second freshness, 60-second stale-if-error policy, allowlist, and OAuth
  renewal semantics remain unchanged.

## Boundaries

### Allowed Changes
crates/lake-iceberg/src/lib.rs
crates/lake-iceberg/tests/catalog.rs
docs/design/iceberg-federation.md
specs/issue-208-iceberg-snapshot-singleflight.spec.md

### Forbidden
crates/lake-meta/**
crates/lake-metasrv/**
crates/lake-catalog/**
crates/lake-query/**
crates/lake-flight/**
Lake registry or ticket-schema changes
Iceberg write, DDL, DML, commit, catalog mutation, or catalog enumeration
background refresh loops, retry/circuit-breaker policies, or negative caching
new REST auth, TLS, CA, proxy, DNS, or credential behavior
credentials in metadata, SQL, tickets, logs, metrics, or URLs

## Completion Criteria

Rule: iceberg-snapshot-singleflight — an external cache miss does not amplify
  into per-reader catalog traffic

Scenario: Concurrent snapshot refresh shares one external table load
  Test:
    Package: lake-iceberg
    Filter: concurrent_snapshot_refreshes_share_one_external_load
  Given an expired configured Iceberg table snapshot and concurrent Query
    planners resolving that exact namespace/table
  When all planners arrive while the external table load is still pending
  Then exactly one external table load occurs and every planner receives the
    selected immutable snapshot

Scenario: Refresh failure preserves the last-good snapshot
  Test:
    Package: lake-iceberg
    Filter: configured_namespace_cache_never_enumerates_unconfigured_catalog_state
  Given a previously resolved configured Iceberg table whose freshness window
    has expired
  When its next exact external table load fails within stale-if-error
  Then the last successful immutable snapshot remains available without any
    namespace or table enumeration

Scenario: A cancelled cache-load leader does not strand a later caller
  Test:
    Package: lake-iceberg
    Filter: cancelled_snapshot_leader_allows_a_new_load
  Given a configured Iceberg table load whose leader is blocked in the
    external catalog request
  When that leader is cancelled before the request completes
  Then a later caller establishes a replacement point load and receives the
    immutable snapshot

## Out of Scope

- Cross-replica cache distribution or a shared external catalog cache.
- Changes to Iceberg snapshot-retention policy, Flight ticket contents, or
  external catalog availability behavior beyond per-key request coalescing.
