spec: task
name: "iceberg-cancel-metrics"
inherits: project
tags: [iceberg, metrics, cancellation, resilience]
---

## Intent

An Iceberg snapshot-load leader can be cancelled while an external catalog
request is in flight. The existing handoff regression proves that the next
caller is not stranded, while the federation metrics record the cancellation
as a bounded outcome. Bind those two contracts together so operators retain a
truthful cancellation signal and can distinguish it from an external catalog
failure without exposing an external workload identity.

## Decisions

- Reuse the semaphore-gated in-memory catalog and the real local Prometheus
  recorder already used by the federation regression suite.
- Cancel the first current-snapshot leader, allow one replacement load, and
  assert exactly one bounded `cancelled` and one subsequent `loaded` snapshot
  outcome, alongside exactly two external table loads.
- Assert the rendered scrape contains no configured namespace, table, endpoint,
  warehouse, or credential-looking value.
- This is telemetry-contract coverage only. It must not change cache, retry,
  timeout, OAuth, REST, cancellation, or read-only federation behavior.

## Boundaries

### Allowed Changes
crates/lake-iceberg/tests/catalog.rs
specs/issue-251-iceberg-cancel-metrics.spec.md
verification/issue-251-iceberg-cancel-metrics.md

### Forbidden
Cargo.toml
Cargo.lock
crates/lake-iceberg/src/**
crates/lake-cli/**
crates/lake-meta/**
crates/lake-metasrv/**
crates/lake-objects/**
crates/lake-query/**
Iceberg write, DDL, DML, commit, catalog mutation, or catalog enumeration
REST protocol, auth, TLS, proxy, DNS, credential, or object-storage behavior
new environment variables, telemetry listeners, metric endpoints, or dynamic metric labels
cache freshness, stale-if-error, capacity, retry, timeout, or cancellation behavior

## Completion Criteria

Rule: iceberg-cancel-metrics — a cancelled snapshot leader remains both
  recoverable and observable without identity labels

Scenario: Cancelling a snapshot-load leader emits a bounded handoff signal
  Test:
    Package: lake-iceberg
    Filter: cancelled_snapshot_leader_metrics_preserve_handoff_visibility
    Level: integration
    Test Double: semaphore-gated in-memory Iceberg catalog and Prometheus recorder
  Given an immediate-refresh Iceberg snapshot load blocked before its external table response
  When its leader is cancelled and a replacement caller completes the load
  Then the recorder exposes one bounded `cancelled` outcome followed by one `loaded` outcome
  And exactly two external table loads occur without a retry policy
  And no namespace, table, endpoint, warehouse, or credential value appears in the scrape

## Out of Scope

- New runtime metrics, labels, listeners, or telemetry endpoints.
- Metrics for OAuth cancellation, catalog internals, scans, bytes, or per-table
  identifiers.
- Changing the existing snapshot cache, stale-if-error, retry, timeout, OAuth,
  or cancellation policy.
