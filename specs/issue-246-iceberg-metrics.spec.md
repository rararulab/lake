spec: task
name: "iceberg-metrics"
inherits: project
tags: [iceberg, metrics, observability, resilience]
---

## Intent

Query replicas already bound every external Iceberg REST operation, but the
Prometheus endpoint exposes no federation-specific outcome. When an upstream
catalog slows, fails, or repeatedly requires OAuth renewal, an operator cannot
distinguish those conditions from a generic Query failure or prove that the
snapshot cache is shielding the external authority. Expose small, bounded,
identity-free counters for the existing read path without changing its cache,
retry, authentication, or SQL behavior.

## Decisions

- Record counters in `lake-iceberg`, at the existing cache, exact-table load,
  namespace verification, and OAuth-refresh decision points. The library owns
  those outcomes; Query only registers their descriptions with its existing
  Prometheus setup.
- Every label value is a finite protocol outcome or operation name. Namespace,
  table, endpoint, warehouse, credential, SQL, tenant, and principal values
  are forbidden as labels or metric values.
- Use monotonic counters only. Do not add sampling, tracing, timers,
  background work, cache configuration, retries, or a new telemetry listener.
- Verify the normal cache/load path and an external-load error path through a
  Prometheus test recorder, asserting emitted bounded labels and the absence
  of data identities.

## Boundaries

### Allowed Changes
crates/lake-iceberg/Cargo.toml
Cargo.lock
crates/lake-iceberg/src/lib.rs
crates/lake-iceberg/tests/catalog.rs
crates/lake-query/src/telemetry.rs
docs/design/iceberg-federation.md
docs/guides/cli.md
specs/issue-246-iceberg-metrics.spec.md
verification/issue-246-iceberg-metrics.md

### Forbidden
Cargo.toml
crates/lake-cli/**
crates/lake-meta/**
crates/lake-metasrv/**
crates/lake-objects/**
crates/lake-query/src/flight.rs
Iceberg write, DDL, DML, commit, catalog mutation, or catalog enumeration
REST protocol, auth, TLS, proxy, DNS, credential, or object-storage behavior
new environment variables, telemetry listeners, metric endpoints, or dynamic metric labels
cache freshness, stale-if-error, capacity, retry, timeout, or cancellation behavior

## Completion Criteria

Rule: iceberg-federation-metrics — external federation outcomes are observable
  without exporting workload identities

Scenario: A cache miss followed by a cache hit is observable with bounded labels
  Test:
    Package: lake-iceberg
    Filter: snapshot_metrics_report_load_and_cache_hit_without_identity_labels
    Level: integration
    Test Double: in-memory Iceberg catalog and Prometheus recorder
  Given an immediate-refresh-capable Iceberg catalog with one configured table
  When the table is resolved once from the external catalog and once from cache
  Then counters expose the exact bounded cache and load outcomes
  And no namespace, table, endpoint, warehouse, or credential value appears in the scrape

Scenario: An external table-load failure is distinguishable from a cache hit
  Test:
    Package: lake-iceberg
    Filter: snapshot_metrics_report_external_load_failure
    Level: integration
    Test Double: failing in-memory Iceberg catalog and Prometheus recorder
  Given an exact configured table load that the external catalog rejects
  When snapshot resolution returns its existing catalog error
  Then the load-error counter increments with a bounded outcome label
  And no new retry or stale-cache behavior is introduced

## Out of Scope

- Per-table, per-namespace, per-tenant, endpoint, warehouse, or credential
  telemetry.
- Metrics for Iceberg data-file bytes, scan progress, or upstream catalog
  internals that Lake does not own.
- Changing the external federation's read-only scope or any existing request
  timeout, cache, retry, OAuth, and cancellation policy.
