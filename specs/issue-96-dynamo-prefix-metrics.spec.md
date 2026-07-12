spec: task
name: "dynamo-prefix-metrics"
inherits: project
tags: [meta, dynamodb, observability, performance]
---

## Intent

Expose bounded-cardinality evidence for Dynamo prefix amplification, authority
mode, and migration state without leaking any logical metadata identity.

## Boundaries

### Allowed Changes
crates/lake-meta/**
crates/lake-cli/src/metrics.rs
Cargo.lock
README.md
docs/design/dynamo-prefix-metrics.md
docs/guides/cli.md
docs/plans/2026-07-12-dynamo-prefix-metrics.md
specs/issue-96-dynamo-prefix-metrics.spec.md
verification/issue-96-dynamo-prefix-metrics.md

### Forbidden
user-controlled metric labels
prefix, key, tenant, namespace, table, URI, endpoint, cursor, or operation IDs in metrics
per-item histograms
changing Dynamo authority or migration semantics
unbounded in-memory metric state

## Completion Criteria

Scenario: Prefix request work is observable by physical layout
  Test:
    Package: lake-meta
    Filter: dynamo_prefix_metrics_record_bounded_work
  Given v1 Scan and v2 Query responses
  When prefix telemetry is recorded
  Then request, evaluated-item, and returned-item counters identify only bounded layout/API/outcome states

Scenario: Metric labels never contain logical identity
  Test:
    Package: lake-meta
    Filter: dynamo_prefix_metrics_never_export_logical_keys
  Given hostile prefix, key, endpoint, and cursor strings
  When Dynamo telemetry is rendered
  Then none of those strings appear in metric names or labels

Scenario: Runtime authority mode is directly observable
  Test:
    Package: lake-meta
    Filter: dynamo_authority_metric_tracks_monotonic_switch
  Given a node starts on v1 and later observes the completion marker
  When authority telemetry changes
  Then the process gauge moves monotonically from zero to one

Scenario: Durable migration state is directly observable
  Test:
    Package: lake-meta
    Filter: dynamo_migration_barrier_metric_is_identity_free
  Given a durable finalization barrier
  When runtime telemetry is rendered
  Then the barrier is visible without cursor or logical identity labels

## Out of Scope

- Per-table or per-prefix metrics.
- CloudWatch exporter configuration.
- Dynamo autoscaling policy changes.
