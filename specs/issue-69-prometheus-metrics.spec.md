spec: task
name: "prometheus-runtime-metrics"
inherits: project
tags: [observability, prometheus, query, metasrv, operations]
---

## Intent

Structured logs explain individual events and gRPC Health drives routing, but
operators still cannot quantify saturation, catalog refresh failure,
leadership, or maintenance progress. Expose bounded Prometheus signals without
creating a public anonymous endpoint or detached process work.

## Decisions

- Query and Metasrv emit through the `metrics` facade. Only the CLI process
  installs a Prometheus recorder and scrape endpoint.
- Metrics are opt-in through `LAKE_METRICS_ADDR`. The value must be an IP
  loopback socket. Production collection uses a localhost sidecar or node
  agent; hostname and non-loopback addresses fail before listeners bind.
- `GET /metrics` serves text exposition. The HTTP listener and periodic
  recorder upkeep share command cancellation and are joined before return.
- Metric labels are finite enums only. SQL, table, namespace, tenant, URI,
  credential, operation ID, and arbitrary request paths are forbidden labels.
- Query records admission, inflight work, size rejection, catalog refresh
  outcomes, and readiness. Metasrv records append reservations, campaign
  outcomes, write readiness, and bounded maintenance work.

## Boundaries

### Allowed Changes
Cargo.toml
Cargo.lock
crates/lake-cli/**
crates/lake-query/**
crates/lake-metasrv/**
docs/architecture.md
docs/guides/cli.md
README.md
docs/plans/2026-07-12-prometheus-runtime-metrics.md
specs/issue-69-prometheus-metrics.spec.md
verification/issue-69-prometheus-metrics.md

### Forbidden
crates/lake-common/**
crates/lake-flight/**
crates/lake-meta/**
crates/lake-sdk/**
public Flight wire changes
unauthenticated non-loopback metrics listeners
detached telemetry tasks
user/data-derived metric labels
Kubernetes manifests
OTLP exporters

## Completion Criteria

Scenario: Metrics endpoint is private and lifecycle-owned
  Test:
    Package: lake-cli
    Filter: metrics_endpoint_is_loopback_only_and_owned_by_shutdown
  Given an opt-in metrics listener and a process cancellation token
  When configuration, scraping, and shutdown are exercised
  Then only loopback is accepted, GET metrics is exposed, tasks join, and the socket is released

Scenario: Query exports bounded saturation and refresh signals
  Test:
    Package: lake-query
    Filter: query_metrics_cover_admission_and_catalog_refresh
  Given a local Prometheus recorder and controlled query admission and refresh
  When one request is admitted, one saturates, and refresh succeeds then fails
  Then admitted, rejected, inflight, refresh, and readiness series use only bounded labels

Scenario: Metasrv exports authority and maintenance signals
  Test:
    Package: lake-metasrv
    Filter: metasrv_metrics_cover_append_leadership_and_maintenance
  Given controlled append admission, leadership, and one bounded maintenance page
  When saturation and authority transitions occur
  Then reservation, campaign, readiness, and maintenance series are exported without data labels

## Out of Scope

- Distributed tracing or OTLP.
- Dashboards, alert policies, or scheduler manifests.
- Tenant billing and quota enforcement.
- An internet-exposed scrape endpoint.
