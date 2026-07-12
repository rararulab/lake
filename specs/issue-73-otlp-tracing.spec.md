spec: task
name: "bounded-otlp-distributed-tracing"
inherits: project
tags: [observability, tracing, otlp, grpc, query, metasrv]
---

## Intent

Lake has structured logs, authenticated health, and bounded metrics, but a
single SDK request cannot yet be followed across Query, Metasrv, and follower
forwarding. Add opt-in distributed tracing without turning workload identity,
SQL, paths, or object metadata into exported telemetry.

## Decisions

- The CLI process is the only owner of the OpenTelemetry tracer provider and
  OTLP exporter. Export is disabled by default; malformed opt-in configuration
  fails before a command opens storage or listeners.
- Export uses a bounded batch processor. Process completion performs a bounded
  flush/shutdown; collector unavailability during service does not terminate
  Query or Metasrv.
- Standard W3C `traceparent` and `tracestate` metadata carry context across
  public Flight requests, Query-to-Metasrv calls, and Metasrv follower
  forwarding. Authentication metadata remains independent.
- Server spans use finite RPC operation names and bounded outcome attributes.
  SQL text, table, namespace, tenant, principal, URI/path, credential,
  operation ID, media type, and arbitrary request/action values are forbidden
  exported attributes.
- The existing JSON/pretty logging layer remains active with or without OTLP.
  Trace export adds a subscriber layer; it does not replace process logs.

## Boundaries

### Allowed Changes
Cargo.toml
Cargo.lock
crates/lake-cli/**
crates/lake-flight/**
crates/lake-query/**
crates/lake-metasrv/**
crates/lake-sdk/**
deploy/kubernetes/lake.yaml
docs/architecture.md
docs/guides/cli.md
docs/guides/kubernetes.md
README.md
docs/plans/2026-07-12-otlp-distributed-tracing.md
specs/issue-73-otlp-tracing.spec.md
verification/issue-73-otlp-tracing.md

### Forbidden
crates/lake-common/**
crates/lake-meta/**
crates/lake-engine/**
crates/lake-engine-lance/**
public Flight payload changes
metrics or health protocol changes
detached exporter tasks
unbounded telemetry queues
SQL, tenant, object, path, URI, credential, or operation identifiers in spans
vendor-specific collectors, dashboards, or sampling backends

## Completion Criteria

Scenario: OTLP export is opt-in and lifecycle-owned
  Test:
    Package: lake-cli
    Filter: otlp_exporter_is_opt_in_and_lifecycle_owned
  Given process logging and optional OTLP environment configuration
  When disabled, malformed, unavailable, and successful exporter lifecycles are exercised
  Then disabled mode starts no exporter, malformed configuration fails startup, runtime collector failure does not stop service, and shutdown is bounded

Scenario: Flight metadata preserves W3C context without sensitive fields
  Test:
    Package: lake-flight
    Filter: trace_context_roundtrips_through_flight_metadata_without_sensitive_fields
  Given a sampled parent context and authenticated Flight metadata
  When context is injected then extracted
  Then trace and span identity round-trip through only traceparent and tracestate without copying authorization or workload fields

Scenario: Query continues one trace into Metasrv
  Test:
    Package: lake-query
    Filter: query_trace_context_reaches_metasrv_without_data_attributes
  Given an inbound sampled Flight context and an in-process Metasrv observer
  When Query proxies a FILE append
  Then Query and Metasrv spans share the trace while exported attributes contain only bounded RPC and outcome fields

Scenario: Follower forwarding preserves trace context
  Test:
    Package: lake-metasrv
    Filter: follower_forwarding_preserves_trace_context
  Given a secured follower and elected leader with an inbound sampled context
  When the follower forwards a metadata write
  Then the leader continues the same trace and no authentication or workload metadata becomes a span attribute

## Out of Scope

- Collector deployment, storage, dashboards, exemplars, or alerting.
- Log or metric export through OTLP.
- Tail sampling and vendor-specific resource detection.
- Cross-process baggage propagation.
