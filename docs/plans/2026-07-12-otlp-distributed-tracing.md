# OTLP distributed tracing implementation plan

**Goal:** Follow one Flight operation across Lake tiers while preserving
bounded telemetry cardinality and process-owned shutdown.

## Task 1: Lock the metadata contract

1. Add W3C trace-context inject/extract helpers for tonic metadata.
2. Prove only `traceparent` and `tracestate` round-trip.
3. Reject malformed remote context without weakening authentication.

## Task 2: Own exporter lifecycle in the CLI

1. Compose JSON/pretty logging with an optional OpenTelemetry layer.
2. Build a bounded OTLP trace batch exporter from validated environment.
3. Flush and shut down inside a finite process deadline.

## Task 3: Instrument tier boundaries

1. Create bounded server spans for public Query and Metasrv Flight paths.
2. Inject current context into Query-to-Metasrv requests.
3. Preserve the same context through follower-to-leader forwarding.
4. Inject context from the Rust SDK without changing Flight payloads.

## Task 4: Operate and verify

1. Document collector configuration, service names, sampling, redaction, and
   failure behavior.
2. Wire optional collector endpoint/service-name values in the Kubernetes
   reference without exposing collector credentials.
3. Run the guarded spec lifecycle, strict clippy, full gate, integration,
   independent review, and independent verification before merge.
