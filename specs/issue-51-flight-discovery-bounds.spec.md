spec: task
name: "flight-discovery-bounds"
inherits: project
tags: [query, flight-sql, discovery, admission, performance]
---

## Intent

The stateless Query tier must absorb DDoS-like reader fan-out without memory
or CPU growing as concurrent metadata requests multiplied by catalog size.
Today Flight SQL `GetDbSchemas` and `GetTables` bypass query admission and
materialize every matching row into one `RecordBatch`. Reproducer: configure
one Query execution slot, open many authenticated discovery streams over a
large cached catalog, and leave them unread. Every request is admitted and
allocates a full response instead of rejecting at the configured queue bound.

## Decisions

- Reuse the per-replica Query admission semaphore for schema and table `DoGet`
  discovery; authenticate first, then acquire with the existing queue timeout.
- Move the permit into the existing deadline-aware Flight stream wrapper so
  completion, deadline, error, or client drop releases it.
- Add validated discovery limits with a default maximum of 10,000 matching
  rows and a default batch size of 256 rows. The batch size cannot exceed the
  maximum.
- Lazily build fixed-size metadata `RecordBatch` values from one pinned
  `Arc<CatalogGeneration>`; do not collect the complete response first.
- Stop after the configured matching-row maximum and return gRPC
  `ResourceExhausted` before resolving or allocating another row.
- Preserve local authorization and Flight SQL catalog/schema/table/type
  filters before schema resolution. Discovery performs no authority I/O.
- Expose deployment overrides as positive integer environment variables:
  `LAKE_QUERY_MAX_DISCOVERY_ROWS` and
  `LAKE_QUERY_DISCOVERY_BATCH_ROWS`.

## Boundaries

### Allowed Changes
crates/lake-query/**
crates/lake-cli/**
docs/architecture.md
docs/guides/cli.md
docs/plans/2026-07-12-flight-discovery-bounds.md
specs/issue-51-flight-discovery-bounds.spec.md
verification/issue-51-flight-discovery-bounds.md

### Forbidden
crates/lake-meta/**
crates/lake-metasrv/**
crates/lake-sdk/**
crates/lake-engine*/**
durable metadata formats
Flight SQL wire schemas

## Completion Criteria

Scenario: Discovery shares bounded Query admission
  Test:
    Package: lake-query
    Filter: flight_discovery_admission_releases_on_stream_drop
  Given one Query admission slot and an authenticated discovery request holding it
  When a second request waits past the queue timeout and the first stream is dropped
  Then the second is ResourceExhausted and a later request acquires the released slot

Scenario: Table discovery emits bounded batches lazily
  Test:
    Package: lake-query
    Filter: flight_table_discovery_streams_bounded_batches
  Given more authorized matching tables than the configured batch size
  When the client consumes GetTables
  Then every batch is at most the configured size and all matching rows arrive in order

Scenario: Schema discovery emits bounded batches lazily
  Test:
    Package: lake-query
    Filter: flight_schema_discovery_streams_bounded_batches
  Given more authorized namespaces than the configured batch size
  When the client consumes GetDbSchemas
  Then every batch is at most the configured size and all authorized rows arrive

Scenario: Discovery stops at the configured row limit
  Test:
    Package: lake-query
    Filter: flight_discovery_stops_at_configured_row_limit
  Given more matching rows than the configured discovery maximum
  When the client consumes the discovery stream
  Then the stream returns ResourceExhausted without allocating a row beyond the maximum

Scenario: Invalid discovery limits fail before serving
  Test:
    Package: lake-cli
    Filter: discovery_limit_values_are_validated_before_serving
  Given zero, malformed, or batch-greater-than-maximum environment limits
  When Query server configuration is built
  Then configuration fails before binding the Flight listener

## Out of Scope

- Continuation tokens or cross-request catalog pagination.
- Separate CPU pools or separate semaphores for SQL and discovery.
- Changing Flight SQL metadata schemas or SDK APIs.
- Caching encoded discovery responses.
