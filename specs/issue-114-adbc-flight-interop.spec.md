spec: task
name: "adbc-flight-interop"
inherits: project
tags: [query, flight-sql, adbc, interoperability, security]
---

## Intent

Verify Lake with an independently implemented upstream Flight SQL client so
shared assumptions in the Rust SDK cannot hide wire incompatibilities.

## Decisions

- The official Python ADBC Flight SQL wheel is a pinned black-box test client;
  Lake production remains Rust-first and gains no Python runtime dependency.
- ADBC verifies interactive statements. Standard Arrow Flight tests verify
  polling and cancellation because those RPCs are not ordinary DB-API calls.
- Every external process and network wait is bounded.

## Boundaries

### Allowed Changes
mise.toml
interop/adbc/**
crates/lake-query/**
README.md
docs/architecture.md
docs/design/sql-api-over-s3.md
docs/plans/2026-07-13-adbc-flight-interop.md
specs/issue-114-adbc-flight-interop.spec.md
verification/issue-114-adbc-flight-interop.md

### Forbidden
Python or ADBC dependencies in production binaries
unbounded subprocesses network waits or result buffering
disabling TLS authentication or ticket validation for compatibility
claiming ADBC transactions DML prepared statements or metadata support
replacing standard Flight SQL with a bespoke protocol

## Completion Criteria

Scenario: Upstream ADBC reads typed multi-batch SQL
  Test:
    Package: lake-query
    Test: adbc_interop
    Filter: upstream_adbc_reads_typed_multibatch_result
    Ignored: true
  Given a real loopback Lake Query listener
  When the pinned official ADBC Flight SQL driver executes a large typed SELECT
  Then it receives the exact schema rows and more than one Arrow record batch

Scenario: Upstream ADBC preserves the read-only boundary
  Test:
    Package: lake-query
    Test: adbc_interop
    Filter: upstream_adbc_observes_stable_write_rejection
    Ignored: true
  Given an ADBC connection to Lake Query
  When it submits public DML
  Then the driver returns Lake's stable client-visible read-only error

Scenario: Upstream ADBC bearer authentication fails closed
  Test:
    Package: lake-query
    Test: adbc_interop
    Filter: upstream_adbc_bearer_authentication_fails_closed
    Ignored: true
  Given a bearer-protected Query listener
  When ADBC sends the exact bearer or a missing or wrong credential
  Then only the exact bearer can execute SQL

Scenario: Standard Flight polling and endpoint redemption remain interoperable
  Test:
    Package: lake-query
    Filter: poll_flight_info_submits_identity_bound_pinned_job
  Given only the upstream Arrow Flight protocol client types
  When a query is polled and its completed endpoint is redeemed
  Then descriptor chaining and endpoint tickets follow the Flight specification

Scenario: Standard Flight cancellation remains interoperable
  Test:
    Package: lake-query
    Filter: cancel_flight_info_fences_execution_and_reaps_partial_results
  Given a standard PollInfo returned by Query
  When its FlightInfo is sent through CancelFlightInfo repeatedly
  Then cancellation is idempotent and no partial result remains published

Scenario: Interop dependencies and execution are reproducible
  Test:
    Package: lake-query
    Test: adbc_interop
    Filter: adbc_interop_harness_is_pinned_and_bounded
  Given a clean development environment
  When the conformance task is inspected or run
  Then exact upstream versions a frozen lock and finite process deadlines are required

## Out of Scope

- ADBC transactions, DML, prepared statements, bulk ingestion, or catalog
  metadata APIs.
- Shipping Python with Lake.
- Testing every ADBC language binding in this issue.
