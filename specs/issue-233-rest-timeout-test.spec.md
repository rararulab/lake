spec: task
name: "iceberg-rest-timeout-test"
inherits: project
tags: [iceberg, testing, reliability]
---

## Intent

The Iceberg REST startup-timeout test used host wall-clock time to prove a
configured request timeout. Under `mise run ship`, concurrent cold Rust
documentation compilation can delay the test task long enough to exceed its
500 ms wall-clock budget even though the 25 ms HTTP timeout behaves correctly.
The test must instead control the timeout clock and prove that the unresponsive
request fails after the configured deadline without relying on host scheduling.

## Decisions

- Keep the production REST client and its `1..=60000` ms timeout contract
  unchanged; this is a test-observation defect, not a runtime timeout change.
- Use Tokio's paused clock only after the local REST handler has accepted the
  configuration request. Advancing past the configured deadline then proves
  the client aborts an otherwise-pending request deterministically.
- Retain a real loopback Axum/Reqwest path rather than mocking the REST client.

## Boundaries

### Allowed Changes
crates/lake-iceberg/tests/catalog.rs
crates/lake-iceberg/Cargo.toml
specs/issue-233-rest-timeout-test.spec.md
verification/issue-233-rest-timeout-test.md

### Forbidden
crates/lake-iceberg/src/**
Cargo.toml
Cargo.lock
crates/lake-cli/**
crates/lake-query/**
docs/**
production REST timeout values
retry, circuit-breaker, or catalog behavior

## Completion Criteria

Rule: iceberg-rest-timeout-test — an unresponsive REST catalog setup is tested
against the configured timeout, not host scheduling delay

Scenario: REST startup timeout follows the paused configured deadline
  Test:
    Package: lake-iceberg
    Filter: rest_catalog_timeout_bounds_unresponsive_startup
    Level: integration
    Test Double: none; real loopback Axum server and Reqwest client
  Given a loopback REST configuration route that has accepted a request but
    never sends a response
  When test time advances past a 25 ms configured REST timeout
  Then catalog startup fails with a catalog error without waiting for a
    wall-clock response deadline

## Out of Scope

- Changing Iceberg REST connection semantics or production timeout policy.
- Replacing the real HTTP integration path with a mock.
