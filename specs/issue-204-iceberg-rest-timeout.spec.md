spec: task
name: "iceberg-rest-timeout"
inherits: project
tags: [iceberg, rest, resilience, timeout, query]
---

## Intent

Make the latency boundary to an external Iceberg REST catalog explicit and
finite. A nonresponsive catalog must not indefinitely delay Query startup or
occupy a Flight planning permit simply because Lake inherited an upstream HTTP
client default.

## Decisions

- `IcebergCatalogConfig` owns one total REST request/connect timeout, with a
  safe 10-second default and a finite validated range.
- `LAKE_ICEBERG_REST_TIMEOUT_MS` is the deployment override. It is parsed and
  rejected before Query binds, like the rest of the Iceberg catalog settings.
- Lake passes a configured `reqwest::Client` into the pinned Apache REST
  catalog so the bound covers its configuration handshake, namespace checks,
  exact table loads, and OAuth exchanges.
- A timeout remains the existing generic external catalog error; endpoint,
  credential, and response material stay outside Lake errors and tickets.

## Boundaries

### Allowed Changes
crates/lake-iceberg/**
crates/lake-cli/src/commands/serve.rs
README.md
docs/design/iceberg-federation.md
docs/guides/cli.md
Cargo.lock
specs/issue-204-iceberg-rest-timeout.spec.md

### Forbidden
crates/lake-meta/**
crates/lake-metasrv/**
crates/lake-catalog/**
crates/lake-query/**
Lake registry schema changes
Iceberg write, DDL, DML, commit, or catalog mutation operations
retry policy, circuit breaker, background health task, or token service
credential persistence in metadata, SQL, Flight tickets, logs, metrics, or URLs
unbounded Iceberg namespace or table enumeration

## Completion Criteria

Rule: iceberg-rest-timeout — external catalog latency is a Lake-owned bound

Scenario: A nonresponsive REST catalog respects the configured deadline
  Test:
    Package: lake-iceberg
    Filter: rest_catalog_timeout_bounds_unresponsive_startup
  Level: integration
  Test Double: loopback Axum endpoint that deliberately delays `/v1/config`
  Given an Iceberg REST endpoint that accepts a connection but never replies to its configuration request
  When Lake connects with a short validated REST timeout
  Then startup fails as the generic catalog error within that bound rather than waiting for the upstream default

Scenario: REST timeout configuration rejects unsafe values
  Test:
    Package: lake-iceberg
    Filter: rest_timeout_rejects_zero_and_excessive_values
  Level: unit
  Given an otherwise valid Iceberg catalog configuration
  When a zero or excessive REST timeout is selected
  Then configuration fails before HTTP I/O can begin

Scenario: The deployment override is validated before Query binds
  Test:
    Package: lake-cli
    Filter: iceberg_rest_timeout_override_is_validated_before_listener_bind
  Level: unit
  Given an otherwise valid Iceberg REST catalog deployment configuration
  When `LAKE_ICEBERG_REST_TIMEOUT_MS` is valid, missing, malformed, or outside the permitted range
  Then the valid value becomes the catalog request deadline and every invalid value fails during configuration parsing

## Out of Scope

- Retrying a catalog operation, circuit breaking, or any background health
  process.
- Changes to Query's end-to-end Flight execution deadline, the external
  snapshot cache, auth modes, or read-only Iceberg semantics.
