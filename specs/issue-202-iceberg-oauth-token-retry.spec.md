spec: task
name: "iceberg-oauth-token-retry"
inherits: project
tags: [iceberg, rest, oauth, resilience, security, query]
---

## Intent

Long-running Query replicas must keep their bounded read-only Iceberg REST
catalog usable after an OAuth access token expires, without turning Lake into a
token service or persisting credentials outside the deployment runtime.

## Decisions

- Retain the concrete upstream REST catalog only for an OAuth
  client-credential session; static bearer and test-injected generic catalogs
  have no renewal capability.
- After a bounded namespace check or exact table load fails, regenerate the
  OAuth token once and retry that exact read once. The retry never reaches an
  Iceberg write or enumeration operation.
- Concurrent failed reads share a generation-guarded renewal instead of
  stampeding the external token endpoint.
- Keep the existing generic external-catalog error boundary so refresh and
  retry failures cannot expose credentials or untrusted REST response material.

## Boundaries

### Allowed Changes
crates/lake-iceberg/**
Cargo.lock
README.md
docs/design/iceberg-federation.md
docs/guides/cli.md
specs/issue-202-iceberg-oauth-token-retry.spec.md

### Forbidden
crates/lake-meta/**
crates/lake-metasrv/**
crates/lake-catalog/**
crates/lake-query/**
Lake registry schema changes
Iceberg write, DDL, DML, commit, or catalog mutation operations
background token refresh scheduling or a Lake-owned OAuth/token service
credential persistence in metadata, SQL, Flight tickets, logs, metrics, or URLs
unbounded Iceberg namespace or table enumeration

## Completion Criteria

Rule: iceberg-oauth-token-retry — expired bounded REST sessions recover safely

Scenario: An expired OAuth session renews one exact table lookup
  Test:
    Package: lake-iceberg
    Filter: rest_catalog_oauth_expiry_regenerates_once_for_exact_table_load
  Given a REST catalog that accepts an initial OAuth token at startup then rejects it for one exact table load
  When Lake receives that bounded read failure
  Then it exchanges client credentials once more and retries only the same table load successfully

Scenario: Concurrent expired reads share a single OAuth renewal
  Test:
    Package: lake-iceberg
    Filter: concurrent_oauth_expiry_shares_one_session_renewal
  Given multiple exact table loads fail against the same expired OAuth session
  When they concurrently request renewal
  Then Lake performs one generation-guarded token exchange and every read retries successfully

Scenario: External REST failures remain opaque to Lake callers
  Test:
    Package: lake-iceberg
    Filter: rest_catalog_failures_redact_runtime_bearer_tokens
  Given an external catalog returns a failure response containing its received authorization value
  When Lake renders the catalog failure
  Then the generic error and its Debug rendering expose neither credentials nor the untrusted REST response material

## Out of Scope

- Timer-based background refresh, external secret rotation, or a Lake-owned
  OAuth/token service.
- Static bearer token renewal, additional catalog types, Iceberg writes, or
  unbounded external discovery.
