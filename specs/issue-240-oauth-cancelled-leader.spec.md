spec: task
name: "iceberg-oauth-cancelled-leader"
inherits: project
tags: [iceberg, oauth, cancellation, concurrency, resilience]
---

## Intent

External Iceberg OAuth renewal is single-flight. A Query request can be
cancelled while its leader is waiting for the identity provider. The leader's
drop path must publish an opaque failed outcome so concurrent bounded readers
do not wait forever and do not start an additional renewal for the same
observed token generation.

## Decisions

- Exercise the public REST connector through a real loopback Axum server and
  its real Reqwest client rather than inspecting private coordination state.
- Keep the leader's token request pending, start a distinct-table follower,
  cancel the leader, and require the follower to complete under a bounded
  timeout.
- Treat cancellation as the existing opaque `IcebergError::Catalog` outcome.
  It is coordination only: no timer, retry/backoff, negative cache, new
  credential behavior, catalog enumeration, or write path is introduced.

## Boundaries

### Allowed Changes
crates/lake-iceberg/tests/catalog.rs
docs/design/iceberg-federation.md
specs/issue-240-oauth-cancelled-leader.spec.md
verification/issue-240-oauth-cancelled-leader.md

### Forbidden
crates/lake-cli/**
crates/lake-query/**
crates/lake-meta/**
crates/lake-metasrv/**
Cargo.toml
Cargo.lock
Iceberg write, DDL, DML, commit, catalog mutation, or catalog enumeration
background token refresh, timers, retry/backoff policy, circuit breaker, or negative cache
credential persistence in metadata, SQL, Flight tickets, logs, metrics, or URLs
new REST auth, TLS, CA, proxy, DNS, or credential behavior

## Completion Criteria

Rule: iceberg-oauth-cancelled-leader — cancellation of one OAuth renewal
  leader releases same-generation followers without another token exchange

Scenario: A cancelled OAuth renewal leader releases its concurrent follower
  Test:
    Package: lake-iceberg
    Filter: cancelled_oauth_renewal_leader_releases_follower
    Level: integration
    Test Double: real loopback Axum REST catalog and Reqwest client
  Given startup OAuth has produced one valid token and exact table reads return
    unauthorized while the next token exchange is held pending
  When a second exact table read has joined that renewal and the first reader
    is cancelled
  Then the follower returns the generic catalog error within its bounded wait
    and exactly one renewal reaches the token endpoint

## Out of Scope

- Retrying a cancelled or failed renewal outside the existing one renewal plus
  one exact metadata-read retry.
- Changing successful renewal behavior, static bearer behavior, snapshot
  caching, external catalog authority, or read-only federation policy.
