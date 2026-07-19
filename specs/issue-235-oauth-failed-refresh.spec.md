spec: task
name: "iceberg-oauth-failed-refresh-singleflight"
inherits: project
tags: [iceberg, oauth, resilience, concurrency, security]
---

## Intent

Lake's OAuth REST connector correctly coalesces concurrent expired-token reads
when `regenerate_token` succeeds because the successful exchange advances the
session generation. When the exchange itself fails, however, the generation
stays unchanged. Callers waiting behind the renewal mutex can then each issue
another client-credential exchange. A catalog or identity-provider outage can
therefore turn fan-out across distinct Iceberg tables into token-endpoint
traffic.

Concurrent bounded reads that observed one OAuth session generation must share
one in-flight renewal outcome, including a failed outcome. This preserves the
Query tier's cache-shield property without changing external catalog authority
or Lake's credential boundary.

## Decisions

- Replace the mutex-held renewal with a per-generation in-flight result that
  followers await. The leader publishes either its successful regenerated
  generation or its opaque catalog error; every follower observes that exact
  result.
- Remove the completed in-flight result immediately after publication. A later
  independent failed read may try a new bounded renewal; this is coordination,
  not negative caching, timer refresh, backoff, or a circuit breaker.
- Keep the existing OAuth contract: only client-credential REST sessions can
  renew; a failed bounded metadata read triggers at most one renewal and one
  retry of the same namespace check or table lookup.
- Exercise the public connector with a real loopback Axum REST catalog. The
  first exchange succeeds for startup; concurrent distinct-table reads then
  receive unauthorized responses while one deliberately failing renewal is
  held long enough for followers to join.

## Boundaries

### Allowed Changes
crates/lake-iceberg/src/lib.rs
crates/lake-iceberg/tests/catalog.rs
docs/design/iceberg-federation.md
specs/issue-235-oauth-failed-refresh.spec.md
verification/issue-235-oauth-failed-refresh.md

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

Rule: iceberg-oauth-failed-refresh-singleflight — one failed OAuth renewal is
  shared by concurrent bounded readers of one observed session generation

Scenario: Concurrent distinct-table OAuth failures share one failed renewal
  Test:
    Package: lake-iceberg
    Filter: concurrent_oauth_refresh_failure_is_single_flight
    Level: integration
    Test Double: real loopback Axum REST catalog and Reqwest client
  Given an OAuth REST catalog whose startup token is valid, whose distinct
    table loads return unauthorized, and whose next token exchange fails
  When concurrent readers resolve those exact configured tables while the
    failed renewal is in flight
  Then every read returns the generic catalog error and exactly one renewal
    request reaches the token endpoint

## Out of Scope

- Recovering a failed independent renewal with a retry policy or background
  token service.
- Refreshing static bearer tokens or supporting any additional catalog type.
- Changing external-table snapshot caching, namespace authorization, or
  Iceberg's read-only federation policy.
