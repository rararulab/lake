spec: task
name: "bounded-sdk-schema-cache"
inherits: project
tags: [sdk, cache, schema, singleflight, performance]
---

## Intent

Typed SDK inserts currently plan `SELECT * ... LIMIT 0` before every row only
to rediscover a table schema that is immutable within one table incarnation.
Cache successful schema lookups inside the SDK so fleet writers do not turn
repeated rows or concurrent clones into repeated Query planning load.

## Decisions

- A `LakeClient` owns one asynchronous schema cache shared by every clone.
- Capacity and time-to-live are finite, validated builder inputs. Defaults are
  1,024 tables and 60 seconds; zero, excessive, or effectively unbounded
  values fail before connecting.
- Concurrent misses for one table are singleflighted. Different tables remain
  independent.
- Lookup errors are never cached. A later request may recover immediately.
- Expiration bounds stale schema exposure after drop/recreate. Callers that
  coordinate replacement may explicitly invalidate one table or the complete
  cache without rebuilding the client.
- Cache hits preserve the existing authentication, trace propagation, typed
  binding, upload, resumable append, and authority validation paths.

## Boundaries

### Allowed Changes
Cargo.toml
Cargo.lock
crates/lake-sdk/**
README.md
docs/architecture.md
docs/plans/2026-07-12-sdk-schema-cache.md
specs/issue-75-sdk-schema-cache.spec.md
verification/issue-75-sdk-schema-cache.md

### Forbidden
crates/lake-common/**
crates/lake-meta/**
crates/lake-query/**
crates/lake-metasrv/**
crates/lake-engine/**
crates/lake-engine-lance/**
server-side catalog behavior changes
public Flight payload changes
unbounded caches or lock maps
caching lookup failures
cache keys containing SQL text, credentials, paths, or object metadata

## Completion Criteria

Scenario: Repeated and concurrent schema lookups are bounded and singleflighted
  Test:
    Package: lake-sdk
    Filter: schema_cache_coalesces_concurrent_lookups_across_clones
  Given one connected client, its clones, and a counting Query endpoint
  When repeated and concurrent inserts request the same table schema
  Then one successful schema Flight lookup populates the shared bounded cache while typed inserts preserve their existing behavior

Scenario: Expiry and explicit invalidation refetch while failures remain retryable
  Test:
    Package: lake-sdk
    Filter: schema_cache_expiry_and_invalidation_refetch_without_caching_failures
  Given a short-lived schema cache and a Query endpoint whose first lookup can fail
  When lookup is retried, the entry expires, one table is invalidated, or the cache is cleared
  Then failures are not retained and each stale or invalidated entry is refetched without affecting unrelated entries

Scenario: Schema cache configuration is finite and validated before connect
  Test:
    Package: lake-sdk
    Filter: schema_cache_rejects_unbounded_configuration
  Given caller-provided capacity and TTL settings
  When zero or values above the documented production ceilings are supplied
  Then client construction fails before network or storage setup and valid defaults remain finite

Scenario: Invalidation fences stale in-flight schema loads
  Test:
    Package: lake-sdk
    Filter: schema_cache_invalidation_fences_in_flight_loader
  Given a schema lookup for an old table incarnation is still in flight
  When that table or the complete cache is invalidated before the lookup finishes
  Then a new lookup does not join the old loader and the old result cannot repopulate the cache

## Out of Scope

- Caching SQL result batches or append results.
- Cross-process or distributed SDK caches.
- Push invalidation from the metastore.
- Suppressing authority-side schema validation or append errors.
