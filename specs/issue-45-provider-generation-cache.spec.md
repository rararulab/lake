spec: task
name: "provider-generation-cache"
inherits: project
tags: [catalog, query, cache, performance]
---

## Intent

SQL planning currently resolves every table by reopening its storage dataset
and rebuilding a DataFusion `TableProvider`. Repeated and concurrent reads of
one immutable table generation therefore multiply storage metadata traffic.
Each query replica must reuse one provider per exact registry-visible
generation while preserving snapshot isolation across append and
drop/recreate.

## Decisions

- Cache providers inside the process-local catalog shared by its schema
  providers; query replicas do not share mutable cache state.
- Identify a generation by table name, physical location, incarnation, and
  registry version. A changed version or recreated table cannot reuse an old
  provider.
- Coalesce concurrent loads for the same generation. Failed and missing loads
  are not cached, so recovery can be observed without waiting for expiry.
- Bound the cache by entry count. Old immutable generations may remain until
  normal eviction but are no longer selected after the registry cache
  observes a new generation.

## Boundaries

### Allowed Changes
crates/lake-catalog/**
crates/lake-query/**
docs/architecture.md
docs/plans/2026-07-12-provider-generation-cache.md
specs/issue-45-provider-generation-cache.spec.md
verification/issue-45-provider-generation-cache.md

### Forbidden
crates/lake-engine-lance/**
crates/lake-meta/**
crates/lake-metasrv/**
crates/lake-sdk/**

## Completion Criteria

Scenario: Concurrent planning loads one provider per generation
  Test:
    Package: lake-catalog
    Filter: concurrent_provider_loads_are_singleflighted
  Given many concurrent table resolutions observe the same registration generation
  When they request its DataFusion provider together
  Then the storage engine is opened and the provider is built exactly once

Scenario: Version changes select a fresh provider
  Test:
    Package: lake-catalog
    Filter: provider_cache_separates_versions
  Given a cached provider for one registry version
  When the registration advances to a new version
  Then resolution builds and returns a provider pinned to the new version

Scenario: Recreate cannot reuse the dropped incarnation
  Test:
    Package: lake-catalog
    Filter: provider_cache_separates_incarnations
  Given a cached provider for a table incarnation
  When the name is dropped and registered again with a new incarnation
  Then resolution builds a provider for the replacement generation

Scenario: Failed provider loads remain retryable
  Test:
    Package: lake-catalog
    Filter: failed_provider_load_is_not_cached
  Given the first storage provider load fails
  When a later resolution retries the same generation
  Then the storage engine is called again and the successful provider is cached

Scenario: Write acknowledgement fences stale registration fills
  Test:
    Package: lake-catalog
    Filter: invalidation_fences_an_inflight_stale_registration_fill
  Given an old registration lookup pauses after reading the pre-append version
  When append publishes a new version and the query replica invalidates the table
  Then the old lookup cannot repopulate the registration generation used by later queries

Scenario: Provider cache is bounded
  Test:
    Package: lake-catalog
    Filter: provider_cache_respects_capacity
  Given more immutable generations than the configured cache capacity
  When cache maintenance completes
  Then its entry count does not exceed that capacity

## Out of Scope

- Sharing provider caches across query processes.
- Changing registry-cache staleness or write visibility semantics.
- Caching query plans or result batches.
- Storage-engine-specific provider internals.
