spec: task
name: "catalog-generation"
inherits: project
tags: [catalog, discovery, snapshot, performance]
---

## Intent

Flight discovery currently deep-clones the full listing and then performs
separate schema lookups. Refresh may publish between those operations, mixing
names from one generation with schemas from another, while every request pays
O(catalog) clone cost. Listing and schema must travel together in one immutable
request-pinned generation.

## Decisions

- Publish each complete catalog snapshot behind an `Arc`; readers clone only
  the pointer and never mutate a generation.
- Expose read-only listing and schema accessors on the pinned generation.
- DataFusion synchronous listing methods and Flight discovery obtain one Arc
  and perform no authority I/O.
- Refresh builds a private replacement and swaps the Arc only after the full
  registry scan succeeds.
- Keep registration/provider caches independent from listing generations.

## Boundaries

### Allowed Changes
crates/lake-catalog/**
crates/lake-query/**
docs/architecture.md
docs/plans/2026-07-12-catalog-generation.md
specs/issue-49-catalog-generation.spec.md
verification/issue-49-catalog-generation.md

### Forbidden
crates/lake-engine-lance/**
crates/lake-meta/**
crates/lake-metasrv/**
crates/lake-sdk/**

## Completion Criteria

Scenario: Pinning a generation avoids full catalog cloning
  Test:
    Package: lake-catalog
    Filter: cached_generation_clones_only_the_arc
  Given a warmed catalog generation
  When multiple readers pin the current generation
  Then they share the same immutable allocation

Scenario: A pinned generation remains internally consistent across refresh
  Test:
    Package: lake-catalog
    Filter: pinned_generation_keeps_names_and_schemas_together
  Given a reader pins generation A
  When refresh atomically publishes generation B with different tables and schemas
  Then the pinned reader sees only A and a new reader sees only B

Scenario: Flight table discovery uses one generation
  Test:
    Package: lake-query
    Filter: flight_table_discovery_reads_one_catalog_generation
  Given table names and schemas belong to one published generation
  When Flight builds a GetTables response during a concurrent refresh
  Then every returned name and schema comes from the request-pinned generation

Scenario: Failed refresh preserves the published Arc
  Test:
    Package: lake-catalog
    Filter: failed_refresh_preserves_generation_identity
  Given a current immutable generation
  When the next full registry scan fails
  Then the exact same Arc remains published

## Out of Scope

- Discovery admission, row limits, or multi-batch pagination.
- Changing refresh TTL or stale-while-revalidate policy.
- Caching Flight response batches.
- Durable catalog snapshot storage.
