spec: task
name: "query-catalog-client"
inherits: project
tags: [query, catalog, metadata, authority, flight, cache]
---

## Intent

Remove the catalog registry KV interface from production Query. `LakeCatalog`
currently accepts `MetaStoreRef` and directly reads registry keys even though
Metasrv is the bounded catalog authority. Query already has an authenticated
Metasrv Flight connection for writes; catalog refreshes and point resolutions
must use that control plane while SQL continues reading table data directly
from object storage through the storage engine.

## Decisions

- Define a read-only `CatalogSource` seam in `lake-catalog`. It returns one
  resolved registration or one generation-coherent directory response and
  exposes no CAS, raw key, scan-prefix, or delete operation.
- Keep a local metastore adapter for in-process development and catalog unit
  tests. Production `lake query` constructs only the authenticated remote
  source for catalog use; it never falls back to the local adapter.
- Add a bounded, versioned `catalog_snapshot` Metasrv action. A caller supplies
  its last opaque generation; the authority returns `not_modified` or a full
  directory assembled between matching generation reads with bounded retries.
- Fail remote snapshots closed until the monotonic directory authority marker
  exists. Account registrations incrementally and admit one full snapshot per
  Metasrv process until its response is dropped.
- Authorize the full snapshot only for QueryService, MetadataPeer, or Admin.
  Point resolution remains namespace-delegated. Bound request, entry count,
  schema bytes, and total response bytes before returning a Flight result.
- Preserve LakeCatalog's first-warm fail-closed behavior, last-good stale-while-
  revalidate behavior, refresh coalescing, local sync listings, registration
  TTL/cache fencing, append read-your-write invalidation, and immutable query
  snapshot pinning.
- Physical storage-engine metadata is not catalog authority. Lance manifest KV
  separation/least-privilege credentials are a follow-up; this issue removes
  only direct registry reads from `LakeCatalog` and production Query wiring.

## Boundaries

### Allowed Changes
Cargo.toml
Cargo.lock
crates/lake-catalog/**
crates/lake-cli/**
crates/lake-common/**
crates/lake-metasrv/**
crates/lake-query/**
README.md
docs/architecture.md
docs/plans/2026-07-13-query-catalog-client.md
specs/issue-122-query-catalog-client.spec.md
verification/issue-122-query-catalog-client.md

### Forbidden
changing durable registry keys values directory markers or mutation semantics
routing table data or Arrow record batches through Metasrv
exposing raw MetaStore CAS scan prefix delete or credentials through CatalogSource
unbounded snapshot request response entry schema or retry limits
serving a directory assembled across different authoritative generations
per SQL row batch object or cache hit metadata RPC traffic
production Query fallback from remote catalog source to direct registry reads
weakening TLS bearer role namespace delegation or error redaction
changing async query state result manifest ticket or object layouts
claiming physical Lance manifest KV credential separation in this issue

## Completion Criteria

Scenario: Metasrv returns one bounded coherent catalog generation
  Test:
    Package: lake-metasrv
    Filter: remote_catalog_snapshot_is_generation_coherent_and_bounded
  Given a registry mutation races a full catalog snapshot and configured small limits
  When a QueryService requests catalog_snapshot with an older generation
  Then Metasrv retries to one matching generation or fails boundedly and never returns a mixed directory

Scenario: Remote source preserves catalog resolution semantics
  Test:
    Package: lake-query
    Filter: remote_catalog_source_matches_local_catalog_resolution
  Given the same registered tables behind a local adapter and a secured real Metasrv listener
  When both LakeCatalog instances warm and resolve immutable table snapshots
  Then their listings schemas locations engines incarnations and versions are identical

Scenario: Warm catalog cache removes metadata RPCs from SQL hot path
  Test:
    Package: lake-query
    Filter: remote_catalog_cache_hit_uses_zero_metadata_rpcs
  Given a warmed remote catalog and a counting Metasrv client
  When repeated planning resolves the same unexpired registration and directory generation
  Then no additional metadata RPC is issued for cache hits

Scenario: Metadata outage preserves last good catalog generation
  Test:
    Package: lake-query
    Filter: remote_catalog_outage_serves_last_good_generation
  Given a successfully warmed remote catalog whose Metasrv becomes unavailable
  When the catalog becomes stale and concurrent queries trigger revalidation
  Then one bounded refresh fails while every caller immediately observes the last good generation

Scenario: Production Query catalog wiring has no direct registry source
  Test:
    Package: lake-cli
    Filter: query_catalog_wiring_requires_remote_metadata_source
  Given cloud or local server configuration for lake query
  When QueryEngine is constructed for serving
  Then its catalog source is the authenticated metadata client and missing invalid security or endpoint configuration fails before bind without fallback

Scenario: Proxied append invalidation preserves read your write
  Test:
    Package: lake-query
    Filter: remote_catalog_append_invalidation_observes_committed_version
  Given a cached remote registration and a successful FILE append acknowledgement
  When the same Query connection plans the table again
  Then the old cache fill is fenced and the next delegated resolve returns the committed version without a full directory refresh

## Out of Scope

- Moving Lance external manifest metadata out of the registry DynamoDB table.
- Push-based catalog invalidation, watch streams, or cluster-global cache state.
- Pagination of the target-scale 10^4-entry in-memory directory beyond one
  explicitly bounded snapshot response.
- Changing SQL, Flight SQL, DataLocation, async query, or storage object formats.
