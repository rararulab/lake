spec: task
name: "iceberg-rest-federation"
inherits: project
tags: [iceberg, datafusion, query, flight, catalog]
---

## Intent

Lake needs to read existing Iceberg episode datasets through the same
stateless Flight SQL entry point as Lake-owned tables. Without this adapter,
`SELECT * FROM iceberg.analytics.episodes` cannot resolve an external table;
users must build a separate reader and lose the shared SQL, admission,
authorization, and ticket experience.

This advances `goal.md`'s disaggregated read path: Query fans out and reads
external object data directly while the external Iceberg REST catalog remains
the authority for its metadata and snapshots. It does not put read fan-out on
Lake Metasrv, turn Lake into a warehouse/transaction engine, or add a storage
node tier.

## Decisions

- Add a focused `lake-iceberg` adapter crate using Apache
  `iceberg-rust` v0.10.0-rc.4 at the immutable upstream revision
  `b882e63652f4d4a172812994679a1dbc0c64cbd0`. The release candidate is used
  because its official DataFusion 53.1 / Arrow 58 integration matches Lake,
  while the corresponding crates.io release is not yet published. Do not use
  an unofficial compatibility crate.
- The first connector is optional and deployment-configured. It accepts one
  REST endpoint, warehouse, and a finite explicit namespace allowlist; invalid
  or incomplete configuration fails before Query binds. No endpoint secret or
  cloud credential is persisted in Lake metadata or added to a Flight ticket.
- Expose external tables only under the separate `iceberg` catalog. Lake-owned
  tables stay under `lake`; unqualified names retain Lake semantics.
- Do not use upstream `IcebergCatalogProvider`: it performs unbounded
  namespace/table enumeration and exposes write-capable providers. The Lake
  adapter lazily loads an exact table only after a name has been authorized,
  bounds its namespace/provider cache, and exposes only
  `IcebergStaticTableProvider` instances.
- Planning records the external catalog, namespace, table, and Iceberg
  snapshot ID in the encrypted statement ticket. DoGet reconstructs only that
  static snapshot; it never resolves a newer current snapshot. If that exact
  snapshot was expired upstream, execution fails closed.
- The read-only SQL gate rejects Iceberg DDL/DML before any external catalog,
  manifest, or object mutation. Lake registry schema, Metasrv ownership, and
  native Lance commit semantics are unchanged.
- External metadata refresh is bounded. A transient refresh
  failure retains an already loaded last-good external snapshot and never
  poisons Lake-owned catalog availability.

## Boundaries

### Allowed Changes
Cargo.toml
Cargo.lock
deny.toml
crates/lake-iceberg/**
crates/lake-query/**
crates/lake-cli/**
docs/architecture.md
docs/assets/architecture-overview.html
docs/design/iceberg-federation.md
docs/guides/cli.md
README.md
specs/issue-188-iceberg-rest-federation.spec.md
verification/issue-188-iceberg-rest-federation.md

### Forbidden
crates/lake-meta/**
crates/lake-metasrv/**
crates/lake-catalog/**
crates/lake-engine/**
crates/lake-engine-lance/**
crates/lake-objects/**
crates/lake-sdk/**
Lake registry schema changes
Lake Metasrv commit-protocol changes
Iceberg CREATE, DROP, ALTER, INSERT, UPDATE, DELETE, MERGE, or commit operations
copying Iceberg object data through Query or Metasrv
endpoint secrets, cloud credentials, signed URLs, or object bytes in Lake metadata or Flight tickets
unbounded namespace/table enumeration or unbounded process-local provider state

## Completion Criteria

Rule: iceberg-read-only — `iceberg` is an external read-only catalog

Scenario: Configured REST table reads through the external SQL catalog
  Test:
    Package: lake-iceberg
    Filter: rest_catalog_table_is_queryable_through_iceberg_catalog
  Given an Iceberg REST catalog with a configured `analytics` namespace and episode table
  When SQL selects `iceberg.analytics.episodes` through the adapter's external catalog
  Then the adapter returns the external table result through a static provider without a Lake registry entry

Scenario: Flight statement tickets preserve the planned Iceberg snapshot
  Test:
    Package: lake-query
    Filter: flight_ticket_keeps_iceberg_snapshot_after_catalog_advances
  Given Flight SQL plans an Iceberg table at snapshot A and the external catalog advances to snapshot B
  When the client executes the issued DoGet ticket
  Then it reads snapshot A and never silently substitutes B

Scenario: Iceberg mutation SQL is rejected before external mutation
  Test:
    Package: lake-query
    Filter: iceberg_catalog_mutations_are_rejected_before_any_write
  Given a configured external Iceberg catalog that records mutation attempts
  When a client submits Iceberg DDL or DML through direct or Flight SQL
  Then Lake returns a read-only error and the external catalog records no mutation

Rule: connector-lifecycle — Connector lifecycle is validated and bounded

Scenario: Invalid external configuration fails before Query accepts traffic
  Test:
    Package: lake-cli
    Filter: iceberg_configuration_is_all_or_nothing_before_listener_bind
  Given absent, partial, malformed, and valid Iceberg REST environment configuration
  When `lake query` constructs its served Query context
  Then disabled and valid configurations are exact while invalid configuration fails before listener bind

Scenario: External refresh failure retains a last-good static snapshot
  Test:
    Package: lake-iceberg
    Filter: configured_namespace_cache_never_enumerates_unconfigured_catalog_state
  Given a successfully loaded Iceberg table snapshot followed by a transient REST catalog failure
  When another query requests that same table within the configured staleness bound
  Then the adapter returns the last-good snapshot and Lake-owned Query planning remains independent

Scenario: External catalog state remains bounded to configured namespaces
  Test:
    Package: lake-iceberg
    Filter: configured_namespace_cache_never_enumerates_unconfigured_catalog_state
    Level: unit
    Test Double: recording REST catalog client with configured and unconfigured namespace fixtures
  Given an external catalog containing configured and unconfigured namespaces
  When the adapter warms and resolves a configured table
  Then it contacts only the configured namespace/table path and retains no unbounded namespace or table listing

## Out of Scope

- Iceberg write support, DDL, commit retry/idempotency, schema or partition evolution.
- Mirroring Iceberg table metadata into the Lake registry or propagating it through Metasrv.
- Importing/copying Iceberg data into Lake storage.
- Additional catalog types (Glue, Hive, SQL, S3 Tables) or multiple configured Iceberg catalogs.
- Cross-catalog joins, cross-table transactions, Iceberg GC, snapshot expiry, or maintenance ownership.
