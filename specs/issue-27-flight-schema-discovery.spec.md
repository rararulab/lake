spec: task
name: "flight-schema-discovery-cache"
inherits: project
tags: [catalog, flight-sql, schema, cache, metadata]
---

## Intent

Return truthful Arrow table schemas from Flight SQL `GetTables` without adding
reader-proportional metadata traffic or weakening tenant discovery filtering.

## Decisions

- New table registrations persist a versioned opaque Arrow IPC schema payload.
  `lake-meta` stores bytes and remains independent of Arrow/DataFusion.
- Registration JSON keeps the schema field optional so pre-upgrade entries
  continue to decode. Missing legacy schemas are reported explicitly when a
  client requests `include_schema=true`; they are never represented as empty.
- `MetaStore::scan_prefix` returns bounded/paginated key-value entries. RocksDB
  and DynamoDB implement it natively; catalog refresh loads the registry in one
  authority scan instead of N namespace scans.
- Query discovery reads one immutable process-local catalog snapshot. Tenant
  filtering happens before any schema is appended to the Flight response.

## Boundaries

### Allowed Changes
- `Cargo.toml`
- `Cargo.lock`
- `crates/lake-catalog/**`
- `crates/lake-meta/**`
- `crates/lake-query/**`
- `crates/lake-metasrv/**`
- `README.md`
- `docs/architecture.md`
- `docs/plans/**`
- `specs/**`
- `**/.github/**`
- `**/AGENT.md`
- `**/CLAUDE.md`
- `**/docs/guides/mise-ci.md`
- `**/docs/guides/workflow.md`
- `**/mise.toml`

The final six patterns account for shared-checkout workflow history already
present before this workspace. This issue does not edit those files.

### Forbidden
- Per-discovery metastore or storage-engine lookups
- Persisting Arrow/DataFusion types in `lake-meta`
- Returning an empty schema for a table whose schema is unknown
- Exposing schemas for unauthorized namespaces
- Changing table data or commit visibility semantics

## Completion Criteria

Scenario: registration schema payload is backward compatible
  Test:
    Package: lake-meta
    Filter: registration_schema_payload_is_backward_compatible
  Given an old registration JSON without schema bytes and a new registration
  When both are decoded and re-encoded
  Then the old entry remains readable and the new opaque payload round-trips

Scenario: prefix entry scans are bounded and return values
  Test:
    Package: lake-meta
    Filter: prefix_entry_scan_returns_stripped_keys_and_values
  Given matching and non-matching RocksDB entries
  When the registry prefix is scanned
  Then only matching stripped keys and exact values are returned in order

Scenario: Flight discovery returns cached real schemas
  Test:
    Package: lake-query
    Filter: flight_table_discovery_returns_cached_real_schema
  Given authorized and hidden tables with distinct persisted Arrow schemas
  When an authorized principal requests tables with `include_schema=true`
  Then only authorized tables and their exact schemas are returned and the
  request performs zero metastore operations after refresh

Scenario: legacy unknown schemas fail honestly
  Test:
    Package: lake-query
    Filter: flight_table_discovery_rejects_unknown_legacy_schema
  Given a visible legacy registration without schema bytes
  When `include_schema=true` is requested
  Then discovery returns FailedPrecondition rather than an empty schema
