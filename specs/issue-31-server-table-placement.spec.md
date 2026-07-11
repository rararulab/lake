spec: task
name: "server-table-placement"
inherits: project
tags: [storage, metadata, ddl, security]
---

## Intent

Make remote table creation server-authoritative: a Flight client submits only
the table identifier and schema, while the metadata service derives the
dataset `TableLocation` from trusted process configuration. The same placement
policy is shared by local in-process administration so local and remote DDL
cannot drift.

Without this work, remote DDL can register a caller-selected dataset URI.
Reproducer: configure a metadata service with one managed table root, submit a
`create_table` action containing a different location, then resolve the table;
the registry points outside the configured root. A second reproducer uses path
separators or dot segments in a namespace/name and observes a derived path
outside the intended namespace directory. Both outcomes violate the metadata
authority's ownership of table placement.

This advances `goal.md`'s disaggregated-storage and metadata-authority signals:
the metadata tier remains authoritative for where tables live, while engines
continue to consume the storage-neutral `TableLocation` abstraction. It does
not add a storage-node tier or couple higher layers to Lance.

## Decisions

- Introduce a storage-neutral table placement value object in `lake-metasrv`,
  the tier that owns registry placement authority. It supports a trusted local
  root and a trusted S3 bucket/prefix and derives a deterministic location from
  a validated `TableRef` while leaving `lake-common` as thin shared newtypes.
- Placement validation rejects empty identifiers, dot segments, path
  separators, control characters, and identifiers longer than the documented
  bound before any engine or metastore mutation begins. Existing primitive ID
  constructors remain source-compatible; placement is the enforcement seam.
- `MetasrvServerConfig` carries the trusted placement policy. The Flight
  `create_table` action contains only namespace, name, and columns.
- A legacy caller-supplied `location` field is rejected rather than ignored,
  so old clients fail closed and cannot appear to select placement.
- `lake client create-table` removes `--location`. Local/cloud `Context`
  builds one placement policy and reuses it for both in-process operations and
  the Metasrv server.
- Existing internal `Metasrv::create_table` and engine APIs retain explicit
  `TableLocation` arguments for trusted library callers and focused tests.

## Boundaries

### Allowed Changes
- `crates/lake-common/**`
- `crates/lake-metasrv/**`
- `crates/lake-cli/**`
- `**/README.md`
- `docs/architecture.md`
- `docs/plans/**`
- `specs/**`
- `verification/**`

### Forbidden
- `crates/lake-meta/**`
- `crates/lake-engine/**`
- `crates/lake-engine-lance/**`
- `crates/lake-query/**`
- `crates/lake-sdk/**`

## Constraints

- Do not accept or silently ignore caller-selected dataset locations on remote
  DDL.
- Do not introduce backend-specific storage clients or Lance types in the
  placement policy.
- Do not change the registry commit protocol or drop-table lifecycle.

## Completion Criteria

Scenario: remote create derives the registered location from server policy
  Test:
    Package: lake-metasrv
    Filter: remote_create_uses_server_table_placement
  Given a Metasrv Flight service configured with a managed table root
  When a client creates a table using only its identifier and schema
  Then resolve returns the deterministic location beneath that configured root

Scenario: remote create rejects a legacy caller-selected location
  Test:
    Package: lake-metasrv
    Filter: remote_create_rejects_caller_location
  Given a create-table action containing the legacy location field
  When the action reaches the metadata service
  Then it fails with invalid argument before engine or registry mutation

Scenario: placement rejects identifiers that can escape the managed root
  Test:
    Package: lake-metasrv
    Filter: table_placement_rejects_unsafe_identifiers
  Given a namespace or table name containing empty, dot-segment, separator,
  control-character, or overlong input
  When the placement policy derives a table location
  Then derivation fails without producing a location

Scenario: local and S3 placement produce deterministic managed locations
  Test:
    Package: lake-metasrv
    Filter: table_placement_derives_managed_locations
  Given a valid table reference and trusted local or S3 configuration
  When the placement policy derives the location
  Then the result remains below the local root or S3 prefix and ends in a
  single `.lance` dataset segment

Scenario: remote CLI no longer exposes caller-selected placement
  Test:
    Package: lake-cli
    Filter: remote_create_table_has_no_location_argument
  Given the remote create-table command definition
  When clap parses its arguments
  Then namespace/name and columns are accepted without `--location`, and
  supplying `--location` is rejected

## Out of Scope

- Migrating or rewriting locations of already-registered tables.
- Tenant-specific namespace ownership and authorization policy.
- Durable drop tombstones, lease epochs, storage credentials, or presigning.
- Object-stage placement for SQL `FILE` values; this task covers table datasets.
