spec: task
name: "query-manifest-authority"
inherits: project
tags: [query, metadata, manifest, dynamodb, authority, least-privilege]
---

## Intent

Make a production Query replica deployable without catalog DynamoDB authority.
Although catalog reads now use authenticated Metasrv RPCs, `lake query` still
builds the all-in-one `Context`, opens the registry table, constructs a local
Metasrv, and shares that registry MetaStore with Lance external manifests.
Split server startup contexts and physical manifest KV from catalog KV without
changing either durable format.

## Decisions

- `lake query` constructs a Query-specific context containing only its storage
  engine and credential-free managed-stage descriptor. It never constructs a
  catalog MetaStore, Metasrv, or table-placement authority.
- Cloud Lance manifests use `LAKE_MANIFEST_DYNAMODB_TABLE`, defaulting to
  `lake_manifests`, while catalog authority uses `LAKE_DYNAMODB_TABLE`,
  defaulting to `lake_registry`.
- Parse catalog, manifest, and enabled async table names before any network
  connection. Empty names or any overlap between a base and another
  authority's `_prefix_v2` companion fail closed.
- Metasrv/admin cloud context opens distinct registry and manifest MetaStore
  handles. Query opens only the manifest handle and does not provision tables;
  operators pre-provision them and enforce read-only Query IAM.
- Query uses a read-only external-manifest adapter that rejects every mutation
  and fails closed instead of installing a missing legacy latest pointer.
- Keep local all-in-one commands and selftest behavior. Local served Query uses
  local Lance directly and creates no Rocks catalog database.
- Keep async query state on independent `LAKE_ASYNC_DYNAMODB_TABLE` storage.

## Boundaries

### Allowed Changes
Cargo.lock
README.md
crates/lake-cli/**
crates/lake-engine-lance/**
deploy/kubernetes/lake.yaml
docs/architecture.md
docs/guides/kubernetes.md
docs/plans/2026-07-13-query-manifest-authority.md
specs/issue-124-query-manifest-authority.spec.md
verification/issue-124-query-manifest-authority.md

### Forbidden
changing registry manifest async-query or Dynamo durable item formats
routing Lance physical manifest reads through Metasrv
giving served Query a catalog MetaStore or in-process Metasrv fallback
using the registry table as the manifest table even for backward compatibility
provisioning Dynamo tables from the read-only Query startup path
weakening catalog TLS bearer role namespace or error-redaction behavior
changing SQL Flight DataLocation object async-result or ticket protocols
claiming process code can replace least-privilege cloud IAM policy

## Completion Criteria

Scenario: Local served Query constructs no catalog authority
  Test:
    Package: lake-cli
    Filter: query_context_has_no_catalog_authority
  Given an empty local data directory
  When the served Query-specific context is constructed
  Then it exposes only engine and managed-stage state and creates no catalog Rocks database or Metasrv

Scenario: Cloud table authorities cannot alias
  Test:
    Package: lake-cli
    Filter: cloud_manifest_table_alias_fails_before_connect
  Given empty names or registry manifest async base/companion table collisions
  When cloud storage configuration is validated
  Then startup fails before any DynamoDB or S3 client is constructed

Scenario: Query manifest reads cannot mutate physical authority
  Test:
    Package: lake-engine-lance
    Filter: read_only_manifest_store_never_mutates_legacy_state
  Given a read-only manifest adapter over current and legacy pointer layouts
  When Query resolves current state or a caller attempts migration put or delete
  Then current reads succeed while every mutation fails before metastore publication and missing latest pointers require metadata migration

Scenario: Metadata and Query use distinct cloud authority handles
  Test:
    Package: lake-cli
    Filter: cloud_storage_wiring_separates_registry_and_manifest_authority
  Given distinct validated registry physical manifest and async table groups
  When metadata and Query storage plans are derived
  Then metadata opens registry plus manifest authorities while Query opens manifest plus independently validated async state without catalog access or provisioning

Scenario: Kubernetes reference declares the manifest boundary
  Test:
    Package: lake-cli
    Filter: kubernetes_reference_is_secure_and_matches_runtime_contract
  Given the production Kubernetes reference
  When its runtime ConfigMap and Query deployment are validated
  Then an independent manifest table is configured and the documented Query IAM boundary excludes registry access

## Out of Scope

- Replacing `MetaStore` with a read-only manifest-specific Rust trait.
- Creating cloud IAM roles or applying DynamoDB infrastructure.
- Separate AWS credentials inside the Metasrv process, which legitimately
  needs both registry and manifest writes.
- Changing catalog snapshot or Lance commit protocols.
