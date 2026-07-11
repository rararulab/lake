spec: task
name: "managed-stage-discovery"
inherits: project
tags: [sdk, query, objects, flight, s3]
---

## Intent

Make the Rust database connection sufficient for SQL `FILE` I/O. Application
code supplies one query endpoint; the SDK discovers the immutable managed-stage
configuration through query and continues moving multi-gigabyte object bytes
directly between the SDK and storage.

## Decisions

- `lake-common` owns a versioned, serde-encoded `ManagedStageDescriptor` and
  the Flight action name. Backends are local root or S3 bucket/prefix plus
  optional region/endpoint and path-style behavior.
- The descriptor schema has no credential, token, signed URL, or object-byte
  field. Production credentials come from the SDK process's AWS workload
  identity/default credential chain.
- Query serves one metadata-only custom Flight action from immutable startup
  configuration. It does not contact metasrv for discovery and does not proxy
  object reads or writes.
- `LakeClient::connect(query_endpoint)` discovers and constructs its stage.
  The existing dependency-injection path is renamed
  `connect_with_store(query_endpoint, store)` for tests and advanced embedding.
- Local query mode advertises its managed object directory. Cloud mode
  advertises the configured S3 bucket and `LAKE_MANAGED_OBJECT_PREFIX`, default
  `managed-objects`.

## Boundaries

### Allowed Changes
- `Cargo.toml`
- `Cargo.lock`
- `crates/lake-common/**`
- `crates/lake-objects/**`
- `crates/lake-query/**`
- `crates/lake-sdk/**`
- `crates/lake-cli/**`
- `README.md`
- `docs/architecture.md`
- `docs/design/managed-objects.md`
- `docs/plans/**`
- `specs/**`
- `verification/**`
- `scripts/test-integration.ts`
- `**/.github/**`
- `**/AGENT.md`
- `**/mise.toml`
- `**/CLAUDE.md`
- `**/docs/guides/mise-ci.md`
- `**/docs/guides/workflow.md`

The final six patterns account for shared-checkout history from merged issue 6
that the repository-wide worktree verifier still reports. This workspace does
not edit root CI, mise configuration, or those workflow guides.

### Forbidden
- `crates/lake-meta/**`
- `crates/lake-metasrv/**`
- Static AWS access keys, secret keys, session tokens, or signed URLs in the
  descriptor or Flight result
- Sending object payload bytes through query or metasrv
- Fetching managed-stage configuration per SQL query or per object read
- Removing the explicit injected-store constructor

## Completion Criteria

Scenario: managed-stage descriptors are versioned and contain no secrets
  Test:
    Package: lake-common
    Filter: managed_stage_descriptors_roundtrip_without_credentials
  Given local and S3 stage configurations
  When they are serialized through the shared wire representation
  Then backend identity and connection hints round-trip without credential or
  object-payload fields

Scenario: query returns its immutable managed-stage descriptor
  Test:
    Package: lake-query
    Filter: managed_stage_action_returns_configured_descriptor
  Given a query service configured with a managed stage
  When a client invokes the discovery Flight action
  Then exactly one metadata result contains the configured versioned descriptor

Scenario: unsupported managed-stage protocol versions fail closed
  Test:
    Package: lake-common
    Filter: managed_stage_rejects_unsupported_protocol_version
  Given a descriptor declaring a newer unsupported protocol version
  When a client decodes the discovery result
  Then decoding returns a typed version error instead of guessing backend
  semantics

Scenario: SDK connects with only query and completes local FILE I/O
  Test:
    Package: lake-sdk
    Filter: client_discovers_local_stage_from_query
  Given query configured with a local managed stage
  When LakeClient connects using only the query endpoint
  Then FILE insert, SQL query, sequential open, and range open succeed without
  constructing or connecting to metasrv in the SDK

Scenario: explicit managed-store injection remains supported
  Test:
    Package: lake-sdk
    Filter: client_accepts_managed_object_store_abstraction
  Given an explicitly injected managed store
  When the advanced constructor connects
  Then the existing insert, query, decode, and direct-read behavior remains
  available

Scenario: query-only SDK discovers S3 and keeps bytes off query
  Test:
    Package: lake-sdk
    Filter: sdk_s3_stage_discovery_localstack_is_wired
  External verification: `mise run test-integration` runs
  `sdk_discovers_s3_stage_and_streams_directly_localstack` against LocalStack.
  Given query advertises a LocalStack S3 managed prefix
  When the SDK connects with only query and inserts a multipart-sized FILE
  Then SQL stores a stable s3 DataLocation and direct full/range reads return
  the original bytes

## Out of Scope

- Browser presigned multipart uploads
- Tenant authorization and credential vending
- Python SDK
- Rotating descriptor configuration without restarting query
- Multiple managed stages per query endpoint
