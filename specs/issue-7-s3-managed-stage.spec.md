spec: task
name: "s3-managed-stage"
inherits: project
tags: [objects, sdk, s3]
---

## Intent

Make the Rust SDK's SQL `FILE` path usable by remote clients and
multi-gigabyte production objects. The SDK must stream bytes directly to a
Lake-owned S3 prefix, store only stable immutable `DataLocation` values in
tables, and read object bytes directly without turning query or metasrv into a
payload proxy.

Today `LakeClient` owns a concrete `LocalObjectStore`, so a remote client must
share the query process's filesystem. A multi-gigabyte upload also lacks S3
multipart completion/abort behavior. This slice introduces one managed-stage
abstraction with local and S3 implementations while preserving the SQL API.

## Decisions

- `ManagedObjectStore` is an async, object-safe SDK boundary. Upload sources
  and returned readers are boxed async streams, so neither backend buffers an
  entire video/model and `LakeClient` stays cloneable through `Arc`.
- `S3ObjectStore` receives a configured AWS SDK client, bucket, and managed
  key prefix. Credentials, endpoint, region, and path-style behavior remain
  client configuration and never enter SQL or `DataLocation`.
- Non-empty S3 writes use multipart upload with parts of at least 5 MiB. The
  implementation completes only after every part succeeds and aborts on input
  or S3 errors.
- Stored identities are stable `s3://bucket/key` URIs plus content type, byte
  size, and SHA-256. Signed URLs are read capabilities and remain out of the
  persisted row.
- Direct reads validate both bucket and managed prefix before issuing
  `GetObject`, matching the local backend's containment invariant.
- The existing local implementation and public SQL `FILE` binding remain
  source-compatible at construction: `LakeClient::connect` accepts either
  concrete backend.

## Boundaries

### Allowed Changes
- `Cargo.toml`
- `Cargo.lock`
- `crates/lake-common/**`
- `crates/lake-objects/**`
- `crates/lake-sdk/**`
- `README.md`
- `docs/architecture.md`
- `docs/design/managed-objects.md`
- `docs/plans/**`
- `specs/**`
- `verification/**`
- `scripts/test-integration.ts`
- `.github/workflows/ci.yml`

### Forbidden
- Sending object bytes through query or metasrv
- Storing AWS credentials, endpoints, or signed URLs in table rows
- Accepting arbitrary S3 bucket/key values through public SQL
- Changing registry, table-version, or query-forwarding protocols
- Buffering a whole object in memory
- Publishing a completed S3 object after a failed source stream

## Completion Criteria

Scenario: S3 stage streams multipart upload and direct read
  Test:
    Package: lake-objects
    Filter: s3_multipart_roundtrip_localstack_is_wired
  External verification: `mise run test-integration` runs
  `s3_multipart_roundtrip_localstack` against LocalStack because the spec
  runner does not execute ignored infrastructure tests.
  Given a source larger than one minimum S3 multipart part
  When the S3 managed stage uploads and opens it against LocalStack
  Then the DataLocation has stable identity and verified size/hash and the
  direct reader yields the original bytes

Scenario: interrupted multipart upload publishes no object
  Test:
    Package: lake-objects
    Filter: interrupted_s3_upload_is_aborted_is_wired
  External verification: `mise run test-integration` runs
  `interrupted_s3_upload_is_aborted` against LocalStack because the spec
  runner does not execute ignored infrastructure tests.
  Given a reader that fails after at least one uploaded part
  When the S3 managed stage continues the multipart upload
  Then it returns the source error and no completed object exists

Scenario: SDK FILE insert accepts either managed-stage backend
  Test:
    Package: lake-sdk
    Filter: client_accepts_managed_object_store_abstraction
  Given a Lake client configured with the local managed stage through the
  shared abstraction
  When it inserts, queries, decodes, and opens a FILE
  Then the existing public round trip succeeds without a concrete local-store
  field in LakeClient

Scenario: SDK SQL FILE insert streams through the S3 stage
  Test:
    Package: lake-sdk
    Filter: sdk_file_insert_uses_s3_stage_is_wired
  External verification: `mise run test-integration` runs
  `sdk_file_insert_uses_s3_stage` against LocalStack because the spec runner
  does not execute ignored infrastructure tests.
  Given a Lake client configured with an S3 managed stage and a multipart-sized
  video source
  When it executes the parameterized INSERT, queries the FILE, and opens it
  Then SQL returns a stable s3 DataLocation and the direct reader yields the
  original bytes without query or metasrv carrying the payload

Scenario: S3 direct reader rejects locations outside its managed prefix
  Test:
    Package: lake-objects
    Filter: s3_reader_rejects_locations_outside_managed_prefix
  Given a DataLocation naming another bucket or key prefix
  When the S3 stage is asked to open it
  Then it rejects the location before issuing GetObject

## Out of Scope

- Authentication and tenant authorization at query/metasrv
- Presigned upload/download APIs and browser clients
- Resuming an interrupted upload across SDK process restarts
- Managed-object garbage collection, deduplication, or lifecycle policies
- Server-side encryption policy, replication, and cross-region routing
