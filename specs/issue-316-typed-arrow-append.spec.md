spec: task
name: "typed-arrow-append"
inherits: project
tags: [sdk, arrow, append, robotics, episode, rust]
---

## Intent

Expose a bounded public Rust SDK path that appends caller-supplied Arrow
`RecordBatch` values to one exact `TableRef`. This is the missing bridge
between Lake's implemented Episode/ArtifactRef v1 encoder and its existing
durable, query-mediated append protocol.

Current reproducer:

1. Build a valid `EpisodeBundleV1`, including one Episode row and its complete
   GC-visible ArtifactRef set, then encode it with
   `lake_objects::episode_artifact_table_v1`.
2. Connect a normal public `LakeClient` to Query and try to publish that batch
   to an Episode table.
3. Observe that the SDK only exposes the narrow scalar/`FILE`
   `insert`/`insert_many` binder. It cannot accept the already-validated Arrow
   value, even though Query, Metasrv, and the storage engine already carry and
   commit generic Arrow batches.

Without this slice, a robotics ingest Adapter must bypass the public SDK,
re-encode a flat contract through an ever-growing SQL scalar parser, or add a
robotics-specific write API to a generic transport crate. The required
behavior is a format-neutral `append_batches` API that validates bounded Arrow
input and the authoritative table schema before `DoPut`, then reuses the exact
operation identity, digest, checkpoint, retry, and commit path already used by
typed SQL inserts.

This advances the `goal.md` ingest -> inspect -> select loop and the Phase 0
robot-training delivery sequence. It preserves Query as the stateless public
write proxy, Metasrv as the bounded commit authority, and object storage as the
only object-byte data plane. It does not turn Lake into a general warehouse,
Rerun Hub clone, training orchestrator, or cross-table transaction engine.

## Decisions

- Add `LakeClient::append_batches(&TableRef, Vec<RecordBatch>) ->
  Result<Version>` as the generic public write surface. Ownership makes the
  first implementation finite and lets the existing Flight encoder consume
  batches without cloning their arrays.
- Accept 1..=10,000 aggregate rows. Reject an empty vector, batches whose total
  row count is zero or over the limit, and arithmetic overflow before schema
  lookup or `DoPut`.
- Require every supplied batch to have the exact same Arrow schema, including
  field names, order, data types, nullability, and schema metadata. Validate
  this caller-local property before any RPC.
- Resolve the target table schema through the existing bounded SDK schema
  cache after local validation. The input schema must exactly equal that
  authoritative schema; v1 performs no coercion, projection, field reordering,
  default filling, or schema evolution.
- Encode all batches as one bounded Arrow Flight stream and retain the existing
  64 MiB encoded payload ceiling. The append has one UUIDv7 operation identity,
  one payload digest, one durable checkpoint, and one table-version result.
- Extract one shared append-preparation helper so `append_batches` and the
  existing `insert`/`insert_many` path attach the same command descriptor,
  validate the same payload ceiling, persist the same replay-safe checkpoint,
  and use the same ambiguous-result retry machinery.
- Keep scalar/`FILE` insert validation and direct object upload behavior
  unchanged. `append_batches` itself never uploads or reads object bytes;
  `DataLocation` cells are immutable metadata already produced by an Adapter
  or object-upload path.
- Support `connect_query_only`. Generic append needs Query schema lookup and
  `DoPut`, but no SDK-process object-store credential or managed-stage
  discovery.
- Prove the public path end to end with
  `episode_artifact_table_v1`: one call publishes the Episode row and every
  ArtifactRef row in one table version, and normal public SQL reads them back.
- Update the architecture/design status to mark generic typed Arrow append as
  implemented while leaving format Adapters and later phases planned.

## Boundaries

### Allowed Changes
Cargo.lock
crates/lake-sdk/**
README.md
docs/architecture.md
docs/design/robot-training-lakehouse.md
specs/issue-316-typed-arrow-append.spec.md
verification/report.md

### Forbidden
crates/lake-common/**
crates/lake-flight/**
crates/lake-meta/**
crates/lake-catalog/**
crates/lake-query/**
crates/lake-metasrv/**
crates/lake-engine/**
crates/lake-engine-lance/**
crates/lake-objects/**
crates/lake-iceberg/**
crates/lake-cli/**
goal.md
site/**
.github/**
new Flight or Metasrv append protocols
object bytes in Query, Metasrv, SQL rows, or append Flight payloads
schema coercion, projection, reordering, defaults, or evolution
robotics-specific SDK APIs or format-parser dependencies
unbounded, streaming, or cross-table appends

## Completion Criteria

Rule: public-arrow-append — one validated Arrow payload commits atomically

Scenario: Episode and ArtifactRef rows append through a Query-only SDK
  Test:
    Package: lake-sdk
    Filter: sdk_typed_arrow_append_commits_episode_artifact_bundle
  Level: integration
  Targets: crates/lake-sdk/src/lib.rs
  Given a Query-only `LakeClient`, an Episode table with the v1 contract
  schema, and a multi-Artifact `EpisodeBundleV1` encoded by
  `episode_artifact_table_v1`
  When `append_batches` targets that exact `TableRef`
  Then one new table version contains exactly one Episode row and every
  ArtifactRef row, and no object-store discovery, upload, or byte proxy is
  required

Scenario: invalid Arrow input fails before append side effects
  Test:
    Package: lake-sdk
    Filter: sdk_typed_arrow_append_rejects_invalid_batches_before_put
  Level: integration
  Targets: crates/lake-sdk/src/lib.rs
  Given empty, zero-row, excessive-row, inconsistent-schema, and
  table-schema-mismatched batch inputs
  When `append_batches` validates them
  Then each returns a typed bounded error before `DoPut`, locally invalid
  inputs perform no schema RPC, and no table version is published

Rule: append-recovery — public Arrow writes use the existing durable identity

Scenario: an ambiguous Arrow append converges without a duplicate commit
  Test:
    Package: lake-sdk
    Filter: sdk_typed_arrow_append_reuses_durable_idempotent_transport
  Level: integration
  Targets: crates/lake-sdk/src/lib.rs, crates/lake-sdk/src/append_checkpoint.rs
  Given a typed Arrow append with checkpointing enabled and a lost first append
  result after Metasrv commits it
  When the SDK retries or resumes the prepared append
  Then the same UUIDv7 identity, digest, encoded payload, and checkpoint are
  reused, exactly one table version is committed, and the conclusive success
  removes the checkpoint

## Out of Scope

- Uploading raw Artifact bytes, manifest bytes, or local paths through
  `append_batches`.
- RRD, MCAP, LeRobot, ROS, or Rerun readers, writers, metadata extraction, or
  Adapter interfaces.
- Public Episode-specific ingestion convenience APIs.
- DatasetRevision, TrainingView, retention, Python/PyTorch clients, Viewer
  launch, Materializations, or derived-Layer scheduling.
- Schema evolution, casts, partial-column batches, default values, streaming
  iterators, or unbounded buffers.
- Cross-table transactions or changes to Query, Metasrv, the append wire,
  storage engines, object reference accounting, or commit coordination.
