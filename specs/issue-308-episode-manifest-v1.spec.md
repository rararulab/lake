spec: task
name: "episode-manifest-v1"
inherits: project
tags: [robotics, episode, manifest, provenance, serde]
---

## Intent

Define the immutable, format-neutral EpisodeManifest that sits between source
format Adapters and the already-implemented Episode/ArtifactRef table contract.
The manifest is the structured authority for one Episode's searchable summary,
recording representations, timelines, stream summaries, Layers, and logical
Artifact bindings; table scalar columns are a derived acceleration surface.

Current reproducer:

1. Let an RRD importer and an MCAP or LeRobot importer each build a valid
   `EpisodeBundleV1` using the current public types.
2. Store importer-defined JSON as the manifest Artifact. One importer names
   timelines and recording selectors one way while the other uses unrelated
   fields, and either importer may describe an Artifact that is absent from the
   top-level ArtifactRef rows or publish scalar summary values that disagree
   with its manifest bytes.
3. Ask one future inspect/training reader to load both Episodes, or let managed
   object GC traverse their retained table versions.

Both bundles satisfy the current table contract, but there is no versioned wire
shape for the reader to decode and no validation joining manifest semantics to
GC-visible ArtifactRefs. The result is either format-specific core code,
ambiguous training inputs, or a manifest-described object that can be reclaimed
because it was never retained by an ArtifactRef.

Required behavior: Lake exposes a strict EpisodeManifest v1 JSON contract in
`lake-common`. Valid construction canonicalizes stable collections and enforces
cross-reference invariants. Decoding rejects corrupt, unsupported, unknown, or
non-canonical wire values. Binding a manifest to ArtifactRefs derives the
Episode summary row and requires a bidirectional exact match between every
manifest Artifact binding and every non-manifest ArtifactRef before an
`EpisodeBundleV1` is returned.

This advances the `goal.md` ingest -> inspect -> select loop and the stated
requirement that Episode identity remain independent of RRD, MCAP, LeRobot,
file, and shard boundaries. It preserves Lake as the data/revision authority
without making it a Rerun Hub clone, model-training orchestrator, or
cross-table transaction engine.

## Decisions

- Add transport-neutral v1 value types for an Episode summary, Recording,
  Timeline, Stream, Layer, and logical Artifact binding, aggregated by
  `EpisodeManifestV1`. They use Lake-owned strings/enums and contain no format
  parser, storage-engine, Arrow, SDK, Rerun, ROS, MCAP, or LeRobot types.
- The JSON wire carries an explicit `format_version = 1`, denies unknown
  fields, and is encoded as compact UTF-8 JSON. Decode validates the version,
  rebuilds the aggregate through the same constructor, and rejects a wire value
  whose collection ordering or duplicates are not canonical.
- The Episode summary contains the same robot/task/time/outcome values as the
  Episode table row, but not `manifest_artifact_id`. Binding receives that
  already-uploaded logical Artifact identity and derives `EpisodeRecordV1`;
  callers never supply a second independent scalar summary.
- A Recording has a stable `recording_id`, open format discriminator, and
  optional producer version. One logical Recording may be backed by several
  Artifacts, and one physical Artifact may be selected by several Episodes.
- A Timeline has a stable identity and a closed v1 kind of `sequence` or
  `timestamp`. A Stream belongs to one Recording, references one or more known
  Timelines, and may declare media type, codec, or schema fingerprint. Detailed
  per-chunk/entity/topic metadata remains in the source Artifact or an immutable
  sidecar rather than expanding this manifest.
- A Layer has a stable identity and a closed v1 kind covering base,
  annotation, prediction, quality, embedding, and visualization data. A valid
  Episode has exactly one base Layer; producer details remain optional strings
  in v1 and do not introduce workflow orchestration.
- Each manifest Artifact binding mirrors the logical sibling fields of one
  non-manifest `ArtifactRefV1`: artifact, Layer, role, optional Recording,
  selector, schema fingerprint, and producer version. It may associate stream
  identities and may bind an index/sidecar to another known Artifact identity.
- Aggregate validation requires non-empty and unique identities, resolves every
  Recording/Timeline/Stream/Layer reference, prevents self-referential
  sidecars, and canonicalizes all set-like collections by stable identity.
- Binding requires one existing `role = manifest` ArtifactRef for the supplied
  manifest identity, verifies that its media type, byte length, and SHA-256
  equal this manifest's canonical JSON bytes, then compares manifest bindings
  and all other ArtifactRefs as a canonical multiset. Missing, extra, stale, or
  semantically mismatched references fail with a typed error before Arrow
  encoding.
- The manifest intentionally does not list its own Artifact binding. This
  avoids a digest/self-reference cycle; its top-level ArtifactRef binds the
  immutable manifest bytes after upload.
- Publish a v1 manifest media-type constant. No `DataLocation`, URI, digest,
  object bytes, credentials, signed URLs, or arbitrary untyped JSON extension
  bags appear in the manifest wire.
- Keep the contract pure and I/O-free in `lake-common`; format-specific
  selectors remain opaque Adapter-owned strings and validation uses bounded
  memory proportional only to one Episode's manifest metadata.
- Preserve the existing Episode/ArtifactRef public types and Arrow schema. This
  issue adds no Query or Metasrv request and no data-plane behavior.
- Update architecture/design status only where needed to record the implemented
  manifest contract and keep Adapters, generic append, Viewer, and TrainingView
  work explicitly planned.

## Boundaries

### Allowed Changes
Cargo.lock
crates/lake-common/**
docs/architecture.md
docs/design/robot-training-lakehouse.md
specs/issue-308-episode-manifest-v1.spec.md
verification/report.md

### Forbidden
crates/lake-engine/**
crates/lake-engine-lance/**
crates/lake-meta/**
crates/lake-catalog/**
crates/lake-query/**
crates/lake-metasrv/**
crates/lake-sdk/**
crates/lake-cli/**
crates/lake-flight/**
crates/lake-objects/**
crates/lake-iceberg/**
goal.md
site/**
.github/**
changing DataLocation or the Episode/ArtifactRef Arrow schema
embedding object identity, bytes, credentials, or signed URLs in the manifest
adding Rerun, MCAP, LeRobot, ROS, Arrow, or storage-engine dependencies
implementing format parsers, generic append, Viewer, Python, or TrainingView APIs

## Completion Criteria

Rule: manifest-wire-v1 — strict canonical format-neutral metadata

Scenario: v1 manifest canonically round-trips two recording representations
  Test:
    Package: lake-common
    Filter: episode_manifest_v1_roundtrips_two_recording_formats
  Level: unit
  Targets: crates/lake-common/src/episode_manifest.rs
  Given one Episode with RRD and MCAP Recording descriptors, sequence and
  timestamp Timelines, Stream summaries, a base Layer, and several Artifact
  bindings
  When the manifest is constructed, encoded, and decoded
  Then the decoded value equals the canonical manifest, carries version 1, and
  its wire contains no DataLocation, URI, digest, credential, or object bytes

Rule: artifact-binding-v1 — manifest semantics and table reachability agree

Scenario: binding derives the table summary and proves complete reachability
  Test:
    Package: lake-common
    Filter: episode_manifest_v1_binds_complete_artifact_refs
  Level: unit
  Targets: crates/lake-common/src/episode_manifest.rs, crates/lake-common/src/robotics.rs
  Given a canonical manifest, its uploaded manifest ArtifactRef, and exact
  non-manifest ArtifactRefs for every manifest binding
  When the manifest binds them into an EpisodeBundleV1
  Then the Episode row is derived from the manifest summary and every logical
  binding has one GC-visible top-level ArtifactRef with matching semantics,
  while the manifest ArtifactRef matches the canonical JSON media type, size,
  and SHA-256

Scenario: missing, extra, or mismatched ArtifactRefs fail closed
  Test:
    Package: lake-common
    Filter: episode_manifest_v1_rejects_artifact_binding_mismatch
  Level: unit
  Targets: crates/lake-common/src/episode_manifest.rs
  Given a valid manifest but ArtifactRefs with stale manifest bytes, a missing
  object, an extra object, a duplicate object, a different
  Layer/role/format/selector, or another episode_id
  When binding is attempted
  Then each case returns a typed contract error and no EpisodeBundleV1

Scenario: corrupt, future, and non-canonical manifest wires are rejected
  Test:
    Package: lake-common
    Filter: episode_manifest_v1_rejects_invalid_wire
  Level: unit
  Targets: crates/lake-common/src/episode_manifest.rs
  Given malformed JSON, an unsupported format version, an unknown field,
  duplicate identities, dangling references, and unsorted set-like collections
  When EpisodeManifest v1 decode is attempted
  Then each input returns a typed decode or validation error rather than a
  partially valid manifest

## Out of Scope

- Uploading the manifest Artifact or adding a public generic RecordBatch append
  path.
- Implementing RRD, MCAP, or LeRobot readers, metadata extraction, selector
  parsing, RRD footer reads, or derived RRD Materializations.
- DatasetRevision retention, TrainingView, deterministic splits, Python/PyTorch
  readers, Viewer launch, authorization, or direct-read capabilities.
- Nested FILE references, table schema changes, GC protocol changes, or
  cross-table transactions.
