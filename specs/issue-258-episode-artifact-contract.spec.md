spec: task
name: "episode-artifact-table-contract-v1"
inherits: project
tags: [robotics, episode, artifact, arrow, gc]
---

## Intent

Turn the robot-training direction in `goal.md` into its first falsifiable data
contract: one logical Episode can be stored beside several immutable Artifact
references without making Episode identity equal a file or hiding any managed
object from Lake's incremental GC lineage.

Current reproducer:

1. Define an Episode summary row and two ArtifactRef rows in one Dataset table:
   one reference is the Episode manifest and one is a base Recording.
2. Because the table mixes logical rows with physical references, the Episode
   row needs a null `object FILE` cell while each ArtifactRef needs a complete
   top-level `DataLocation` value.
3. Append the batch through the Lance engine and enumerate retained object
   references.

The current reference extractor rejects the null Episode cell before commit,
so the documented two-record-kind table cannot be represented. If an importer
instead hides both objects in a manifest, the append succeeds but GC does not
see either reference. If it makes every Episode equal one FILE, shared shards
and multi-Artifact Episodes become impossible.

Required behavior: Lake exposes one versioned, format-neutral Episode bundle
contract whose Arrow batch contains an Episode summary plus one top-level FILE
per ArtifactRef. The engine treats a null FILE cell as no reference, rejects a
partially-null FILE identity, and publishes every present Artifact as retained
reference lineage before the table version becomes visible.

This advances the `goal.md` ingest/inspect/select loop and the signal that
immutable multimodal Episode data remains reproducible at exact table versions.
It preserves per-table commits, direct object-storage I/O, engine neutrality,
and the rule that Query and Metasrv carry metadata only. It does not make Lake a
training orchestrator, a Rerun Hub clone, or a cross-table transaction engine.

## Decisions

- Add transport-neutral v1 records for an Episode summary, ArtifactRef, and an
  Episode bundle. The contract uses Lake-owned scalar/string identifiers and
  `DataLocation`; it contains no Rerun, MCAP, LeRobot, Lance, URI-credential, or
  parser-specific type.
- The v1 Arrow schema is one flat Dataset-table schema with a non-null
  `record_kind` discriminator and non-null `episode_id`. Fields belonging to the
  other record kind are nullable at the Arrow level and are validated by the
  contract encoder.
- An Episode row requires a non-empty `episode_id` and
  `manifest_artifact_id`. Searchable robot/task/time/outcome values remain
  optional scalar columns. Its ArtifactRef-only fields, including `object`, are
  null.
- An ArtifactRef row requires non-empty `episode_id`, `artifact_id`, `layer_id`,
  and `role`, plus one complete top-level `object FILE` using the exact existing
  `DataLocation` struct shape. Format, selector, schema fingerprint, and
  producer version are optional sibling columns, never additions to
  `DataLocation`.
- An initial Episode bundle contains exactly one Episode summary and at least
  one ArtifactRef. It must contain a `role = manifest` reference whose
  `artifact_id` matches `manifest_artifact_id`; every reference belongs to the
  same Episode. Invalid bundles fail before a RecordBatch is returned.
- Several ArtifactRef rows may carry the same immutable object with different
  Episode selectors; physical deduplication must not change logical Episode
  identity. GC lineage may deduplicate identical object identities but must
  retain every distinct object present in the batch.
- The Lance reference extractor skips only a null parent FILE cell. For every
  non-null parent, all four identity children remain mandatory; a partial null
  fails the append before manifest publication.
- Update the direction/architecture text only where needed to record the exact
  v1 encoding and distinguish implemented contract from later manifest,
  adapter, SDK, and TrainingView work.

## Constraints

- `DataLocation` and its Arrow child fields remain unchanged.
- Object bytes never enter the Episode RecordBatch, Query, Metasrv, engine
  reference sidecars, or metadata KV.
- The core contract must not depend on a concrete recording format or storage
  engine.
- Reference discovery stays streaming and bounded; no table-row scan is added
  to GC.
- The existing manifest-first, reference-lineage-complete, registry-CAS commit
  order remains unchanged.

## Boundaries

### Allowed Changes
Cargo.lock
crates/lake-common/**
crates/lake-objects/**
crates/lake-engine-lance/**
docs/architecture.md
docs/design/robot-training-lakehouse.md
specs/issue-258-episode-artifact-contract.spec.md

### Forbidden
crates/lake-engine/**
crates/lake-meta/**
crates/lake-catalog/**
crates/lake-query/**
crates/lake-metasrv/**
crates/lake-sdk/**
crates/lake-cli/**
crates/lake-flight/**
crates/lake-iceberg/**
goal.md
site/**
.github/**
changing the DataLocation Arrow shape
embedding object bytes or credentials in table metadata
adding Rerun, MCAP, LeRobot, ROS, or Python dependencies
adding format parsers, viewers, training readers, or TrainingView APIs

## Completion Criteria

Scenario: v1 bundle encodes one logical Episode with several physical Artifacts
  Test:
    Package: lake-objects
    Filter: episode_artifact_table_v1_encodes_multi_artifact_bundle
  Given one Episode summary, its manifest ArtifactRef, and a base Recording
  ArtifactRef
  When the v1 contract encodes the bundle as one Arrow RecordBatch
  Then it emits one episode row plus two artifact_ref rows, the episode row has
  a null object cell, and both artifact_ref rows have top-level exact
  DataLocation values under the same episode_id

Scenario: a missing manifest reference fails before encoding
  Test:
    Package: lake-objects
    Filter: episode_artifact_table_v1_rejects_missing_manifest_reference
  Given an Episode whose manifest_artifact_id has no matching role=manifest
  ArtifactRef
  When the v1 contract validates the bundle
  Then it returns a typed contract error and produces no RecordBatch

Scenario: every present Artifact remains visible to retained-reference GC
  Test:
    Package: lake-engine-lance
    Filter: nullable_file_rows_keep_present_episode_artifacts_reachable
  Given one encoded Episode bundle with a null FILE cell on the Episode row and
  distinct manifest and Recording FILE values on ArtifactRef rows
  When the batch is appended and retained object references are enumerated
  Then the append commits and retained lineage contains both distinct object
  identities without treating the null Episode cell as an object

Scenario: a partially-null Artifact identity fails closed
  Test:
    Package: lake-engine-lance
    Filter: partially_null_file_identity_fails_before_manifest_publication
  Given a nullable FILE column whose parent is present but one DataLocation
  child is null
  When the Lance engine consumes the append batch
  Then the append returns an error and the table version does not advance

## Out of Scope

- Defining or uploading the versioned Episode manifest Artifact.
- Adding generic public SDK RecordBatch append; this contract supplies the
  schema/batch primitive for that later issue.
- RRD, MCAP, or LeRobot parsing and materialization.
- DatasetRevision retention, TrainingView, Python/PyTorch readers, derived
  Layer append, authorization, or Viewer integration.
- Nested FILE reference extraction or changing GC removal semantics.
