# lake-common

Shared transport-neutral types used across every tier. Foundation crate —
depends on no other workspace crate, so everyone can depend on it.

## Invariants

- No I/O and no tier-specific dependencies.
- Wire protocols are versioned, validate on decode, and never carry credentials.
- FILE append identities are UUIDv7 values; payload digests are lowercase
  SHA-256 and remain tenant-neutral until authenticated by the server.
- `EpisodeBundleV1` validates the initial Episode-to-Artifact aggregate before
  Arrow encoding; format and storage-engine types never enter this crate.
- `EpisodeManifestV1` is canonical, strict JSON metadata: its table summary is
  derived, and binding verifies the uploaded manifest plus every ArtifactRef.
- `Version` is opaque: the registry stores and compares it, never interprets
  it. Each engine decides what it encodes.

## Layout

- `ids.rs` — `Namespace`, `TableName`, `TableRef`, `Version`
- `location.rs` — `TableLocation` (a table's dataset URI)
- `file_write.rs` — transport-neutral idempotent FILE append command payload
- `managed_stage.rs` — versioned, credential-free managed-stage discovery
- `episode_manifest.rs` — canonical Episode metadata and ArtifactRef binding
- `episode_manifest_tests.rs` — manifest wire, binding, and rejection contracts
- `robotics.rs` — format-neutral Episode/ArtifactRef v1 values and invariants
