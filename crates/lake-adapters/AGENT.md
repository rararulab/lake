# lake-adapters

Leaf crate for bounded robot-recording metadata inspection.

## Invariants

- Public inputs, outputs, and errors are format-neutral.
- Every adapter returns `lake_common::EpisodeManifestV1` directly.
- Caller-supplied byte, request, scan, and record limits are mandatory.
- Charge every source read before I/O; never return partial manifests.
- RRD and MCAP decoding uses their upstream crates, not local parsers.
- Present corrupt indexes fail closed; only absent indexes may fall back.
- Fallback scans are finite and retain Episode-level aggregate state only.
- Lake-owned Episode, Recording, Layer, and Artifact identities come from context.
- This crate never uploads, commits, authorizes, or proxies recording bytes.

## Layout

- `source.rs` — random-access source contract and budget enforcement
- `model.rs` — neutral context, errors, and manifest mapping
- `rrd.rs` / `mcap.rs` — format-private extraction implementations
