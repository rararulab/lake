# Bounded Rust SDK batch insert plan

**Goal:** amortize metadata commits across a finite group of episode rows
without routing large object bytes through SQL or Flight.

## Task 1: Lock the public contract

1. Add an owned multi-row SDK API and typed empty/limit errors.
2. Keep the existing single-row API source-compatible by delegating to it.
3. Validate every row before any upload begins.

## Task 2: Build one bounded Arrow batch

1. Reorder each row into table-schema order after validation.
2. Upload FILE values sequentially through the existing managed store.
3. Build one array per field and one `RecordBatch` for all rows.
4. Reuse the existing operation identity, digest, retry, and resume path.

## Task 3: Prove behavior

1. Commit several distinct files and scalar values in one version.
2. Query every row and directly read every returned `DataLocation`.
3. Inject a bad later row and prove zero uploads and no version advance.
4. Reject empty/excessive batches before any endpoint or store access.

## Task 4: Ship

1. Update the managed FILE example, README, and architecture contract.
2. Run spec lifecycle, strict SDK clippy, full gate, independent review, and
   independent verification before merge.
