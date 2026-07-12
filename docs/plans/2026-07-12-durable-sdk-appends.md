# Durable SDK append recovery implementation plan

**Goal:** Make the exact FILE append operation recoverable after an SDK process
crash without re-uploading large objects or risking a duplicate row.

## Task 1: Lock the durable format and bounds

1. Add a private versioned append-checkpoint record containing stage identity,
   operation ID, exact encoded Flight messages, and an integrity digest.
2. Derive checkpoint paths only from validated operation IDs and use a distinct
   suffix from multipart-upload checkpoints.
3. Enforce per-file and directory-entry ceilings before reading payload bytes.
4. Publish with write, file sync, atomic rename, and parent-directory sync.
5. Open recovery files without following links, validate size on the opened
   handle, and cap the actual read even if the file grows concurrently.

## Task 2: Bind persistence to append lifecycle

1. Persist a prepared append before its first Flight RPC when a checkpoint root
   is configured.
2. Retain state for ambiguous transport failures and retry-window expiry.
3. Remove state after success or an unambiguous terminal rejection.
4. Keep the existing in-memory path byte-for-byte compatible when durable state
   is not configured.

## Task 3: Add restart recovery APIs

1. List a bounded, sorted set of pending operation IDs without loading payloads.
2. Load one operation only after filename, size, format, integrity, stage,
   descriptor operation, and digest validation.
3. Resume the loaded operation through the existing `resume_append` path so no
   new operation ID or object upload is possible.
4. Document and enforce the finite server operation-retention horizon.

## Task 4: Prove crash boundaries and fail-closed behavior

1. Simulate loss before the first conclusive response and recover with a fresh
   SDK instance.
2. Commit once while preserving the checkpoint, then prove restart replay
   returns the original version and clears state.
3. Exercise success, terminal rejection, and ambiguous failure cleanup rules.
4. Exercise corrupt, oversized, path-invalid, stage-mismatched, and descriptor-
   mismatched state before any network append.

## Task 5: Operate and ship

1. Document checkpoint ownership, durability, recovery commands/API, bounds,
   and the single-trust-domain directory requirement.
2. Run spec lifecycle, package tests, strict clippy, full gate, and rustdoc.
3. Obtain independent correctness review and independent fixed-head verification
   before publishing and merging.
