# Fence Metasrv Publications Implementation Plan

**Goal:** Make every production Metasrv metadata publication atomic with the
latest live leadership lease while preserving engine-commit recovery.

### Task 1: Publish exact lease guards

1. Add failing election/leadership tests for exact installed bytes, epoch, local
   expiry, renewal replacement, and takeover replacement.
2. Return an owned lease guard from successful campaigns and publish it with
   the local monotonic deadline.
3. Expose a fail-closed `current_guard` snapshot to the write path.

### Task 2: Add the production fenced metastore adapter

1. Add failing adapter tests for create/update/delete, stale takeover, fresh
   renewal, same-key rejection, and missing/expired authority.
2. Delegate reads and translate target CAS/delete into issue #33's native
   guarded mutation using a fresh guard per call.
3. Wire production ControlPlane and maintenance to the adapter while election
   retains the raw store.

### Task 3: Prove mutation coverage and recovery

1. Instrument the raw store in tests and exercise registry create/version,
   append record/fence/terminal cleanup, maintenance, and operation GC.
2. Add a deterministic paused-leader engine-commit recovery test with the same
   append operation identity.
3. Fail the remote production drop request before engine removal until the
   durable tombstone task lands.

### Task 4: Verify and publish

1. Document publication fencing and the explicit destructive-drop boundary.
2. Run explicit jj candidate spec lifecycle, strict Clippy, LocalStack, clean
   gate, and rustdoc.
3. Obtain independent reviewer APPROVE and verifier PASS before merge.
