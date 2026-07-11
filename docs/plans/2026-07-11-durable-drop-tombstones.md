# Durable Drop Tombstones — Implementation Plan

Issue: #37

## Protocol

For an exact table registration `R` with incarnation `I`:

1. CAS-create immutable `drop/<namespace>/<name> = Tombstone(I, R)` through
   the lease-fenced production metastore.
2. Conditionally delete the registry pointer only if it is still exactly `R`.
   A missing pointer is already converged; a different incarnation is never
   removed.
3. Call the engine's idempotent remove on `R.location`.
4. Exact-delete the tombstone through the fenced metastore.

Each remote create receives a unique generation-qualified location. Therefore
step 3 may safely finish after leadership changes or after a replacement is
created: old and new object prefixes cannot overlap.

## Work units

1. Write RED tests for generation-qualified placement and tombstone encoding,
   exact identity, bounded listing, and idempotent prepare.
2. Add the compact tombstone module and bounded maintenance cursor. Keep the
   record immutable so every transition is an exact CAS/delete, not a mutable
   procedure log.
3. Write RED crash-window tests using pausing/failing engine and metastore test
   doubles. Implement prepare, registry detach, engine cleanup, and finalize as
   one restartable per-table routine.
4. Make table creation drain same-name tombstones before materializing a new
   unique generation. Add the delayed-old-remove/recreate hostile test.
5. Re-enable Flight `drop_table`, update action descriptions and two-node
   forwarding tests, then prove repeat-after-handoff convergence.
6. Add bounded leader-maintenance tombstone recovery and tests showing a
   successor completes cleanup without another client request.
7. Update architecture and verification evidence. Run explicit candidate
   spec lifecycle, strict clippy, LocalStack-relevant tests, clean gate,
   independent reviewer, and independent verifier before push.

## Safety review checklist

- No object delete occurs before durable tombstone publication.
- No registry delete can remove a replacement registration.
- No remote create reuses an old dataset prefix.
- A stale leader may continue old-prefix object deletion, but cannot mutate
  tombstone/registry metadata after takeover.
- Corrupt tombstones fail closed and remain inspectable.
- Maintenance processes a bounded page and advances a durable-process-local
  cursor without loading all tombstones.
