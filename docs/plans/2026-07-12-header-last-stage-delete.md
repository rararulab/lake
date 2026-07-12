# Operation-fenced reference-stage lifetime plan

**Goal:** remove terminal publish/delete races and make crash residue
deterministically reclaimable without an unbounded object-store scan.

1. Keep header-last publication and bounded exact-prefix cleanup.
2. Add an engine-neutral operation-expiry hook with a default no-op.
3. Stop deleting Lance stages from append and reconciliation success paths.
4. Invoke exact-stage cleanup from expired-operation GC while holding the table
   lock and before deleting the durable operation record.
5. Keep the record on cleanup failure so the paged GC cursor can retry it.
6. Prove engine retention/expiry, Metasrv cleanup ordering, same-operation
   convergence, and malformed-header fail-closed behavior deterministically.
7. Run spec lifecycle, affected package tests/clippy, full gate, rustdoc,
   independent correctness review, and fixed-head verification before merge.
