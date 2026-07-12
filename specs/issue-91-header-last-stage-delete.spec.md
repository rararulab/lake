spec: task
name: "operation-fenced-reference-stage-lifetime"
inherits: project
tags: [engine, lance, idempotency, cleanup, concurrency, maintenance]
---

## Intent

Make reference-stage lifetime follow the durable append-operation lifetime.
Terminal append/reconcile must not race a same-operation persister by deleting
its stage. Expired-operation GC must reclaim the exact stage before deleting
the only durable record that can identify and retry that cleanup.

## Decisions

- Lance retains an exact operation's stage after final lineage publication.
- `TableHandle::expire_append` is an engine-neutral, default-no-op lifecycle
  hook; Lance implements it as bounded deletion of the exact stage prefix.
- Metasrv calls the hook while holding the table lock and before deleting the
  expired operation record. A cleanup error retains the record for retry.
- Missing/recreated tables skip exact-stage cleanup because their old dataset
  is owned by the durable drop lifecycle; the record can still expire.
- Chunk zero remains a header-last publication marker. Cleanup rejects a
  present malformed header and drains a missing/unpublished stage prefix under
  the per-append chunk bound.

## Boundaries

### Allowed Changes
crates/lake-engine/**
crates/lake-engine-lance/**
crates/lake-metasrv/**
docs/architecture.md
docs/plans/2026-07-12-header-last-stage-delete.md
specs/issue-91-header-last-stage-delete.spec.md
verification/issue-91-header-last-stage-delete.md

### Forbidden
crates/lake-common/**
crates/lake-meta/**
crates/lake-query/**
crates/lake-sdk/**
timing sleeps or probabilistic race tests
unbounded prefix collection or deletion
deleting an operation record after stage-cleanup failure
accepting a present malformed header

## Completion Criteria

Scenario: Replay stage lives until explicit operation expiry
  Test:
    Package: lake-engine-lance
    Filter: append_stage_is_retained_until_explicit_expiry
  Given a committed append whose final reference lineage is durable
  When normal append completion returns
  Then its exact stage remains replayable until expire_append removes it

Scenario: Operation GC reclaims stage before durable identity
  Test:
    Package: lake-metasrv
    Filter: operation_gc_reclaims_exact_lance_stage_before_record
  Given an expired committed operation with a retained Lance stage
  When bounded operation GC processes its durable record under the table lock
  Then the exact stage is absent before the operation record is deleted

Scenario: Headerless crash residue is reclaimed boundedly
  Test:
    Package: lake-engine-lance
    Filter: expire_append_drains_headerless_multichunk_crash_residue
  Given a publisher wrote multiple non-header chunks and crashed before chunk zero
  When the coordinator expires the exact operation
  Then bounded prefix cleanup removes every invisible residue chunk

Scenario: Same-operation append regression remains convergent
  Test:
    Package: lake-engine-lance
    Filter: lance_transaction_history_converges_idempotent_append
  Given two handles appending the exact same operation concurrently
  When commit and finalization interleave naturally
  Then both return the same version without staging NotFound

Scenario: Cleanup failure keeps the operation retryable
  Test:
    Package: lake-metasrv
    Filter: operation_gc_retains_record_when_stage_cleanup_fails
  Given an expired operation whose exact Lance stage is malformed
  When operation GC cannot safely reclaim the stage
  Then the stage and durable operation record remain for diagnosis and retry

Scenario: Malformed publication header remains fail-closed
  Test:
    Package: lake-engine-lance
    Filter: stage_cleanup_rejects_malformed_publication_header
  Given a present stage header that cannot declare a valid chunk set
  When operation expiry requests cleanup
  Then the call returns an error and the malformed marker remains

## Out of Scope

- Retrying arbitrary object-store errors inside one cleanup call.
- Changing transaction-history or final-reference formats.
- Reclaiming datasets already handed to the drop-table lifecycle.
- Online mixed-version writers using the pre-publication-marker protocol; drain
  commit-capable nodes before upgrading them together.
