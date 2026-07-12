spec: task
name: "idempotent-stage-cleanup-race"
inherits: project
tags: [engine, lance, concurrency, idempotency, recovery, references]
---

## Intent

Make same-operation concurrent Lance appends and crash recovery converge even
when one actor deletes the shared reference-staging journal after terminal
finalization while another actor is about to read it. Missing staging is
success only when durable transaction history and complete final sidecars prove
the exact terminal operation; otherwise reference lineage still fails closed.

## Decisions

- Keep the operation-derived stage identity and immediate bounded cleanup.
- Do not serialize table appends or introduce process-local correctness locks.
- If staging persistence loses an already-observed chunk, `append_reserved`
  re-runs exact operation reconciliation before returning the storage error.
- If finalization loses a staged chunk, re-check the complete final sidecar set;
  success is valid only when all chunks for the committed version are durable.
- Use test-only Tokio `Notify` barriers at the two storage boundaries to force
  winner cleanup between contender observation and read deterministically.
- Preserve payload-digest conflict detection before returning a committed
  version for any replay.

## Boundaries

### Allowed Changes
crates/lake-engine-lance/**
docs/architecture.md
docs/plans/2026-07-12-idempotent-stage-cleanup-race.md
specs/issue-86-idempotent-stage-cleanup-race.spec.md
verification/issue-86-idempotent-stage-cleanup-race.md

### Forbidden
crates/lake-engine/**
crates/lake-meta/**
crates/lake-metasrv/**
global or per-table append serialization
sleep-based or probabilistic race tests
unbounded retry loops
retaining terminal or abandoned staging forever
treating missing staging as success without complete durable final sidecars
weakening tenant, operation-ID, or payload-digest matching

## Completion Criteria

Scenario: Concurrent append survives terminal staging deletion
  Test:
    Package: lake-engine-lance
    Filter: same_operation_append_survives_terminal_stage_cleanup
  Given two handles appending the exact same operation and payload
  When the winner deletes staging after the contender observes an existing chunk but before it reads
  Then both calls return the same committed version and terminal staging is absent

Scenario: Concurrent recovery survives terminal staging deletion
  Test:
    Package: lake-engine-lance
    Filter: concurrent_recovery_survives_terminal_stage_cleanup
  Given a committed append whose final reference sidecar still needs recovery
  When one reconciler completes and deletes staging after another has observed incomplete final state
  Then both reconcilers return the committed version and complete final lineage remains readable

Scenario: Replay with a different payload is rejected
  Test:
    Package: lake-engine-lance
    Filter: different_payload_replay_remains_idempotency_conflict
  Given a terminal append for one tenant and operation ID
  When a replay uses the same identity with a different payload digest
  Then reconciliation fails with `IdempotencyConflict` rather than returning the committed version

Scenario: Missing staging without final lineage fails closed
  Test:
    Package: lake-engine-lance
    Filter: missing_stage_without_final_lineage_fails_closed
  Given a committed transaction whose staging was lost before final reference sidecars were written
  When exact-operation reconciliation runs
  Then it returns an error and does not claim the reference lineage is complete

## Out of Scope

- A general staging garbage collector or retention policy.
- Cross-process distributed locks.
- Changing Lance transaction-history layout.
- Reference-sidecar format changes.
