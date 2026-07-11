spec: task
name: "lease-fenced-mutations"
inherits: project
tags: [metadata, ha, lease, fencing]
---

## Intent

Add the durable primitive needed to prevent a paused former metadata leader
from publishing state after another node takes over. Election leases gain a
monotonic epoch, and `MetaStore` gains an atomic guarded compare-and-swap that
checks the exact current lease record in the same transaction as a target
create, update, or delete.

Without this work, an old leader can pass the local deadline check, pause,
resume after takeover, and perform an ordinary registry CAS that is unrelated
to the lease. Reproducer: leader A reads a table registration and pauses;
after its lease expires, leader B takes over; A resumes and CAS-updates the
target key. The target mutation succeeds because the registry expected value
still matches. With a guarded CAS, B's takeover changes the guard record and
A's target remains byte-for-byte unchanged.

This advances `goal.md`'s metadata failover signal while preserving the
bounded stateful authority. It is the storage/election foundation for fencing
create, append, drop, and maintenance in subsequent integration work; it does
not put coordination on the query read path.

## Decisions

- `LeaseValue` carries a serde-backward-compatible monotonic `epoch`. A first
  acquisition uses epoch 1. A legacy epoch 0 is upgraded to 1 by the next
  successful renewal or takeover; after that, same-holder renewal preserves
  the epoch and takeover increments it. Epoch exhaustion fails closed.
- `MetaStore` exposes one atomic guarded mutation capable of target create,
  exact-value update, and exact-value delete. The guard and target must be
  different keys.
- RocksDB implements the operation under its existing write mutex and one
  write batch. DynamoDB implements it with `TransactWriteItems` containing a
  guard `ConditionCheck` plus conditional target `Put` or `Delete`.
- A guard or target condition mismatch returns `Ok(false)` with no target
  mutation. Capacity, transaction conflict, serialization, and backend errors
  remain typed errors; they must not be flattened into a false condition.
- Backends without a native implementation fail explicitly rather than
  emulating two non-atomic operations.
- This issue supplies the primitive and election token only. Wiring every
  Metasrv mutation through it is the immediately following task.

## Boundaries

### Allowed Changes
- `crates/lake-meta/**`
- `crates/lake-metasrv/**`
- `docs/architecture.md`
- `docs/plans/**`
- `specs/**`
- `verification/**`

### Forbidden
- `crates/lake-common/**`
- `crates/lake-engine/**`
- `crates/lake-engine-lance/**`
- `crates/lake-catalog/**`
- `crates/lake-query/**`
- `crates/lake-sdk/**`
- `crates/lake-cli/**`

## Constraints

- Do not emulate guarded mutation as a guard read followed by an ordinary CAS.
- Do not treat Dynamo transaction conflicts or throttling as condition mismatch.
- Do not change registry layout or engine commit ordering in this foundation.

## Completion Criteria

Scenario: lease epoch changes on takeover but not renewal
  Test:
    Package: lake-metasrv
    Filter: lease_epoch_advances_only_on_takeover
  Given legacy and epoch-bearing leases held by node A
  When A renews and node B later takes over the expired lease
  Then legacy epoch 0 upgrades to 1, later renewals preserve the epoch, and B
  receives the next epoch

Scenario: guarded target mutation is atomic with the lease condition
  Test:
    Package: lake-meta
    Filter: guarded_mutation_rejects_stale_guard_without_target_change
  Given a guard and a target value in RocksDB
  When the guard changes before a stale guarded create, update, or delete
  Then each operation returns false and the target remains unchanged

Scenario: current guard permits create update and delete
  Test:
    Package: lake-meta
    Filter: guarded_mutation_applies_all_target_transitions
  Given an exact current guard value
  When guarded create, exact update, and exact delete run in sequence
  Then every transition succeeds and the target has the expected final state

Scenario: Dynamo guarded mutation uses the production atomic contract
  Test:
    Package: lake-meta
    Filter: dynamo_guarded_mutation_is_wired
  Given the Dynamo backend implementation
  When the source contract is inspected by its non-ignored wiring test
  Then it contains a transaction guard and conditional target mutation

Scenario: a guarded mutation cannot target its own guard key
  Test:
    Package: lake-meta
    Filter: guarded_mutation_rejects_same_key
  Given a current guard value
  When a caller tries to update that same key as the guarded target
  Then the operation returns an invalid-mutation error and the guard is
  unchanged

## Out of Scope

- Wiring registry create/version/delete and append operation records to the
  new primitive.
- Durable drop tombstones and destructive storage cleanup recovery.
- Query caching, SQL behavior, object upload, or table placement.
