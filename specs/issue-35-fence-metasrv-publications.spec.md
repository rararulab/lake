spec: task
name: "fence-metasrv-publications"
inherits: project
tags: [metadata, ha, lease, fencing]
---

## Intent

Route every metadata publication performed by the production Metasrv through
the atomic lease guard delivered by issue #33. The write path must fetch the
latest exact lease bytes immediately before each CAS or delete so ordinary
same-holder renewals do not spuriously invalidate a long engine operation,
while a takeover prevents a paused former leader from publishing after it
resumes.

## Decisions

- Election returns the exact serialized lease bytes it successfully installed.
  Leadership publishes those bytes together with the epoch and local monotonic
  deadline; an expired local deadline never yields a guard.
- Production Metasrv uses a metadata-store adapter that delegates reads but
  translates every CAS/delete into `guarded_mutate` using a freshly loaded
  leadership guard. Election itself continues to use the raw store.
- Registry create/version/delete, append operation records and active fences,
  terminal publication/cleanup, and maintenance/operation-GC metadata all pass
  through the adapter without read-then-CAS lease emulation.
- An engine commit followed by a rejected stale publication remains
  recoverable by the new leader through existing operation reconciliation.
- Destructive drop is not claimed safe by guarding only the final registry
  delete. Until a durable tombstone protocol exists, the production remote
  drop path must fail closed before deleting objects.

## Boundaries

### Allowed Changes
- `crates/lake-meta/**`
- `crates/lake-metasrv/**`
- `crates/lake-cli/**`
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

## Constraints

- Do not use one lease snapshot for an entire long-running engine operation.
- Do not route election lease CAS through the fenced adapter.
- Do not delete table objects before a durable drop tombstone exists.

## Completion Criteria

Scenario: paused former leader cannot publish after takeover
  Test:
    Package: lake-metasrv
    Filter: stale_leader_cannot_publish_after_takeover
  Given leader A pauses before a metadata CAS and leader B takes over
  When A resumes with its formerly valid local state
  Then A's CAS returns false and the target remains unchanged

Scenario: renewal does not invalidate a long operation
  Test:
    Package: lake-metasrv
    Filter: publication_uses_fresh_guard_after_renewal
  Given one holder renews while an engine operation remains in progress
  When the operation publishes after the renewal
  Then it uses the newly published exact lease bytes and succeeds at the same epoch

Scenario: append recovery converges after stale publication rejection
  Test:
    Package: lake-metasrv
    Filter: append_recovers_after_stale_leader_engine_commit
  Given leader A commits an engine version and loses leadership before publication
  When leader B retries the same operation identity
  Then B reconciles and publishes one version without duplicate rows

Scenario: production remote drop fails before object deletion
  Test:
    Package: lake-metasrv
    Filter: remote_drop_requires_durable_tombstone
  Given a registered table on a production fenced control plane
  When a client requests drop before tombstones are implemented
  Then the request fails closed and engine remove is never called

Scenario: all production metadata CAS and delete operations are fenced
  Test:
    Package: lake-metasrv
    Filter: production_metadata_mutations_use_guarded_store
    Level: integration
    Test Double: recording raw metastore
  Given a production Metasrv server view backed by a recording raw metastore
  When its target CAS and delete interface is exercised
  Then no ordinary target CAS or delete reaches the raw metastore

## Out of Scope

- Durable drop tombstones and object cleanup recovery.
- DynamoDB key-schema migration or query memory limits.
