spec: task
name: "durable-drop-tombstones"
inherits: project
tags: [metadata, ddl, recovery, fencing]
---

## Intent

Restore production remote table drop through a durable, restartable protocol.
No crash or leader takeover may leave a registry entry pointing at deleted
objects, and delayed cleanup from an old table incarnation must never delete a
same-name replacement.

## Decisions

- Persist one immutable tombstone under `drop/<namespace>/<name>/<incarnation>`
  before deleting the exact registry registration or touching table objects.
  The tombstone contains the exact registration, including its incarnation and
  old dataset location.
- Execute steps in this order: guarded tombstone create, guarded conditional
  registry delete, idempotent engine remove, guarded exact tombstone delete.
  Retrying any prefix of the sequence must converge.
- Production/server-authoritative creates use a unique storage generation in
  every dataset location. This is the physical fence that makes a delayed old
  `engine.remove` harmless after drop/recreate; the metadata incarnation remains
  the logical fence.
- A create first resumes outstanding tombstones for the same table while
  holding its table coordinator. Leader maintenance also scans a bounded page
  of tombstones so cleanup does not require another client request.
- All tombstone and registry mutations flow through the production
  lease-fenced metastore view. Object deletion cannot be transactional with the
  lease and therefore relies on unique generation-qualified locations.
- Remote Flight drop is re-enabled only through this protocol and remains
  idempotent when the registration is already absent.

## Boundaries

### Allowed Changes
- `**/Cargo.lock`
- `crates/lake-metasrv/**`
- `crates/lake-meta/**`
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
- storing object bytes or unbounded procedure payloads in the metastore
- reusing a deterministic dataset location across remote table incarnations

## Constraints

- Do not delete objects before the tombstone is durable.
- Do not blindly delete a registry entry; compare the exact old registration.
- Do not require a live original leader to finish cleanup.
- Keep maintenance scans cursor-paged and bounded.

## Completion Criteria

Scenario: crash after tombstone publication resumes safely
  Test:
    Package: lake-metasrv
    Filter: drop_resumes_after_tombstone_publication_crash
  Given a drop crashes after its tombstone is durable but before registry deletion
  When a successor resumes the tombstone
  Then it removes the exact registration, deletes the old dataset, and clears the tombstone

Scenario: partial object cleanup is restartable
  Test:
    Package: lake-metasrv
    Filter: drop_resumes_after_partial_object_deletion
  Given object deletion fails after removing only part of the old dataset
  When cleanup retries through the idempotent engine
  Then all old objects and the tombstone are removed without restoring the registry

Scenario: old cleanup cannot delete a replacement incarnation
  Test:
    Package: lake-metasrv
    Filter: stale_drop_cannot_delete_recreated_table
  Given an old cleanup is delayed while the same table name is recreated
  When the delayed engine removal resumes
  Then it targets only the old generation and the replacement remains readable

Scenario: stale leader cannot finalize drop metadata after takeover
  Test:
    Package: lake-metasrv
    Filter: stale_leader_cannot_finalize_drop_after_takeover
  Given leader A pauses after registry deletion and leader B takes over
  When A resumes and attempts to clear the tombstone
  Then its guarded delete is rejected and B can converge the same tombstone

Scenario: repeated remote drop converges across handoff
  Test:
    Package: lake-metasrv
    Filter: remote_drop_is_idempotent_across_leader_handoff
    Level: integration
  Given a table is remotely dropped while metadata leadership changes
  When the client repeats the same drop through the successor
  Then both requests converge to an absent registration, absent dataset, and absent tombstone

Scenario: server placement isolates table incarnations
  Test:
    Package: lake-metasrv
    Filter: server_placement_uses_unique_dataset_generation
  Given two server-authoritative creates use the same table name
  When their locations are derived
  Then the locations are distinct generation-qualified dataset prefixes

Scenario: tombstone maintenance is cursor-paged and bounded
  Test:
    Package: lake-metasrv
    Filter: drop_tombstone_maintenance_is_bounded
  Given more durable tombstones exist than one configured maintenance page
  When the leader runs one cleanup sweep
  Then it scans at most one page and retains a continuation for the next sweep

## Out of Scope

- General-purpose procedure framework or cross-table transactions.
- Object-store lifecycle policies and orphan inventory GC.
- DynamoDB key-schema migration.
- Query cache push invalidation.
