spec: task
name: "idempotent-file-append"
inherits: project
tags: [append, idempotency, commit, flight, ha, lance]
---

## Intent

Make one logical SQL `FILE` append commit at most one physical table version
within Lake's documented idempotency-retention window, even when the final
Flight `PutResult` is lost, requests race, or the metadata leader fails.

Current reproducer:

1. The SDK uploads a managed object and sends its `DataLocation` row through
   Query to Metasrv.
2. Metasrv commits the Lance version and advances the registry pointer.
3. The connection is dropped before the SDK receives `PutResult`.
4. The SDK retries the same logical insert.
5. The current protocol performs a second engine append, advances the registry
   again, and exposes two rows.

Required behavior: the retry returns the first committed `Version`, the engine
contains one append transaction, the registry advances once, and object
reference lineage remains complete.

This advances the `goal.md` signal that a standby resumes from HA-KV-durable
state after metadata-leader failure. It preserves per-table snapshot
publication and does not introduce cross-table transactions, route object
bytes through Query or Metasrv, or couple upper tiers to Lance.

## Decisions

- The idempotency identity is `(tenant, table, operation_id)`. `operation_id`
  is a validated, time-bearing, high-entropy value generated once by the SDK
  after all object uploads complete.
- The SDK retains the encoded Arrow metadata batch, operation ID, and digest
  for all transparent retries of an ambiguous `DoPut`. Retrying the append
  must not upload the video/model bytes again.
- The payload digest is a versioned SHA-256 digest over the actual ordered
  Arrow Flight metadata messages. It covers `DataLocation` values but never
  the referenced object bytes.
- The descriptor carries the declared operation ID, digest algorithm/version,
  and digest. Metasrv must verify the digest against the actual incoming
  `FlightData`; a caller-supplied digest is not trusted.
- Query derives tenant identity from the authenticated inbound `Principal`,
  authorizes the namespace, and forwards the operation descriptor plus an
  authenticated delegated tenant/namespace context. Metasrv derives the
  operation scope only from authenticated transport context, never from a
  client-supplied tenant string. Followers preserve the exact trusted scope
  while forwarding to the leader.
- A replay of the same `(tenant, table, operation_id)` and verified payload
  digest returns the original committed `Version`. Reuse with a different
  verified payload digest returns a deterministic conflict and performs no
  append.
- Metasrv stores only compact CAS-managed operation coordination records in
  `MetaStore`: identity, digest, base/result versions, state, and timestamps.
  Arrow batches, object bytes, credentials, signed URLs, and arbitrary request
  payloads are forbidden in metadata.
- The engine append boundary accepts the idempotency context. Implementations
  must either commit the operation once or discover and return the previously
  committed version.
- Lance persists tenant-scoped operation identity and payload digest in
  transaction properties. Append commits disable automatic rebase. After a
  commit conflict or failover, Lance examines transaction history: a matching
  operation and digest converges on its committed version; a matching operation
  with a different digest conflicts; no match may be retried by the caller
  without changing the logical operation identity.
- Object-reference data is staged durably before the Lance manifest becomes
  visible. Lance transaction properties retain enough sidecar identity to
  finish publishing the canonical per-version reference chunks after a crash.
  A replay that discovers an already-committed manifest must repair or verify
  its reference sidecar before Metasrv publishes or returns the version.
- The registry remains the visibility boundary. Operation recovery preserves
  engine-manifest-first, reference-lineage-complete, registry-CAS-second
  ordering.
- Recovery covers failures after reservation, engine commit, reference
  finalization, registry CAS, terminal-state publication, and before the final
  `PutResult`.
- Concurrent same-payload replays execute at most one physical engine append.
  Waiters converge on its terminal result. Leadership changes do not erase or
  duplicate the operation.
- Operation retention is finite and deployment-visible, with a production
  default. Cleanup is leader-only and bounded by metastore pages.
- Pending records are never blindly deleted. An expired pending record is
  first reconciled against engine transaction history and the registry.
- Once an operation ID is older than the retention window, replay fails
  explicitly with an idempotency-expired status even if its terminal record
  has already been collected. Expiry never silently turns an old ID into a new
  append.
- `docs/architecture.md`, `specs/project.spec`, and affected crate cards are
  updated so the metastore invariant permits compact durable coordination
  records while continuing to forbid data-plane payloads.

## Constraints

- Query remains stateless; it stores no durable operation record.
- Object bytes continue to flow only between the SDK and managed object
  storage.
- Backend-specific RocksDB/DynamoDB types remain confined to `lake-meta`.
- Lance-specific transaction APIs remain confined to `lake-engine-lance`.
- `Version` remains opaque outside the engine.
- No registry pointer may reference a Lance version whose object-reference
  lineage is missing or unverified.
- Digest mismatch, corrupt operation state, missing transaction history, or
  unrecoverable reference lineage fail closed.
- Existing tenant authorization is applied before operation lookup so an
  operation ID cannot become a cross-tenant existence oracle.

## Boundaries

### Allowed Changes

- `Cargo.toml`
- `Cargo.lock`
- `crates/lake-common/**`
- `crates/lake-flight/**`
- `crates/lake-meta/**`
- `crates/lake-engine/**`
- `crates/lake-engine-lance/**`
- `crates/lake-metasrv/**`
- `crates/lake-query/**`
- `crates/lake-sdk/**`
- `crates/lake-cli/**`
- `docs/architecture.md`
- `specs/project.spec`
- `specs/issue-29-idempotent-append.spec.md`
- `verification/issue-29-idempotent-append.md`
- `**/.github/**`
- `**/AGENT.md`
- `**/CLAUDE.md`
- `**/docs/guides/**`
- `**/mise.toml`

The recursive root patterns above admit pre-existing shared-checkout workflow
and guide edits reported by the lifecycle tool. This issue does not modify
those files; its own changes remain confined to the issue workspace paths
listed above.

### Forbidden

- `crates/lake-objects/**`
- `crates/lake-catalog/**`
- `goal.md`
- `README.md`
- `docs/design/**`

## Completion Criteria

Scenario: append descriptor carries validated operation identity and digest
  Test:
    Package: lake-common
    Filter: file_append_request_roundtrip
  Given a tenant-neutral FILE append descriptor with an operation ID and
  versioned payload digest
  When it is encoded and decoded
  Then every field round-trips and malformed, missing, or trailing data is
  rejected

Scenario: actual Flight payload digest is verified before commit
  Test:
    Package: lake-metasrv
    Filter: mismatched_flight_payload_digest_is_rejected_before_commit
  Given a descriptor whose declared digest differs from the Arrow messages
  When Metasrv consumes the DoPut stream
  Then it returns InvalidArgument and neither the engine version nor registry
  pointer advances

Scenario: same operation replay returns the original committed version
  Test:
    Package: lake-metasrv
    Filter: same_operation_replay_returns_original_version
  Given one committed append
  When the identical tenant, table, operation ID, and payload are replayed
  Then the original Version is returned and the engine append count remains one

Scenario: operation identity cannot be reused for another payload
  Test:
    Package: lake-metasrv
    Filter: same_operation_with_different_payload_conflicts
  Given one reserved or committed operation identity
  When the same identity is submitted with a different verified payload digest
  Then the request conflicts without consuming a second engine append

Scenario: concurrent replays execute one engine append
  Test:
    Package: lake-metasrv
    Filter: concurrent_replays_execute_one_engine_append
  Given concurrent requests with the same operation identity and payload
  When all requests race through the metadata leader
  Then all successful requests return one Version and the engine appends once

Scenario: every append crash window converges
  Test:
    Package: lake-metasrv
    Filter: append_crash_windows_reconcile_without_duplicates
  Given deterministic failures after reservation, engine commit, registry CAS,
  and terminal-state publication
  When Metasrv is reconstructed over the same MetaStore and the operation is
  replayed
  Then every case returns one committed Version and one registry advancement

Scenario: leader failover preserves in-flight operation identity
  Test:
    Package: lake-metasrv
    Filter: leader_failover_reconciles_inflight_append
  Given a two-node metadata deployment and an append committed by the first
  leader without delivering its result
  When that leader stops and the client retries through the standby
  Then the standby returns the original Version without a second append

Scenario: Lance transaction history converges without automatic rebase
  Test:
    Package: lake-engine-lance
    Filter: lance_transaction_history_converges_idempotent_append
  Given a Lance append carrying operation identity and digest properties
  When its result is lost or its commit races a newer parent version
  Then history identifies the existing operation and replay returns its exact
  committed Version without automatic rebase

Scenario: recovered append preserves object-reference lineage
  Test:
    Package: lake-engine-lance
    Filter: recovered_idempotent_append_restores_reference_lineage
  Given a failure after Lance manifest commit but before reference chunks are
  finalized
  When the same operation is replayed
  Then the existing version is discovered, its reference sidecar is repaired
  idempotently, and every appended DataLocation is retained exactly once

Scenario: Query preserves trusted tenant and operation identity
  Test:
    Package: lake-query
    Filter: query_forwards_authenticated_append_operation_scope
  Given two authenticated tenants using the same operation ID and table name
  When Query forwards their authorized FILE appends
  Then Metasrv receives distinct trusted tenant scopes while each operation ID
  and payload digest remains unchanged

Scenario: SDK retries a lost response without re-uploading or duplicating
  Test:
    Package: lake-sdk
    Filter: sdk_retries_lost_put_result_without_reupload_or_duplicate
  Given an object store that counts uploads and a Flight endpoint that drops the
  first response after commit
  When LakeClient performs one logical insert
  Then the SDK reuses its encoded batch and operation ID, uploads once, returns
  the committed Version, and a SQL read observes one row

Scenario: operation cleanup is bounded and expiry fails closed
  Test:
    Package: lake-metasrv
    Filter: operation_gc_is_bounded_and_expired_replay_fails_closed
  Given pending and terminal operation records across metastore pages
  When leader-only cleanup runs after the configured retention horizon
  Then pending records are reconciled, eligible terminal records are removed in
  bounded pages, recent records remain, and an expired replay cannot append

## Out of Scope

- Cross-table transactions or general SQL write idempotency
- Idempotency for create, drop, maintenance, or object-GC commands
- Exactly-once guarantees after the documented retention window; expired
  operations fail explicitly
- Content-level deduplication of independently generated operation IDs
- Moving object bytes, upload checkpoints, or object credentials into metadata
- Redesigning managed-object inventory or GC beyond preserving append reference
  lineage
- Asynchronous query-result materialization or a durable query scheduler
