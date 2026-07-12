spec: task
name: "async-query-results"
inherits: project
tags: [query, flight, poll-flight-info, async, object-storage, leases, cancellation]
---

## Intent

Deliver multi-GB SQL results without holding one gRPC stream or one Query
replica for the whole client lifetime. Implement the standard Flight
`PollFlightInfo` lifecycle with durable external coordination, bounded result
parts, resumable worker ownership, and stateless polling.

## Decisions

- Use a dedicated injected `AsyncQueryStore`; it may reuse the MetaStore CAS
  backend implementation but never the catalog authority/table or its hot
  path. State records are compact, versioned, tenant-bound, and size-limited.
- Put the encrypted pinned job specification and result manifest in a
  service-owned `AsyncResultStore`. Coordination records contain only bounded
  identifiers, hashes, progress counters, fencing lease data, and object
  identities—not raw SQL, credentials, Arrow batches, or URLs.
- Initial `PollFlightInfo` parses/authorizes SQL and pins the exact snapshots.
  Follow-up descriptors carry an encrypted principal/tenant-bound poll handle;
  any replica can read the external state and attempt an expired lease.
- Workers stream DataFusion output into bounded Arrow IPC parts. Completion is
  one CAS transition referencing an immutable manifest after all part uploads;
  partial output is never advertised as complete.
- Use Flight's standard `CancelFlightInfo` action. Cancellation/expiry fences
  workers, aborts unfinished uploads, and is idempotent across retries.
- Completed parts become short-lived identity-bound `FlightEndpoint` tickets.
  `DoGet` streams the exact immutable local or S3 result part from any Query
  replica; raw object URLs and storage credentials never enter Flight.

## Boundaries

### Allowed Changes
Cargo.toml
Cargo.lock
crates/lake-common/**
crates/lake-meta/**
crates/lake-objects/**
crates/lake-query/**
crates/lake-sdk/**
crates/lake-cli/**
deploy/kubernetes/lake.yaml
README.md
docs/architecture.md
docs/design/sql-api-over-s3.md
docs/guides/kubernetes.md
docs/plans/2026-07-12-async-query-results.md
specs/issue-110-async-query-results.spec.md
verification/issue-110-async-query-results.md

### Forbidden
Query-local durable job or poll state
query state in the table catalog authority hot path
raw SQL, credentials, presigned URLs, or Arrow batches in coordination records
unbounded records, result parts, part bytes, polls, leases, retries, or cleanup
publishing Flight endpoints before an immutable complete manifest
latest-version fallback instead of ticket-pinned table snapshots
blind state writes or unfenced worker completion
bespoke HTTP SQL submission

## Completion Criteria

Scenario: Async state transitions are CAS fenced and bounded
  Test:
    Package: lake-query
    Filter: async_query_state_machine_fences_workers_and_terminal_states
  Given concurrent replicas and one compact queued job
  When they claim renew complete fail cancel or expire it
  Then only the current lease epoch may advance legal bounded transitions and terminal states are immutable

Scenario: Poll submission is identity bound and snapshot exact
  Test:
    Package: lake-query
    Filter: poll_flight_info_submits_identity_bound_pinned_job
  Given authenticated SQL with physical lake tables
  When the first PollFlightInfo request is submitted
  Then the external job specification contains encrypted exact snapshots and the returned descriptor works on another replica only for that identity

Scenario: Polling does not hit the catalog authority
  Test:
    Package: lake-query
    Filter: poll_flight_info_submits_identity_bound_pinned_job
  Given an already submitted query and a failing table metastore
  When another replica polls queued running and completed states
  Then progress remains available without catalog metadata calls

Scenario: Result materialization is bounded and atomic
  Test:
    Package: lake-query
    Filter: async_result_manifest_publishes_only_after_bounded_parts
  Given a delayed multi-batch DataFusion stream
  When a worker materializes it
  Then each Arrow part obeys byte and row bounds and completion appears only after every immutable part and manifest succeed

Scenario: Worker crash is taken over without duplicate publication
  Test:
    Package: lake-query
    Filter: async_query_state_machine_fences_workers_and_terminal_states
  Given a worker dies after uploading a part but before completion
  When another replica claims the expired lease
  Then fencing prevents stale completion and recovery converges on one manifest without exposing drafts

Scenario: CancelFlightInfo stops and cleans partial work
  Test:
    Package: lake-query
    Filter: cancel_flight_info_fences_execution_and_reaps_partial_results
  Given a running async query with an unfinished upload
  When its owner sends the standard CancelFlightInfo action repeatedly
  Then cancellation is idempotent the stream stops and partial objects are never published

Scenario: Completed endpoints are scoped and short lived
  Test:
    Package: lake-query
    Filter: poll_flight_info_submits_identity_bound_pinned_job
  Given a completed manifest in local and S3 result stores
  When the owner polls completion
  Then identity-bound endpoints reference only exact manifest parts and expire with no object URL or credential disclosure

Scenario: SDK polls downloads and decodes standard Arrow results
  Test:
    Package: lake-sdk
    Filter: sdk_async_query_roundtrip_uses_poll_flight_info
  Given a result larger than one configured part
  When the Rust SDK submits polls and consumes it
  Then it returns the same ordered Arrow batches through standard Flight semantics without keeping the submission stream open

## Out of Scope

- Cross-query scheduling fairness beyond configured per-tenant and per-replica
  admission limits.
- Cross-region replication of result objects or the async state backend.
- Parquet result format; the first production format is Arrow IPC stream.
- A bespoke REST SQL API.
