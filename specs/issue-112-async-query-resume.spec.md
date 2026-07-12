spec: task
name: "async-query-resume"
inherits: project
tags: [sdk, query, flight, poll-flight-info, idempotency, resume]
---

## Intent

Make durable async queries restart-safe from the caller's perspective. A
client can persist an opaque versioned handle, reconnect through any Query
replica, resume polling, cancel, or consume completed endpoints. Retrying a
lost initial response converges on the same job instead of executing twice.

## Decisions

- The SDK generates one random 128-bit submission id and carries it in the
  standard `CommandStatementQuery.transaction_id` field because Lake does not
  expose SQL transactions. Query treats it solely as an async idempotency key.
- The durable job id is derived from the authenticated tenant, principal, and
  submission id. Reuse by another identity is impossible; reuse with a
  different canonical pinned statement fails closed.
- The public SDK handle contains only a version, opaque encrypted poll
  descriptor, and advertised capability expiry. It is size bounded, redacted
  in Debug, and JSON round-trippable for caller-owned persistence.
- `query_async` remains the convenience API over explicit `submit_async`,
  `resume_async`, `cancel_async`, and result consumption methods.

## Boundaries

### Allowed Changes
Cargo.toml
Cargo.lock
crates/lake-query/**
crates/lake-sdk/**
README.md
docs/architecture.md
docs/design/sql-api-over-s3.md
docs/plans/2026-07-12-async-query-resume.md
specs/issue-112-async-query-resume.spec.md
verification/issue-112-async-query-resume.md

### Forbidden
raw SQL snapshots credentials object URLs or storage locations in SDK handles
catalog-authority reads during resume or retry
unbounded handle bytes retries polling or lifetimes
identity-agnostic idempotency keys
returning an existing job for a different pinned statement
bespoke submission RPCs outside standard Flight

## Completion Criteria

Scenario: Lost initial response converges on one durable job
  Test:
    Package: lake-query
    Filter: async_submission_id_retries_converge_on_one_job
  Given the same authenticated principal statement and submission id
  When initial PollFlightInfo is retried after its response is lost
  Then both responses name one durable job and only one executable state record exists

Scenario: Submission id cannot alias another statement
  Test:
    Package: lake-query
    Filter: async_submission_id_rejects_statement_alias
  Given an existing durable submission id
  When its owner reuses it with different SQL or pinned snapshots
  Then Query fails closed without replacing or executing the original job

Scenario: SDK handle is bounded versioned and redacted
  Test:
    Package: lake-sdk
    Filter: async_query_handle_roundtrips_without_disclosing_payload
  Given a submitted async query
  When its public handle is serialized logged and restored
  Then only bounded opaque capability bytes version and expiry are observable

Scenario: SDK restart resumes on another Query replica
  Test:
    Package: lake-sdk
    Filter: sdk_resumes_async_query_after_client_restart
  Given a persisted async handle and a disconnected original client
  When a fresh client resumes through another Query replica
  Then it polls dedicated state and consumes the original ordered Arrow result

Scenario: Resumed owner can cancel idempotently
  Test:
    Package: lake-sdk
    Filter: sdk_cancels_resumed_async_query_idempotently
  Given a restored handle for queued or running work
  When its owner sends CancelFlightInfo repeatedly
  Then status converges without a catalog lookup and execution cannot publish completion

Scenario: Convenience query_async uses explicit durable lifecycle
  Test:
    Package: lake-sdk
    Filter: sdk_query_async_delegates_to_restart_safe_handle
  Given the existing query_async API
  When it completes a query
  Then it uses one explicit submission handle and the same resume/result path

## Out of Scope

- Persisting SDK handles on behalf of callers; applications choose their own
  database, file, or workflow checkpoint.
- Cross-user transfer of query ownership.
- SQL transaction support.
