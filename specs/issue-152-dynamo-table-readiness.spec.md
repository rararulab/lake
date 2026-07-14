spec: task
name: "dynamo-table-readiness"
inherits: project
tags: [dynamodb, bootstrap, readiness, production]
---

## Intent

Make the DynamoDB metastore safe during a first cloud deployment and when
several services bootstrap concurrently. `CreateTable` is asynchronous, but
the SDK `table_exists` waiter used by `open_tables` treats `CREATING` as an
unmatched terminal response. A first or concurrent `ensure_table` can
therefore fail while either the legacy or prefix-v2 table is not usable yet.

## Decisions

- Replace the SDK waiter with a bounded, asynchronous `DescribeTable` polling
  policy. Only `TableStatus::Active` is ready.
- Retry the DynamoDB transitional `CREATING` and `UPDATING` states at a
  deliberate cadence. A short `ResourceNotFoundException` propagation after a
  create is also transient.
- Stop after a fixed number of observations; include the table name and last
  status in readiness timeout or unavailable-state errors.
- Use one production polling policy for both the legacy table and its
  `_prefix_v2` companion. `open_tables` remains DescribeTable-only so
  pre-provisioned runtime identities do not require CreateTable.
- Bind the status policy with injected, no-delay observations rather than
  relying on LocalStack's usually-immediate ACTIVE transition. Keep the
  LocalStack roundtrip as an end-to-end smoke test.

## Boundaries

### Allowed Changes
crates/lake-meta/src/dynamo.rs
crates/lake-meta/src/error.rs
crates/lake-meta/tests/dynamo_localstack.rs
specs/issue-152-dynamo-table-readiness.spec.md
verification/issue-152-dynamo-table-readiness.md

### Forbidden
crates/lake-query/**
crates/lake-sdk/**
crates/lake-objects/**
crates/lake-metasrv/**
metadata schema migration or object-store changes
new DynamoDB permissions for pre-provisioned runtime identities

## Completion Criteria

Scenario: bootstrap waits for each transitional table until it is ACTIVE
  Test:
    Package: lake-meta
    Filter: dynamo_table_readiness_retries_transitional_statuses_until_active
    Level: unit
    Test Double: deterministic DescribeTable observations and a zero-delay sleeper
  Given a legacy or prefix-v2 table whose DescribeTable responses are
  CREATING then UPDATING
  When the table becomes ACTIVE before the observation bound
  Then bootstrap succeeds only after the ACTIVE observation and the retry
  cadence is used between transitional observations

Scenario: a table that never becomes ready fails within the observation bound
  Test:
    Package: lake-meta
    Filter: dynamo_table_readiness_times_out_with_last_status
    Level: unit
    Test Double: deterministic DescribeTable observations and a zero-delay sleeper
  Given DescribeTable continually reports CREATING
  When the bounded readiness policy exhausts its observations
  Then it returns a diagnostic timeout containing the table name and CREATING
  rather than waiting forever

Scenario: unavailable table statuses fail diagnostically
  Test:
    Package: lake-meta
    Filter: dynamo_table_readiness_rejects_unavailable_status
    Level: unit
    Test Double: deterministic DescribeTable observations and a zero-delay sleeper
  Given DescribeTable reports a non-transitional, non-ACTIVE state
  When bootstrap opens that table
  Then it returns a diagnostic error containing the table name and status

Scenario: LocalStack bootstrap roundtrip remains wired
  Test:
    Package: lake-meta
    Filter: dynamo_bootstrap_readiness_localstack_is_wired
    Level: unit
    Test Double: source-level wiring check for the ignored LocalStack roundtrip
  Given a LocalStack DynamoDB endpoint
  When `ensure_table` creates the legacy and prefix-v2 tables
  Then the ignored LocalStack roundtrip still calls `ensure_table` and opens
  the pre-provisioned layouts before exercising metadata reads and writes

## Out of Scope

- DynamoDB key-schema migration or validation of a pre-existing table schema.
- Object data paths, SQL, Flight auth, or metadata protocol changes.
- General retry policy for DynamoDB data-plane requests.
