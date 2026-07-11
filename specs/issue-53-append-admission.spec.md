spec: task
name: "append-admission"
inherits: project
tags: [metasrv, flight, append, admission, memory, concurrency]
---

## Intent

The bounded metadata authority must not let concurrent FILE control streams
multiply its memory and engine-commit occupancy without a process-wide limit.
Today one request is capped at 64 MiB, but every authenticated `DoPut` may
buffer that amount before digest verification and may then hold decoded Arrow,
engine transaction, and reference-set state. Reproducer: start one Metasrv and
open many valid append streams concurrently while their commits are paused;
all requests are accepted and process occupancy grows approximately as
requests multiplied by the per-stream limit instead of rejecting at a finite
deployment budget.

## Decisions

- Add validated `AppendLimits` containing maximum concurrent appends, queue
  wait, maximum bytes per Flight control stream, and maximum process-wide
  buffered control bytes.
- Defaults are 8 concurrent requests, 100 ms queue wait, 64 MiB per stream,
  and 256 MiB process-wide buffered bytes.
- Before reading the first Flight message, each authenticated `DoPut` acquires
  one concurrency permit and reserves its full configured per-stream maximum
  from a shared byte semaphore. Worst-case reservation avoids incremental
  weighted-permit hold-and-wait deadlocks and makes the memory ceiling
  independent of message interleaving.
- The process-wide byte maximum must be at least one stream maximum and both
  byte values must fit Tokio's weighted semaphore permit representation.
- The combined permit lives through follower upload forwarding or local
  digest verification, decode, engine commit, and response construction. Drop,
  cancellation, rejection, forwarding failure, or completion releases it.
- Queue saturation returns gRPC `ResourceExhausted` without polling the
  request payload. Per-stream overflow retains the existing
  `ResourceExhausted` behavior and occurs before commit.
- Expose positive-integer deployment overrides:
  `LAKE_APPEND_MAX_CONCURRENT`, `LAKE_APPEND_QUEUE_TIMEOUT_MS`,
  `LAKE_APPEND_MAX_STREAM_BYTES`, and `LAKE_APPEND_MAX_BUFFERED_BYTES`.
- Keep digest validation before `Metasrv::append`; do not stream unverified
  payloads into the engine or change durable operation semantics.

## Boundaries

### Allowed Changes
crates/lake-metasrv/**
crates/lake-cli/**
docs/architecture.md
docs/guides/cli.md
docs/plans/2026-07-12-append-admission.md
specs/issue-53-append-admission.spec.md
verification/issue-53-append-admission.md

### Forbidden
crates/lake-query/**
crates/lake-sdk/**
crates/lake-meta/**
crates/lake-engine*/**
durable metadata formats
FILE append wire descriptors

## Completion Criteria

Scenario: Concurrent append saturation is rejected and released
  Test:
    Package: lake-metasrv
    Filter: append_admission_rejects_concurrency_saturation_and_releases
  Given one append concurrency slot and enough byte budget
  When one request holds the permit and a second waits past the queue timeout
  Then the second is ResourceExhausted and a request after release is admitted

Scenario: Buffered metadata has a process-wide worst-case reservation
  Test:
    Package: lake-metasrv
    Filter: append_admission_reserves_worst_case_buffer_budget
  Given two concurrency slots but byte budget for only one maximum-sized stream
  When one append holds its combined permit
  Then another append is ResourceExhausted until the first permit is dropped

Scenario: Admission covers forwarded upload and leader commit lifetime
  Test:
    Package: lake-metasrv
    Filter: forwarded_append_holds_admission_until_commit_finishes
  Given a follower with one append slot forwarding to a leader whose engine commit is paused
  When a second append reaches the same follower before the first response completes
  Then the second is ResourceExhausted and succeeds only after the first commit releases

Scenario: Per-stream byte limit is server-configurable
  Test:
    Package: lake-metasrv
    Filter: configured_append_stream_limit_rejects_before_commit
  Given a valid append whose encoded control payload exceeds the configured stream maximum
  When Metasrv receives the complete Flight stream
  Then it returns ResourceExhausted without publishing a table version

Scenario: Invalid append limits fail before serving
  Test:
    Package: lake-cli
    Filter: append_limit_values_are_validated_before_serving
  Given zero, malformed, semaphore-overflowing, or buffer-smaller-than-stream values
  When Metasrv server configuration is built
  Then configuration fails before binding the Flight listener

## Out of Scope

- Spooling append control payloads to disk.
- Changing the digest algorithm or verifying it after engine work begins.
- Limiting object bytes, which never traverse Query or Metasrv.
- Per-tenant quotas, distributed quotas, or priority scheduling.
- Changing the per-table serialization or durable append state machine.
