spec: task
name: "issue-174-async-ticket-budget"
inherits: project
tags: [sdk, async-query, resource-bounds]
---

## Intent

Keep the Rust SDK's durable asynchronous-query control plane bounded when it
decodes a completed `PollFlightInfo`. Every result part remains streamed only
after the metadata response has passed finite endpoint and ticket-size limits.

## Decisions

- Apply the existing 8 MiB aggregate Flight-ticket budget to asynchronous
  result tickets, in addition to the existing 4,096-endpoint and 16 KiB
  per-ticket bounds.
- Reject an over-budget response as `SdkError::InvalidAsyncQueryResult` before
  any `DoGet` can be started.
- Preserve the existing ordered ticket sequence for a response within all
  bounds.

## Boundaries

### Allowed Changes

- `crates/lake-sdk/src/lib.rs`
- `specs/issue-174-async-ticket-budget.spec.md`

### Forbidden

- Flight protocol or ticket wire-format changes
- Query or Metasrv asynchronous execution changes
- Result-part streaming or object-store code
- New configuration surface

## Completion Criteria

Scenario: Over-budget asynchronous result metadata is rejected
  Test:
    Package: lake-sdk
    Filter: async_result_tickets_rejects_oversized_aggregate
  Given a completed asynchronous FlightInfo with individually valid tickets
  When their aggregate bytes exceed 8 MiB
  Then the SDK rejects it as InvalidAsyncQueryResult before any DoGet request

Scenario: Bounded asynchronous result metadata preserves ordered tickets
  Test:
    Package: lake-sdk
    Filter: async_result_tickets_preserves_order_within_aggregate_budget
  Given a completed asynchronous FlightInfo within every ticket bound
  When the SDK decodes its tickets
  Then it accepts them in endpoint order

## Out of Scope

- Changing the maximum number of asynchronous result parts
- Buffering or downloading result-part data before the caller consumes it
- Retrying malformed FlightInfo responses
