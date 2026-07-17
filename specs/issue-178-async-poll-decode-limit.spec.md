spec: task
name: "issue-178-async-poll-decode-limit"
inherits: project
tags: [sdk, async-query, flight, resource-bounds]
---

## Intent

Keep the Rust SDK's durable asynchronous-query control plane able to receive
every completed `PollFlightInfo` that its documented endpoint and aggregate
ticket budgets permit. The transport decoder must not reject valid metadata
before the SDK's bounded validation runs.

## Decisions

- Give the async Flight client the existing 9 MiB `FlightInfo` control-plane
  decoding ceiling: 8 MiB aggregate tickets plus 1 MiB protocol overhead.
- Exercise a real Flight service returning 257 individually valid 16 KiB
  tickets, whose serialized completed poll response is larger than Arrow
  Flight's default 4 MiB decoder cap but within Lake's ticket budget.
- Keep the existing 8 MiB aggregate ticket validation unchanged; an
  over-budget response must still be rejected as `InvalidAsyncQueryResult`
  after transport decoding.

## Boundaries

### Allowed Changes

- `crates/lake-sdk/src/lib.rs`
- `specs/issue-178-async-poll-decode-limit.spec.md`

### Forbidden

- Flight wire-format changes
- Query or Metasrv asynchronous execution changes
- Raising ticket, endpoint, or direct result-streaming budgets
- New configuration surface

## Completion Criteria

Scenario: Async poll decodes a completed response above the default gRPC limit
  Test:
    Package: lake-sdk
    Filter: sdk_poll_async_decodes_completed_metadata_above_default_grpc_limit
  Given a real Flight service returns 257 valid 16 KiB tickets in a completed PollFlightInfo
  When the SDK polls a valid durable async-query handle
  Then it decodes the over-4-MiB response and returns all tickets in order

Scenario: Async poll still rejects metadata above the aggregate ticket budget
  Test:
    Package: lake-sdk
    Filter: sdk_poll_async_rejects_completed_metadata_above_ticket_budget
  Given a real Flight service returns individually valid tickets whose aggregate exceeds 8 MiB
  When the SDK polls a valid durable async-query handle
  Then transport decoding succeeds but the SDK returns InvalidAsyncQueryResult

## Out of Scope

- Raising result-ticket or endpoint budgets
- Buffering or downloading result-part data before caller consumption
- Retrying malformed completed PollFlightInfo responses
