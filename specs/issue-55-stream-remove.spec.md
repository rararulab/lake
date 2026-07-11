spec: task
name: "stream-remove-results"
inherits: project
tags: [lance, object-store, removal, streaming, memory]
---

## Intent

Deleting a large Lance table must not retain one successful object-deletion
result per object until the whole prefix has been removed. The object-store API
already returns a stream, but `LanceEngine::remove` currently collects that
stream into a `Vec`, making metadata-process memory grow with the dataset object
count during drop.

## Decisions

- Drain successful deletion results one at a time and discard each immediately.
- Preserve fail-fast behavior: return the first deletion error and stop polling
  later stream items.
- Delete external manifest history only after every data-object deletion has
  succeeded.
- Verify retention with drop-tracked synthetic results rather than relying on a
  noisy process-memory measurement.
- Keep the engine trait, object-store concurrency, durable metadata, and wire
  contracts unchanged.

## Boundaries

### Allowed Changes
crates/lake-engine-lance/**
docs/plans/2026-07-12-stream-remove-results.md
specs/issue-55-stream-remove.spec.md
verification/issue-55-stream-remove.md

### Forbidden
crates/lake-engine/**
crates/lake-meta/**
crates/lake-metasrv/**
crates/lake-query/**
crates/lake-sdk/**
durable metadata formats
public wire protocols

## Completion Criteria

Scenario: Successful removal results have constant retention
  Test:
    Package: lake-engine-lance
    Filter: remove_result_stream_keeps_constant_live_items
  Given a large synthetic stream of successful object-deletion results
  When the Lance removal drain consumes the stream
  Then each result is dropped before the next is retained and peak live results remain one

Scenario: Removal stops at the first object deletion error
  Test:
    Package: lake-engine-lance
    Filter: remove_result_stream_stops_after_first_error
  Given successful deletion results followed by an error and additional items
  When the Lance removal drain consumes the stream
  Then it returns that error without polling any item after the failure

## Out of Scope

- Changing delete request concurrency or batching.
- Retrying failed object deletions.
- Redesigning table-drop procedures or manifest storage.
- Changing how object paths are listed by the object-store implementation.
