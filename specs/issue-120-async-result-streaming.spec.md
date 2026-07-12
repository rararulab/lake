spec: task
name: "async-result-streaming"
inherits: project
tags: [query, async, result, memory, streaming, flight]
---

## Intent

Make the durable async result data plane genuinely bounded-memory. The current
worker encodes a complete Arrow IPC part into `Vec<u8>` before checking the
64 MiB limit. `DoGet` then allocates the declared part size, reads the complete
object, and collects every decoded batch before returning a stream. One wide
row or many concurrent downloads can therefore allocate gigabytes outside the
shared DataFusion pool and OOM a Query replica.

## Decisions

- Bridge Arrow's synchronous IPC writer to async object upload through a
  fixed-size, fixed-capacity byte channel and a lifecycle-owned blocking task.
- Enforce the encoded-byte limit inside the writer before accepting bytes;
  object stores must observe an input error and never publish an oversized
  immutable part.
- Feed verified async object chunks through a second fixed-size channel into
  Arrow's official incremental `StreamDecoder`. Decode only bytes actually
  received. Before Arrow decode, cap message metadata and declared body length
  and reject compressed IPC so untrusted decompression lengths cannot bypass
  the encoded-byte ceiling. Then send batches through a bounded output channel
  and return Flight data before object EOF.
- The returned Flight stream owns its reader pump and decoder tasks. Deadline,
  client drop, decode error, or cancellation closes channels, stops storage
  reads, and releases Query admission only after the blocking decoder actually
  exits.
- Keep the existing Arrow IPC part format, manifest/state schema, tickets,
  object layout, and 64 MiB external part limit.

## Boundaries

### Allowed Changes
Cargo.toml
Cargo.lock
crates/lake-query/**
README.md
docs/architecture.md
docs/design/sql-api-over-s3.md
docs/plans/2026-07-13-async-result-streaming.md
specs/issue-120-async-result-streaming.spec.md
verification/issue-120-async-result-streaming.md

### Forbidden
unbounded byte or batch channels vectors read_to_end or batch collection on async result parts
changing async state record manifest ticket or object key formats
changing ManagedObjectStore interfaces or storage backend implementations
publishing partial oversized failed or cancelled result parts
detached async reader pump or IPC encoder decoder tasks
tenant query SQL object URI or credentials in metrics logs or public errors
claiming per-tenant byte accounting or cluster-global memory limits

## Completion Criteria

Scenario: IPC part encoding is bounded before publication
  Test:
    Package: lake-query
    Filter: async_part_encoder_rejects_encoded_overflow_without_publication
  Given a batch whose encoded IPC bytes exceed an injected small part limit
  When the async worker streams it to a controlled object store
  Then the writer fails at the byte ceiling and the store publishes no immutable object

Scenario: IPC upload uses a fixed live byte window
  Test:
    Package: lake-query
    Filter: async_part_encoder_backpressure_bounds_live_bytes
  Given a slow object consumer and a multi-chunk encoded batch
  When the blocking IPC writer outruns upload
  Then it blocks at the fixed channel capacity and observed queued bytes never exceed the configured window

Scenario: Async DoGet returns before complete object read
  Test:
    Package: lake-query
    Filter: async_result_decoder_streams_before_object_eof
  Given a valid IPC object whose reader pauses after its first record batch
  When DoGet starts decoding the part
  Then schema and the first Flight batch are returned while the object reader is still paused

Scenario: Download pipeline buffers only fixed chunks and batches
  Test:
    Package: lake-query
    Filter: async_result_decoder_backpressure_bounds_live_data
  Given a slow Flight consumer and an object containing many batches
  When the async pump and blocking decoder run concurrently
  Then queued input bytes and decoded batches remain within their fixed capacities

Scenario: Dropping a result stream stops owned work and releases admission
  Test:
    Package: lake-query
    Filter: async_result_stream_drop_cancels_pipeline_and_releases_permit
  Given an async DoGet stream blocked on object input while holding Query admission
  When the client drops the stream
  Then the reader pump and decoder terminate and a new request acquires the released permit

Scenario: Invalid IPC terminates without leaking tasks or identity
  Test:
    Package: lake-query
    Filter: async_result_invalid_ipc_fails_bounded_and_redacted
  Given a verified-size object containing malformed IPC bytes
  When the decoder reads it
  Then Flight returns a stable internal error all owned tasks terminate and no identity is exposed

## Out of Scope

- Streaming or changing the bounded manifest and encrypted job specification.
- Per-tenant scanned/result byte, memory, spill, or egress accounting.
- Presigned HTTPS result endpoints or a different result format.
- Storage backend or multipart pipeline changes.
