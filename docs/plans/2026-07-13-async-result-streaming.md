# Async result streaming implementation plan

## Failure mode

`AsyncQueryWorker::write_part` encodes an entire part into a `Vec` and checks
its size afterward. Async result `DoGet` repeats the allocation, then collects
all decoded batches. These allocations bypass DataFusion's memory pool and
multiply with Query admission concurrency.

## Design

1. Add a small `async_ipc` module containing the two deep seams: bounded IPC
   upload and bounded IPC decode. Arrow remains the format authority; Lake owns
   only the sync/async lifecycle bridge.
2. Encoding runs in `spawn_blocking` with a `Write` adapter that emits fixed
   byte chunks into a bounded channel and rejects the next byte past the part
   ceiling. The object upload drains that channel concurrently. Either side
   failing closes the other and the blocking task is joined.
3. Download uses an async fixed-chunk pump that feeds Arrow's low-level
   `StreamDecoder` in a blocking task. The decoder consumes only bytes actually
   received. A bounded framing validator rejects oversized metadata, invalid
   body lengths, and compressed IPC before Arrow decode so declared
   decompression sizes cannot bypass the byte ceiling. The decoder sends the
   schema once plus batches through a bounded output channel; the Flight encoder
   consumes batches directly without collection.
4. A stream guard owns the pump, decoder, deadline, and Query permit. Every
   terminal path cancels the pump and closes both channels. Because an already
   running `spawn_blocking` task cannot be aborted, it retains a shared admission
   permit until it actually exits; only then can replacement work be admitted.
5. Tests use injected small byte limits and controlled slow readers/stores so
   memory bounds are deterministic without allocating production-sized parts.

## Non-goals

Manifest/job-spec JSON remains bounded and collected. The durable schema,
object layout, tickets, and backend store contract do not change.
