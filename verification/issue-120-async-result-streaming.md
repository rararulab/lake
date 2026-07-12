# Issue #120 verification

Candidate base: `3d1ff33088cc797e1ed57ce0e0d2a6aa98e00b25`

## Delivered contract

- Async result encoding writes fixed 64 KiB chunks through a four-slot bounded
  channel and rejects the next byte beyond the per-part or remaining
  per-query ceiling. A failed encoder becomes an object-reader error, so the
  store cannot publish an oversized immutable part.
- Async result redemption reads verified object bytes in fixed chunks and
  feeds Arrow's incremental `StreamDecoder` without a whole-part `Vec` or
  batch collection. A second two-slot channel backpressures decoded batches,
  and the first batch can reach Flight before object EOF.
- Clean stream completion still requires the object reader to reach verified
  EOF. Size, integrity, read, decode, and task failures become stable redacted
  errors.
- A bounded framing validator rejects IPC metadata over 1 MiB, declared bodies
  over the part limit, and RecordBatch/DictionaryBatch compression before Arrow
  decoding. Compressed buffer prefixes therefore cannot declare an unbounded
  decompression allocation.
- One guard owns the reader pump and decoder. The Flight stream owns that guard
  together with Query admission, so completion, error, deadline, cancellation,
  and client drop close the pipeline. The blocking decoder retains a shared
  permit until it really exits, even though Tokio cannot abort an already
  running `spawn_blocking` task.
- The Arrow IPC format, result manifest, tickets, object layout, and 64 MiB
  external part limit remain unchanged.

## Red/green evidence

- The first encoder tests failed to compile because the bounded pipeline
  limits, instrumentation, and streaming reader did not exist.
- The first decoder tests failed to compile because incremental decode and its
  lifecycle-owned guard did not exist.
- The overflow test sends a deliberately wide batch through the real local
  object store and observes an error plus zero regular files after the failed
  write.
- Slow-consumer tests inject tiny chunks and channel capacities, then measure
  peak queued bytes and batches against the exact configured windows.
- A duplex reader pauses after its first batch; the decoder returns that batch
  before the writer is released to send object EOF.
- The Flight-level drop test holds both decode work and a one-slot tenant
  admission permit, drops the returned stream, observes both owned tasks stop,
  and reacquires the same tenant permit.
- Malformed IPC produces only `IPC decoding failed`, with no tenant, query,
  URI, or capability material. A ZSTD-compressed IPC regression first decoded
  successfully, then passed only after pre-decode compression rejection was
  added.
- Review found that `JoinHandle::abort` cannot stop an already running blocking
  decoder. The corrected Flight test holds the decoder at its exit boundary,
  proves replacement admission cannot succeed after stream drop, then releases
  the decoder and proves the permit becomes available.

## Verification

- `mise run doctor` — PASS in the new jj workspace.
- Spec lint — PASS, quality 100%.
- `cargo +nightly fmt --all -- --check` — PASS.
- `git diff --check` — PASS.
- `cargo check -p lake-query --tests` — PASS.
- `cargo clippy -p lake-query --all-targets -- -D warnings` — PASS.
- All six focused completion selectors — PASS.
- Existing real `PollFlightInfo` submission/redemption and atomic manifest
  regression selectors — PASS.
- `mise run spec-lifecycle specs/issue-120-async-result-streaming.spec.md` —
  PASS, all 6/6 scenarios and selectors executed.
- Independent correctness/security review — APPROVE after the compressed-IPC
  allocation and blocking-decoder admission findings were corrected.
- `mise run gate` — PASS in 216.00 seconds, including all workspace targets,
  Query 75/75 unit tests, SDK 44/44 unit tests (two explicit LocalStack tests
  ignored outside their integration runner), upstream ADBC 3/3, site checks,
  and the `ingest -> commit -> SQL` e2e.
