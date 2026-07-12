# Bounded S3 multipart pipeline implementation plan

## Goal

Overlap a finite number of S3 UploadPart requests for multi-gigabyte objects
without changing Lake's constant-memory, immutable-publication, SHA-256, abort,
or resumable-checkpoint contracts.

## Architecture

Extract an internal ordered async pipeline that reads and hashes each 5 MiB
part sequentially, transfers owned buffers into at most N upload futures, and
consumes results in part-number order. Ordinary uploads collect compact
CompletedPart metadata. Resumable uploads publish each ordered result into an
atomic contiguous-prefix checkpoint. Persisted creator concurrency bounds the
remote suffix that a crash can leave ahead of the checkpoint; resume overwrites
that suffix from the verified source rather than trusting it.

## Tasks

1. Add lane-1 RED tests for overlap, peak live request bytes, response order,
   source hash, failure cancellation, configuration, and checkpoint order.
2. Add validated S3 upload concurrency with default four and maximum sixteen.
3. Implement the common bounded ordered pipeline and route ordinary uploads
   through it.
4. Add backward-compatible checkpoint concurrency (`missing = 1`), bounded
   remote suffix reconciliation, and resumable pipeline publication.
5. Extend real LocalStack tests for exact round-trip, interruption/abort, and
   ambiguous suffix overwrite.
6. Document the resource formula and run lifecycle, Clippy, rustdoc, full gate,
   and independent correctness/performance review.
