# Cancel-safe S3 uploads implementation plan

## Goal

Ensure cancelling an ordinary, non-resumable large-object upload converges to
one bounded `AbortMultipartUpload` attempt instead of leaving an orphan.

## Architecture

After multipart creation, start one metadata-only cleanup owner containing the
S3 client and upload identity. A one-shot decision channel disarms it after
successful completion or requests an awaited abort on explicit failure. If the
caller future is dropped, channel closure selects the same abort path. The
owner task lives with the active upload and caps its terminal abort attempt at
30 seconds; it never owns object buffers. Resumable uploads retain their
checkpoint-owned lifecycle.

## Tasks

1. Add a real LocalStack regression that cancels a blocked ordinary upload and
   observes `ListMultipartUploads` converge to empty.
2. Add unit coverage for drop, disarm, and explicit-abort owner transitions.
3. Route ordinary error/completion/cancellation through the cleanup owner while
   preserving part cancellation before abort.
4. Document the process-crash boundary and mandatory S3 incomplete-multipart
   lifecycle rule.
5. Run lane-1 lifecycle, full LocalStack, strict Clippy, rustdoc, full gate, and
   independent review.
