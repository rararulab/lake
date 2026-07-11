# Stream Lance removal results

Issue: #55

## Goal

Keep `LanceEngine::remove` memory for completed object deletions constant while
preserving fail-fast errors and the existing data-before-manifest cleanup order.

## Implementation

1. Add focused tests using drop-tracked stream items for peak retention and a
   poll counter for fail-fast behavior.
2. Introduce a private generic drain helper that consumes each successful item
   immediately and returns the first stream error.
3. Route `object_store::delete_stream` through the helper instead of collecting
   a vector.
4. Run the task contract, crate tests, strict clippy, and the repository gate;
   obtain independent correctness review and release verification before merge.
