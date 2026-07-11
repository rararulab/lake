# Query Admission Control Implementation Plan

**Goal:** Bound resource occupancy per stateless Query replica while preserving
incremental Flight streaming and the metadata shielding invariant.

## Architecture

Add immutable `QueryLimits` plus one process-local `QueryAdmission` semaphore.
GetFlightInfo holds a permit while validating/planning. DoGet acquires another
permit and moves it into a deadline-aware stream wrapper; completion, timeout,
or client drop releases it. Request-size validation occurs before catalog or
DataFusion work. CLI parses finite environment limits once at startup.

## Delivery order

1. RED: saturation plus stream-drop permit release.
2. Implement limits, admission acquisition, and permit-owning Flight stream.
3. RED: execution deadline and oversize SQL/ticket behavior; implement both.
4. RED: CLI environment-value parsing; wire `QueryServerConfig`.
5. Document defaults and gRPC failure semantics.
6. Run spec lifecycle, strict clippy, LocalStack integration, and full gate;
   rebase, re-run, open PR, and merge.
