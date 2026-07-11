# Graceful Flight Shutdown Implementation Plan

**Goal:** Give every Flight process an owned, bounded, testable shutdown
lifecycle suitable for rolling deployment and metadata HA handoff.

## Architecture

Use one `CancellationToken` per serve invocation. The injected shutdown future
triggers tonic graceful drain plus cancellation of server-owned background
tasks. A finite grace timeout bounds connection drain. Query joins its catalog
refresher. Metasrv joins maintenance and campaign; campaign best-effort resigns
the durable lease and clears local authority before returning. CLI alone owns
OS signal handling.

## Delivery order

1. RED: Query listener/refresher shutdown and join.
2. Implement shutdown-aware Query serve and cancellation-aware refresher.
3. RED: in-flight stream clean drain and stuck-stream timeout; implement the
   bounded tonic drain lifecycle.
4. RED: campaign cancellation and lease resignation; implement and join
   Metasrv background loops.
5. RED: Metasrv listener lifecycle; add shutdown-aware serve entry point.
6. Wire CLI SIGINT/SIGTERM, document grace defaults and failure semantics.
7. Run spec lifecycle, strict clippy, LocalStack integration, and full gate;
   rebase, re-run, open PR, and merge.
