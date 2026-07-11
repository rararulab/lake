# gRPC health readiness implementation plan

**Goal:** Expose scheduler-usable liveness and real traffic readiness without
bypassing the existing Flight security boundary.

**Architecture:** Use the standard gRPC health service on each tier's existing
Tonic server. Query readiness follows strict catalog warmup. Metasrv readiness
is derived from the same local lease/remote-forward route used by writes and is
woken by leadership state changes plus the exact local deadline.

## Task 1: Add failing protocol tests

1. Add secured Query health Check/Watch tests for auth, serving after warmup,
   and NOT_SERVING before shutdown drain.
2. Add Metasrv route-state tests covering no leader, remote leader, live local
   lease, and expired remembered-self state.
3. Add Metasrv Watch/shutdown ownership coverage.
4. Confirm RED because neither server registers the health protocol.

## Task 2: Wire Query health

1. Register `tonic-health` behind the existing server interceptor.
2. Mark overall and Flight services serving after strict refresh.
3. Withdraw both statuses before server cancellation.

## Task 3: Wire Metasrv route-aware health

1. Add leadership change subscription and exact local-expiry inspection.
2. Run an owned readiness monitor that publishes Flight status.
3. Withdraw health before drain and join the monitor with maintenance and
   campaign under the total deadline.

## Task 4: Document and verify

1. Document service names, auth/TLS requirements, readiness semantics, and
   shutdown ordering.
2. Run spec lifecycle, strict clippy, full gate, independent verification, and
   independent review.
3. Ship one PR and merge after approval.
