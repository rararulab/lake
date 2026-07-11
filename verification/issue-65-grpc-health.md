# Verification: issue #65 gRPC health readiness

- verdict: **PASS**
- score_authority: `verifier`
- base_sha: `06fc69502b0c47d89abaabe12fcd0d85e5b26974`
- head_sha: `6860eb9b698adb613d58ffa04a7e68d4f9fc78af`
- implementer_evidence: not consulted; all commands below were rerun independently from a clean workspace

## Boundary and revision evidence

- `git merge-base 6860eb9b 06fc6950` returned the exact base SHA.
- `jj st` was clean before verification; `@-` was the fixed candidate `6860eb9b` with conventional commit `feat(servers): expose authenticated gRPC health readiness (#65)`.
- `jj diff --summary -r '06fc6950..6860eb9b'` reported 12 paths. Every path is permitted by the spec allowlist: root Cargo files, `crates/lake-query/**`, `crates/lake-metasrv/**`, the three allowed docs, the implementation plan, and the issue spec. No forbidden crate, listener, or wire-format path changed.

## Selector transition matrix

| Completion selector | Base | Head | Independent focused run |
|---|---:|---:|---|
| `query_grpc_health_requires_auth_and_reports_serving` | 0 | 1 | PASS: 1 passed |
| `query_health_watch_observes_not_serving_on_shutdown` | 0 | 1 | PASS: 1 passed |
| `metasrv_health_tracks_leader_route_and_lease_expiry` | 0 | 1 | PASS: 1 passed |
| `metasrv_health_watch_withdraws_and_monitor_joins` | 0 | 1 | PASS: 1 passed |

`mise run spec-lifecycle specs/issue-65-grpc-health.spec.md` passed under the guarded runner. It reported all four scenarios PASS and confirmed every selector executed at least one test.

## Commands and raw result summary

- `mise run doctor`: PASS (`cargo check`, jj repository, GitHub auth, and origin all healthy).
- Four independent `cargo test -p <package> <selector> -- --nocapture` invocations: PASS, exactly one matching unit test each.
- `cargo test -p lake-query`: PASS (33 unit + 1 integration; 0 failures).
- `cargo test -p lake-metasrv`: PASS (60 unit passed, 1 explicit LocalStack-only ignored; 5 integration passed; 0 failures).
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: PASS.
- Cold-state probe: removed `data/`, confirmed `jj st` clean, then ran `mise run gate`: PASS in 29.47s. Workspace/all-target tests, `lake selftest`, and site typecheck/tests/build all passed. The macOS linker emitted its existing `__eh_frame` size warning during test linking; strict clippy remained warning-free.

## Behavioral probes

- Query performs the strict initial catalog refresh before health/server readiness, exposes the empty liveness service and Flight readiness on the existing Tonic port, and applies the same TLS/bearer interceptor to Flight and health.
- Query publishes Flight and liveness `NOT_SERVING` before cancelling the server during graceful shutdown; its Watch selector observed withdrawal.
- Metasrv starts Flight readiness as `NOT_SERVING`; readiness becomes serving only for an unexpired local lease or a different remote leader route. The exact local lease deadline wakes the monitor and withdraws readiness after expiry while liveness remains serving.
- The fixed shutdown race is closed by a shared publication mutex plus a cancellation recheck under that mutex. Shutdown cancels the monitor before acquiring the same mutex and publishing final `NOT_SERVING`; the regression test additionally asserts no readiness rebound during the drain window.
- Health, campaign, and maintenance tasks remain owned and are joined or aborted/reaped within the existing total shutdown deadline.

## Verdict

**PASS.** Candidate `6860eb9b` satisfies all four completion scenarios, remains inside the declared boundary, closes the reviewed readiness-rebound race, and passes focused, package, strict lint, guarded lifecycle, and full cold-state gate verification.
