# Issue #35 Verification

Candidate: `89c1a4e949bf` (`feat(metasrv): fence metadata publications (#35)`)

## Contract

- Explicit jj candidate `agent-spec lifecycle`: **6/6 PASS**.
- Spec lint quality: **100%**.
- All 11 changed paths are inside the declared boundaries.

## Fencing behavior

- Election returns the exact bytes installed at `election/leader` together
  with the monotonic epoch.
- Leadership replaces the guard on renewal and exposes it only inside the
  conservative local monotonic deadline.
- `FencedMetaStore` loads a fresh guard for every target CAS/delete and maps it
  to the native atomic guarded mutation.
- Deterministic takeover rejects a paused former leader without changing its
  target.
- Same-holder renewal replaces the exact bytes without changing the epoch; a
  later publication succeeds with the renewed guard.
- An injected takeover after engine commit but before metadata publication is
  recovered by the successor at version 2, proving no duplicate append.
- Recording raw-store coverage observes zero ordinary target CAS/delete and
  two guarded mutations through the production server view.
- Remote destructive drop returns `FailedPrecondition` before registry or
  engine mutation until durable tombstones exist.

## Verification commands

- `cargo test -p lake-meta --no-fail-fast`: **13 PASS**.
- `cargo test -p lake-metasrv --lib --tests --no-fail-fast`: **43 PASS**
  (40 unit plus 3 live two-node tests).
- `cargo clippy -p lake-cli -p lake-meta -p lake-metasrv --all-targets -- -D warnings`:
  **PASS**.
- LocalStack ignored Dynamo test: **1 PASS**.
- Fresh `rm -rf data && mise run gate`: **PASS** in 25.03s; workspace tests,
  e2e selftest, hooks, and site checks passed.
- `mise run doc` with `RUSTDOCFLAGS=-D warnings`: **PASS**.

## Remaining safety boundary

Remote table drop is intentionally unavailable. Re-enabling it requires a
durable tombstone committed under the lease guard before object deletion, plus
restartable cleanup and drop/recreate incarnation tests.
