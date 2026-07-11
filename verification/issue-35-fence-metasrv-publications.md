# Issue #35 Verification

Candidate: issue #35 change range from `aa4fe7787dbb` through the current
workspace head.

## Contract

- Explicit jj candidate `agent-spec lifecycle`: **6/6 PASS**.
- Spec lint quality: **100%**.
- All 13 changed paths are inside the declared boundaries.

## Fencing behavior

- Election returns the exact bytes installed at `election/leader` together
  with the monotonic epoch.
- Leadership replaces the guard on renewal and exposes it only inside the
  conservative local monotonic deadline. A process-local publication barrier
  prevents a target transaction from observing the interval between durable
  exact-byte rotation and local guard publication.
- `FencedMetaStore` loads a fresh guard for every target CAS/delete and maps it
  to the native atomic guarded mutation.
- Deterministic takeover rejects a paused former leader without changing its
  target.
- A deterministic interleaving pauses same-holder renewal after the durable
  CAS but before local publication. The concurrent target publication waits,
  then succeeds with the renewed exact bytes at the same epoch.
- An injected takeover after engine commit but before metadata publication is
  recovered by the successor at version 2, proving no duplicate append.
- Recording raw-store coverage executes real registry create/drop, append
  record/fence/terminal cleanup, maintenance version publication, and
  operation GC. It observes zero ordinary target CAS/delete, with every phase
  increasing the guarded-mutation count.
- Remote destructive drop returns `FailedPrecondition` before registry or
  engine mutation until durable tombstones exist; the original dataset remains
  openable after rejection.

## Verification commands

- `cargo test -p lake-meta --no-fail-fast`: **13 PASS**.
- `cargo test -p lake-metasrv --lib --tests --no-fail-fast`: **44 PASS**
  (41 unit plus 3 live two-node tests).
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
