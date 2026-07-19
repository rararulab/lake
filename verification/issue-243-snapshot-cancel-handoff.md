# Issue #243 verification

## Delivered contract

- An Iceberg resolver already waiting on an exact-table snapshot load becomes
  the replacement loader when the original leader is cancelled.
- The handoff is entirely inside the existing caller: it does not need a later
  request to repair the cache and issues only one replacement external load.

## Regression evidence

- The existing gated in-memory Iceberg catalog holds the leader at its first
  `load_table` call. A second resolver starts and joins the cache's in-flight
  load before the leader task is aborted.
- Red: temporarily removing the leader drop cleanup leaves the follower on the
  closed-but-still-registered sender and it exceeds the one-second bounded
  wait. Restoring cleanup lets that follower replace the leader and return the
  snapshot after exactly two table loads.
- The exact regression passed ten consecutive runs.

## Verification

- `cargo +nightly fmt --all --check` — PASS.
- `cargo test -p lake-iceberg` — PASS (15 catalog integration tests and 3
  configuration tests).
- `cargo clippy -p lake-iceberg --all-targets --all-features -- -D warnings` —
  PASS.
- `mise run spec-lint specs/issue-243-snapshot-cancel-handoff.spec.md` — PASS
  (100%).
- `mise run spec-lifecycle specs/issue-243-snapshot-cancel-handoff.spec.md` —
  PASS.
- `mise run ship` — PASS (full local CI, Conventional Commit validation, and
  Jujutsu push gate).
