# Issue #240 verification

## Delivered contract

- A cancelled OAuth renewal leader publishes the existing opaque failure result
  to followers of that in-flight renewal.
- A follower therefore completes under its bounded wait and does not create a
  second client-credential request for the same observed token generation.

## Regression evidence

- The test uses the real loopback Axum REST catalog and the connector's real
  Reqwest client. Startup receives a valid token; exact table reads then return
  unauthorized while the first renewal remains pending.
- A second exact-table reader reaches that pending renewal. Cancelling the
  leader makes the follower return `IcebergError::Catalog` within one second.
- Red: temporarily removing the leader's drop publication makes the follower
  exceed its one-second bounded wait. Restoring the publication returns the
  opaque catalog error and the token endpoint observes exactly one renewal.
- The exact regression passed ten consecutive runs.

## Verification

- `cargo +nightly fmt --all --check` — PASS.
- `cargo test -p lake-iceberg` — PASS (14 catalog integration tests and 3
  configuration tests).
- `cargo test -p lake-iceberg --test catalog
  cancelled_oauth_renewal_leader_releases_follower -- --exact` — PASS; ten
  consecutive runs passed.
- `cargo clippy -p lake-iceberg --all-targets --all-features -- -D warnings`
  — PASS.
- `mise run spec-lint specs/issue-240-oauth-cancelled-leader.spec.md` — PASS
  (100%).
- `mise run spec-lifecycle specs/issue-240-oauth-cancelled-leader.spec.md` —
  PASS.
- `mise run ship` — PASS (full local CI, Conventional Commit validation, and Jujutsu push gate).
