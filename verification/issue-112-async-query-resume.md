# Issue #112 verification

Candidate base: `2c8ee1527a9046a747536a89590ceb5ff1f99b7d`

## Delivered contract

- Every SDK async submission carries a UUIDv7 idempotency key in the standard
  Flight SQL transaction-id field. Query derives an identity-scoped durable
  job id, and exact retries converge through compare-and-swap.
- A reused submission id with another SQL statement fails closed instead of
  aliasing the existing job.
- `AsyncQueryHandle` is versioned, size bounded, expiry checked, serializable,
  and redacts its opaque Flight capability from `Debug` output.
- A new SDK process can deserialize the handle, poll through another Query
  replica, cancel idempotently, and open the completed bounded result stream.
- Ambiguous initial submission failures retry the same descriptor and bypass
  catalog replanning once the durable job exists, preserving its pinned table
  snapshots.

## TDD and protocol evidence

- `async_submission_id_retries_converge_on_one_job` covers sequential Flight
  retries and statement conflicts.
- `coordinator_submission_id_retries_converge_on_one_job` covers concurrent
  compare-and-swap races.
- `poll_flight_info_submits_identity_bound_pinned_job` retries a lost response
  through a fresh Query replica whose catalog backend is intentionally empty;
  it returns the original job and pinned snapshot without replanning.
- SDK tests serialize and restore a handle across clients and replicas, cancel
  the restored job twice, and verify the convenience API delegates to the same
  durable lifecycle.
- Result endpoint count is capped at 4096, and each opaque result ticket must
  be non-empty and no larger than 16 KiB.

## Verification

- `mise run doctor` — PASS.
- `mise run spec-lifecycle specs/issue-112-async-query-resume.spec.md` — PASS,
  all 6/6 scenarios and selectors executed successfully.
- `mise run gate` — PASS in 345.92 seconds, including hooks, Rust workspace
  tests, site checks, and `ingest -> commit -> SQL` e2e self-check. Query passed
  56/56 unit tests; SDK passed 44/44 tests, with two explicit LocalStack tests
  ignored outside their integration runner.
- `cargo clippy -p lake-query -p lake-sdk --all-targets -- -D warnings` — PASS.
- `git diff --check` — PASS.

