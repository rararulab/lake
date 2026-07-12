# Issue #118 verification

Candidate base: `b5d1a17d8e2b0d9964fc1fde1ada82491e2bc8f7`

## Delivered contract

- Durable async workers use bounded process-local global and per-tenant
  running ceilings. Candidate selection round-robins eligible tenants and
  never parks a worker task behind a saturated tenant.
- The scheduler continues bounded state-page scans while owned jobs execute,
  tracks at most the configured worker count, and aborts/joins every owned task
  during shutdown.
- Candidate records are decoded directly from bounded scan values, eliminating
  the previous point read for every scanned job. Invalid records are counted
  and skipped without wedging unrelated work on the same page.
- Worker execution and lease renewal share one absolute deadline. Timeout drops
  the DataFusion stream, stops renewal, and CAS-publishes the stable
  `execution_timeout` terminal failure under the current worker lease.
- CLI and Kubernetes defaults are four workers, one worker per tenant, and a
  30-minute deadline. Validation happens before Query binds or starts
  background work; documentation states these are per-replica limits.

## Red/green evidence

- The first focused compile failed because `AsyncCandidate`, `AsyncScheduler`,
  and `AsyncSchedulerLimits` did not exist.
- The green scheduler test proves a saturated tenant is skipped while an
  eligible neighbor uses the free worker slot; a second test proves page-local
  round-robin selection.
- The scan regression uses a counting metastore adapter and observes zero point
  reads for pending, terminal, and corrupt records returned in one bounded
  page.
- A one-nanosecond worker deadline deterministically produces
  `ExecutionDeadline`, persists `execution_timeout`, and releases scheduler
  capacity. State-machine tests reject stale renewal and completion afterward.

## Verification

- `mise run doctor` — PASS in the new jj workspace.
- `mise run spec-lint specs/issue-118-async-tenant-fairness.spec.md` — PASS,
  quality 100%.
- `cargo check -p lake-query -p lake-cli --tests` — PASS.
- `cargo clippy -p lake-query -p lake-cli --all-targets -- -D warnings` — PASS.
- Focused scheduler, deadline, state-fence, telemetry, CLI, and Kubernetes
  tests — PASS.
- `mise run spec-lifecycle specs/issue-118-async-tenant-fairness.spec.md` —
  PASS, all 6/6 scenarios and selectors executed.
- `git diff --check` — PASS.
- `mise run gate` — PASS in 132.74 seconds, including Rust workspace tests,
  upstream ADBC interoperability, site checks, and `ingest -> commit -> SQL`
  e2e. Query passed 68/68 unit tests; SDK passed 44/44, with two explicit
  LocalStack tests ignored outside their integration runner.
