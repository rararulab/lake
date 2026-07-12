# Issue #114 verification

Candidate base: `e32ff224723ba76b2a37a1073131923ad8da7cc5`

## Delivered contract

- Apache Arrow's official ADBC Flight SQL 1.11.0 driver and PyArrow 24.0.0
  run as a frozen black-box client against real loopback Query listeners.
- The Rust fixture owns ephemeral RocksDB/Lance state, a 20,000-row typed
  table, listener lifecycle, and finite startup, subprocess, and shutdown
  deadlines. Python remains test-only and is absent from production binaries.
- The compatibility matrix proves typed multi-batch `GetFlightInfo`/`DoGet`,
  deployment bearer propagation, missing/wrong bearer rejection, and stable
  client-visible rejection of public DML.
- Existing standard Arrow Flight tests cover `PollFlightInfo` descriptor
  chaining, completed endpoint redemption, and idempotent `CancelFlightInfo`.
  The documentation does not misrepresent these low-level RPCs as ordinary
  ADBC DB-API operations.

## Red/green evidence

The first upstream run failed all three black-box checks:

- A read-only `generate_series` table function was deliberately outside the
  Lake catalog authorization model and failed closed as an unknown resource.
  The fixture now queries an actual registered immutable Lake table, matching
  the supported public contract while still proving multiple Arrow batches.
- `INSERT INTO` a missing table attempted snapshot resolution before
  DataFusion's read-only option check, so ADBC observed a misleading pinned
  snapshot error. Query now classifies DML immediately after parsing and
  returns `InvalidArgument: DML not supported` before catalog or snapshot I/O.
- The bearer success case then passed against a registered Lake table; wrong
  and missing credentials remain rejected by the per-RPC interceptor.

`flight_dml_is_rejected_before_snapshot_resolution` additionally asserts the
stable DML error and proves the catalog authority receives zero scans.

## Verification

- `mise run doctor` — PASS after trusting the new jj workspace.
- `mise run spec-lint specs/issue-114-adbc-flight-interop.spec.md` — PASS,
  quality 100%.
- `mise run spec-lifecycle specs/issue-114-adbc-flight-interop.spec.md` — PASS,
  all 6/6 scenarios and selectors executed.
- `mise run test-adbc` — PASS, 3/3 ignored black-box protocol tests explicitly
  selected through the frozen uv environment.
- `cargo test -p lake-query flight_dml_is_rejected_before_snapshot_resolution`
  — PASS.
- `cargo clippy -p lake-query --all-targets -- -D warnings` — PASS.
- `mise run gate` — PASS in 47.77 seconds on the final cached run, including hooks, the ADBC task,
  Rust workspace tests, site checks, and `ingest -> commit -> SQL` e2e.
  Query passed 57/57 unit tests; SDK passed 44/44, with two explicit LocalStack
  tests ignored outside their integration runner.
- `git diff --check` — PASS.
