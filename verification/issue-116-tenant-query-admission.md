# Issue #116 verification

Candidate base: `6cae973079b08ec1fffe466c72d65eeb09efc35e`

## Delivered contract

- Query admission applies a configurable per-tenant ceiling before the
  existing replica-wide ceiling, so one saturated tenant cannot reserve all
  execution capacity.
- Both admission levels share one absolute queue deadline and are owned by one
  RAII permit across planning or stream execution. Error paths release both
  levels without manual cleanup.
- Tenant tracker state is bounded and stored through weak references, allowing
  inactive tenant entries to be reclaimed without retaining tenant identity.
- All statement, discovery, and asynchronous-result Flight paths use the
  authenticated principal. Metrics and errors expose only identity-free
  admission outcomes.
- CLI and Kubernetes configuration validate global, per-tenant, tracker,
  queue, execution, and SQL-size limits before the Query listener starts.
  Documentation explicitly describes these limits as per-replica rather than
  cluster-global quotas.

## Red/green evidence

- The initial compile failed because tenant-aware construction, principal-aware
  acquisition, and CLI configuration did not exist.
- The first tracker implementation failed the redaction test because derived
  `Debug` exposed a tenant identifier. A custom identity-free implementation
  replaced it.
- The Kubernetes manifest test failed before the two new environment variables
  were added.
- The telemetry test rejected admission outcome labels containing `tenant`;
  the final labels describe saturation scope without carrying identity.

## Verification

- `mise run doctor` — PASS after trusting the jj workspace.
- `cargo check -p lake-query -p lake-cli --tests` — PASS.
- `cargo clippy -p lake-query -p lake-cli --all-targets -- -D warnings` — PASS.
- Focused admission, telemetry, discovery cleanup, CLI validation, and
  Kubernetes manifest tests — PASS.
- `mise run spec-lifecycle specs/issue-116-tenant-query-admission.spec.md` —
  PASS, all 6/6 scenarios and selectors executed.
- `git diff --check` — PASS.
- `mise run gate` — PASS in 131.80 seconds, including hooks, Rust workspace
  tests, upstream ADBC interoperability, site checks, and
  `ingest -> commit -> SQL` e2e. Query passed 61/61 unit tests; SDK passed
  44/44, with two explicit LocalStack tests ignored outside their integration
  runner.
