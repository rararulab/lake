# Issue 23 verification — incremental object reference GC

## Acceptance

- `mise run spec-lint specs/issue-23-object-gc.spec.md`: 100% quality.
- `mise run spec-lifecycle specs/issue-23-object-gc.spec.md`: all six guarded
  scenarios passed; every selector executed at least one test.
- `cargo test -p lake-objects`: 13 unit/wiring tests passed; protocol tests are
  intentionally ignored outside LocalStack.
- `cargo clippy -p lake-objects -p lake-cli --all-targets -- -D warnings`:
  passed.
- `mise run test-integration`: 13/13 ignored LocalStack tests passed, including
  S3 inventory, resumable GC apply, Lance reference sidecars, DynamoDB, SDK
  direct uploads, range reads, and resumable multipart behavior. Nextest
  reported three AWS-runtime tests as leaky but successful; this is the
  existing SDK background-runtime classification, not a test failure.
- `mise run gate`: passed in 56 seconds (hooks, all workspace/all-target Rust
  tests, end-to-end selftest, site typecheck/tests/build).

## Safety properties exercised

- Canonical, chunked parent→child reference deltas reject corrupt and
  unsupported input.
- Lance writes a reference edge before exposing the version to the registry
  CAS and does not retain consumed RecordBatches.
- Missing or corrupt retained lineage prevents plan publication.
- Live references are externally sorted with bounded run size and merge
  fan-in; conflicting identities and removal deltas fail closed.
- Local and S3 inventories are bounded, sorted, and exact-prefix scoped.
- Young, live, and outside-stage candidates cannot enter an immutable plan.
- A plan manifest is published last and authorizes a content-addressed page
  chain. Apply accepts only the page hash named by its durable checkpoint.
- S3 apply resumes after one completed page, accepts an already-absent object,
  and leaves live/young objects untouched.
- CLI dry-run performs no mutation; `--apply` requires a checkpoint and binds
  the plan to the current registry-root fingerprint.

## Operational boundary

The safety horizon must exceed the maximum completed-upload-to-INSERT retry
window. Apply rechecks registry roots before each page and must run in a
write-quiescent window. Current append-only lineage keeps every committed
addition conservatively; non-empty removal deltas are rejected until
retained-snapshot-aware row deletion is implemented. Versioned S3 buckets
also need a noncurrent-version lifecycle policy for physical byte reclamation.
