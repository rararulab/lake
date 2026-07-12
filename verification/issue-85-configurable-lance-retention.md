# Issue #85 verification

Candidate base: `5b82147c318b2ba7b0d62442547ea84835f1512d`

## TDD evidence

- `maintenance_policy_rejects_unbounded_retention` first failed with missing
  `LanceMaintenancePolicy` and retention constants; it passed after adding the
  bounded immutable value object.
- `maintenance_uses_configured_version_retention` first failed because
  `LanceEngine::with_maintenance_policy` did not exist; it passed after policy
  plumbing and proves a real Lance dataset is reduced to the configured three
  recent versions.
- `lance_retention_values_are_validated_before_storage_open` first failed with
  an unresolved parser import; it passed after CLI parsing and wiring before
  local/cloud storage construction.

## Focused verification

- `mise run spec-lint specs/issue-85-configurable-lance-retention.spec.md` —
  PASS, quality 100%.
- `mise run spec-lifecycle specs/issue-85-configurable-lance-retention.spec.md`
  — PASS, all three selectors executed at least one test.
- `cargo test -p lake-engine-lance -p lake-cli --all-targets` — PASS on the
  clean rerun: engine 32/32, CLI 24/24, CLI integrations 5/5, engine wiring
  1/1; LocalStack-only cases remained explicitly ignored.
- `cargo clippy -p lake-engine-lance -p lake-cli --all-targets -- -D warnings`
  — PASS.

## Existing intermittent test

The first package run observed
`lance_transaction_history_converges_idempotent_append` lose a shared
operation staging JSON while two same-operation appends raced. The #85 diff
does not touch append or staging paths and that test never calls maintenance.
The exact selector passed twice in isolation and the complete package rerun
passed. This pre-existing concurrency window is tracked as #86 rather than
being hidden or patched inside #85.
