# Issue #230 verification

## Delivered contract

- `IcebergCatalogConfig::Debug` now reports an opaque configured warehouse
  marker instead of the warehouse identifier.
- The original warehouse string remains available through `warehouse()` and is
  unchanged on the REST catalog connection path.
- No new warehouse parsing or validation is introduced, preserving
  provider-specific object-store URI forms.

## Red/green evidence

- Before the implementation, the new
  `iceberg_catalog_config_debug_redacts_warehouse` regression test failed at
  `warehouse identifier must not appear in diagnostics`: the full synthetic
  `abfss://tenant-secret@...` identifier appeared in `Debug` output.
- After replacing only the diagnostic field, the same test passes while
  asserting that both the full identifier and `tenant-secret` substring are
  absent, the opaque marker is present, and `warehouse()` returns the original
  string.

## Verification

- `cargo test -p lake-iceberg iceberg_catalog_config_debug_redacts_warehouse`
  — PASS after the patch; it was the expected red failure before the patch.
- `cargo +nightly fmt --check --all` — PASS.
- `mise run spec-lint specs/issue-230-iceberg-diagnostic-redaction.spec.md`
  — PASS (100%).
- `mise run spec-lifecycle specs/issue-230-iceberg-diagnostic-redaction.spec.md`
  — PASS (the selector resolved and executed).
- `cargo test -p lake-iceberg` — PASS (12 catalog and 3 configuration tests).
- `cargo clippy -p lake-iceberg --all-targets --all-features -- -D warnings` — PASS.
- `mise run gate` — PASS (hooks, workspace tests, ADBC interoperability,
  end-to-end ingest/commit/SQL, and site build).
