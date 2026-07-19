# Issue #222 verification

## Delivered contract

- README contains one canonical external-Iceberg deployment section near the
  quick start.
- That section contains the complete configuration, authentication, timeout,
  authorization, exact-snapshot, durable-worker, and write-boundary contract.
- Direct `FILE` reads and durable SQL examples remain in the SQL-managed-files
  workflow, where they are relevant to SDK users.

## Static checks

- One `LAKE_ICEBERG_REST_ENDPOINT` configuration block remains in `README.md`.
- One external-Iceberg heading remains in `README.md`.
- The durable `PollFlightInfo` exact-snapshot rule is stated once in the
  canonical deployment section and referenced from the durable-SQL workflow.

## Verification

- README heading/configuration/fence static checks passed.
- `cargo check --workspace --locked` passed against `main@v1.3.2`.
- `prek run --all-files` passed.
- `mise run gate` passed.
- Independent scope/diff review: no P0 or P1 findings.
