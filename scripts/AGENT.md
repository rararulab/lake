# scripts/

Repo automation. **All scripts are TypeScript, run with bun** — no shell
scripts, and no inline scripts in `mise.toml` or CI YAML (they call these
files instead). Read `docs/guides/mise-ci.md` before changing Bun Shell usage.

- `doctor.ts` — session health check (`mise run doctor`)
- `check-conventional-commit.ts` — commit-message lint; two modes:
  `<msg-file>` (prek commit-msg hook) and `--range <rev-range>` (CI)
- `spec-lifecycle-guard.ts` — zero-match guard around `agent-spec
  lifecycle`; exit contract 0/1/2 documented in the file header
- `spec-selftest.ts` — regression lock: the guard must reject
  `specs/fixtures/zero-match.spec.md`
- `test-env.ts` — checkout-scoped LocalStack (DynamoDB + S3) in Docker, up/down
  (`mise run test-env-up` / `test-env-down`); writes the dynamic endpoint to
  `.lake/test-env.env`
- `test-integration.ts` — `mise run test-integration`: up → run the `#[ignore]`
  LocalStack tests (`--run-ignored ignored-only`) against the endpoint → down.
  `mise run test-integration-external` skips Docker lifecycle and runs the same
  package list against CI's already-provisioned LocalStack service container.
- `test-iceberg-env.ts` — checkout-scoped Apache Iceberg REST Catalog + public
  MinIO fixture, up/down, and dynamic endpoints written to
  `.lake/test-iceberg-env.env`.
- `test-iceberg-integration.ts` — `mise run test-iceberg-integration`: own the
  Apache fixture lifecycle, then run the ignored Iceberg interoperability test.
  `test-iceberg-integration-external` consumes caller-managed endpoints.
