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
- `test-env.ts` — checkout-scoped kind + localstack integration deps up/down
  (`mise run test-env-up` / `test-env-down`); writes the dynamic DynamoDB
  endpoint to `.lake/test-env.env`
