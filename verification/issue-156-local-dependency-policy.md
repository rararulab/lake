# Issue #156 verification

Candidate base: `00535ac90fb608004cacc6548a0e0afec3bb99ba`

## Toolchain and policy

- `mise install` — PASS. Installed `cargo-deny 0.19.0` and `cargo-shear
  1.13.1` from the versions pinned in `mise.toml`.
- `mise ls --current` — PASS. Both policy tools resolve to their pinned
  versions from this checkout's `mise.toml`.
- `mise run dependency-deny` — PASS. Advisory, ban, license, and source
  policy all reported `ok`.
- `mise run dependency-shear` — PASS. Reported `no issues found`.

## Gate and task wiring

- `mise run gate` — PASS. Hooks, workspace tests, pinned upstream ADBC tests,
  CLI e2e self-check, and site checks all completed successfully.
- `mise run ci` — the new dependency tasks were both admitted from its
  dependency graph. The run stopped only when the existing LocalStack lifecycle
  could not contact Docker at
  `/Users/ryan/.orbstack/run/docker.sock`; the daemon socket does not exist.
  The two policy tasks were separately run to completion above and were not
  skipped or downgraded.
- A static task/workflow assertion — PASS. `ci` depends on
  `dependency-deny` and `dependency-shear`, `gate` does not; both path-filtered
  GitHub jobs invoke their matching `mise run` task without `install-action`,
  and both are dependencies of `CI OK`.
- TOML/YAML parse and `git diff --check` — PASS.
