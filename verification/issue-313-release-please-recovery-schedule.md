# Verification report — issue #313

- base_sha: 2dca0bc8
- head_sha: 114b95c7
- score_authority: verifier
- implementer_evidence: self_check_only

## Commands

This report was revalidated after the candidate was rebased. The original full
gate evidence remains below; the requested post-rebase checks and the parent's
post-rebase full gate are recorded in the next section.

`jj git fetch`

```text
Nothing changed.
```

`jj st`

```text
The working copy has no changes.
Working copy  (@) : yrswzqwo ed8b7667 (empty) (no description set)
Parent commit (@-): nlwtusks 706c9c30 fix(release): schedule Release Please recovery (#313)
```

`jj log -r 'main@origin | @ | @-' -n 5` after the rebase

```text
@  c033703b (no description set; verification-report child)
○  114b95c7 fix(release): schedule Release Please recovery (#313)
◆  2dca0bc8 main feat(robotics): define EpisodeManifest v1 contract (#308) (#315)
```

The candidate is the verification working copy's parent, `114b95c7`; the
working-copy commit is intentionally a child containing this report only.

### Post-rebase minimal revalidation

`mise exec -- cargo test -p lake-cli --test release_artifacts release_please_has_automatic_recovery_triggers`

```text
running 1 test
test release_please_has_automatic_recovery_triggers ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 7 filtered out; finished in 0.00s
```

Exit status: 0.

`mise run spec-lifecycle specs/issue-313-release-please-recovery-schedule.spec.md`

```text
=== Lifecycle Report (guarded) ===
Spec: release-please-recovery-schedule  stage: complete  passed: true
  [PASS] Release Please has recurring and immediate recovery triggers
spec-lifecycle-guard: OK — every Test selector executed >=1 test
```

Exit status: 0.

`mise run fmt-check`

```text
[fmt-check] $ cargo +nightly fmt --all -- --check
```

Exit status: 0.

`mise run gate`

```text
[hooks] Finished in 119.9ms
[test-adbc] test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out
[e2e] self-check ok
[test] test release_please_has_automatic_recovery_triggers ... ok
[test] Finished in 35.31s
[site-check] Result (24 files):
[site-check] - 0 errors
[site-check] - 0 warnings
[site-check] All matched files use Prettier code style!
Finished in 35.32s
```

Exit status: 0. This is the original full-gate evidence before the rebase.

`mise run gate` after rebase (parent-executed)

```text
e2e self-check ok
workspace tests passed
site check passed
```

Exit status: 0. The process ended normally with no failure output.

`git diff --check origin/main...HEAD`

```text
<no output>
```

Exit status: 0.

## Transition matrix

- fail_to_pass: Observed. The pre-change base workflow deliberately fails the recurring
  recovery probe: `baseline lacks hourly off-the-hour recovery` (exit 1).
  At rebased candidate `114b95c7`, the bound Rust test passes and the guarded
  spec lifecycle confirms its selector executed at least one test.
- pass_to_fail: 0. The revalidated bound checks, formatter, and parent-run
  complete local gate all passed on the rebased candidate.

## Probes

1. **Baseline transition probe** — parsed
   `origin/main:.github/workflows/release-please.yml` and required the exact
   `17 * * * *` cron. Expected: fail before the change. Observed:
   `baseline lacks hourly off-the-hour recovery`; exit 1. PASS.
2. **Recovery and authority wiring** — parsed the candidate workflow and
   required `push` on `main`, `workflow_dispatch`, exactly one schedule with
   cron `17 * * * *`, the non-cancelling
   `release-please-${{ github.ref }}` group, exactly one
   `googleapis/release-please-action@...` step, and exactly one existing image
   handoff. Expected: all preserved through one release authority. Observed:
   `recovery trigger, immediate dispatch, non-cancelling authority, and single image handoff: PASS`; exit 0. PASS.
3. **Boundary and syntax probe** — candidate diff contains only the four
   spec-allowed paths; Ruby YAML parsing printed `YAML parse: PASS`, and
   `git diff --check` exited 0. Expected: valid YAML with no forbidden release
   files or whitespace errors. Observed: PASS.

## Runtime drive

Not applicable. This change only alters GitHub Actions event scheduling; it
does not add or modify a Lake runtime surface. The standard `mise run gate`
still drove the independent Lake e2e selftest from its existing state and it
reported `self-check ok`.

## Verdict

PASS — post-rebase minimum revalidation passed: `114b95c7` preserves the
bounded hourly off-the-hour recovery trigger, immediate manual recovery, and
the sole idempotent release authority. The original independent full gate and
the parent-executed post-rebase full gate are green.
