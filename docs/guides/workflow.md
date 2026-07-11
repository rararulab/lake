# Development Workflow — Spec / Issue → Workspace → Local Commit → Verify → Review → Push → PR → Merge

**Every code change — no matter how small — MUST follow this workflow.**
Single-line fixes, typo corrections, config tweaks, doc updates, and refactors
all go through the workflow below. The main agent must NEVER directly edit
source files on the `main` branch.

Two structural rules shape the flow: **review happens BEFORE push, gating
it**, and an independent **verify step** sits between implement and review —
a fresh-context verifier runs the artifact from clean state; only it may
emit `verified` (implementer evidence is `self_check_only`).

```
Lane 1 (spec-driven — feature, bugfix, anything with testable behavior):
  0. SPEC AUTHOR    →  spec-author writes specs/issue-N-<slug>.spec.md
                       + opens GitHub issue referencing it
  1. WORKSPACE      →  parent creates .worktrees/issue-N-<slug>
                       (jj workspace add) and dispatches implementer
  2. IMPLEMENT      →  implementer reads spec; codes; runs the quality gate
                       (mise run gate; jj fires no git hooks) + lane-1
                       spec-lifecycle; commits LOCALLY (does not push)
  3. VERIFY         →  fresh-context verifier re-runs the gate from clean
                       state, runs the self-check cold, probes; writes
                       verification/report.md (FAIL → one repair round
                       → escalate)
  4. REVIEW         →  reviewer reads workspace diff + spec; verdict
                       (loop until APPROVE)
  5. PUSH + PR      →  implementer runs `mise run ship` (full local gate +
                       push through the jj-pre-push fmt/clippy gate); gh pr create
  6. MERGE          →  gh pr merge --squash --delete-branch (on local-gate PASS
                       + review APPROVE; CI is a post-merge backstop on main)
  7. CLEANUP        →  jj workspace forget + delete the dir

Lane 2 (lightweight chore — structural, cleanup, CI, rename, config):
  0. SPEC AUTHOR    →  spec-author writes the GitHub issue body directly
                       (Intent + prior art + decisions + boundaries; no
                       BDD scenarios; no specs/*.spec.md file)
  1-7. same as lane 1; the verify step runs the issue's `Verify:` commands
       instead of the spec's scenarios
```

## Picking the lane

`spec-author` makes this call. The single test:

> Can I write at least one `Test:` selector that binds to a real test
> function — one that fails before the change and passes after?

- Yes → **lane 1**.
- No → **lane 2**.

If unsure, lane 2 (overhead-on-the-side-of-less). Lane 1's value is the
binding to a real `cargo test` function; without that binding, lane 1
produces ceremony.

## Step 0: spec-author

`spec-author` is invoked **before any issue exists**. The parent agent
hands the user's request (verbatim) to spec-author. Spec-author:

1. Gates the request against the project's scope and the architecture
   invariants in `docs/architecture.md`.
2. Runs the mandatory prior-art search (`gh issue list`, `gh pr list`,
   `git log --grep`, `rg`). Do not skip — re-doing (or re-undoing) prior
   work is the most expensive failure mode.
3. For vague requests, asks 1–3 multi-choice clarifying questions.
4. Writes a private reproducer ("if we don't do this, this concrete bug
   appears: 1. … 2. … 3. observed bad outcome"). If no reproducer can be
   written, the request is too vague — escalate, do not proceed.
5. Picks the lane.
6. Drafts: lane 1 → scaffold with `mise run spec-init <slug>`, fill in
   `specs/issue-TBD-<slug>.spec.md`, lint with
   `mise run spec-lint <spec>` (min-score 0.7); lane 2 → issue body.
7. Files the GitHub issue with `agent:claude` + type + component labels.
   For lane 1, renames the spec from `issue-TBD-` to `issue-N-` once the
   issue number is assigned, and references the spec path in the issue body.

## Auto-chaining

Once the user has acknowledged the proposed plan, the parent agent chains
through the workflow steps mechanically: spec-author → workspace + implementer
→ verifier → reviewer → push → PR → merge. The rule is structured as a **whitelist**:
the only times the agent stops to re-ask are the gates enumerated below.
Anything not on the list runs without re-asking — including, explicitly,
the cases that have historically tripped agents into sycophantic
re-confirmation.

### Confirmation gates (exhaustive)

The parent agent stops and asks the user **only** in these cases:

- **(a) Merging to `main`.** The final gate before code lands. Always ask,
  even when CI is green and review is APPROVE'd.
- **(b) Destructive VCS operations.** `jj abandon` of pushed commits,
  force-push, deleting shared bookmarks, and any other operation that
  rewrites or discards pushed history.

This list is closed. Adding a new gate is a separate user decision — do
not infer one from a single failure mode.

### Default-continue (no re-ask)

Everything else runs without a confirmation round-trip:

- **Status queries mid-flow** — "where are we?" Answer the question; do
  not restate the plan and end with "shall I continue?".
- **Step transitions inside an already-approved plan** — after spec-author
  returns an issue number, the parent dispatches the implementer
  **directly**. The plan was already approved; re-asking is sycophancy,
  not safety.
- **Re-dispatching a stalled subagent** — if a subagent stops mid-task,
  the parent re-dispatches with the carried-over context.
- **Routine workspace / jj tool calls inside an approved change** —
  `jj commit`, `jj rebase -d main` inside the workspace, `mise run ship`,
  `gh pr create`, `gh pr merge` (subject to gate (a)).
- **PR label adjustments** — adding / removing type / component labels
  on a PR the agent owns.

## Step 1: Workspace

```bash
jj workspace add .worktrees/issue-{N}-{short-name}
cd .worktrees/issue-{N}-{short-name} && jj new main   # start work on top of main
```

The parent agent creates the workspace and then dispatches the implementer.
The main agent never edits in-place on `main` and never edits inside the
main checkout — every edit is in a workspace (enforced by
`.claude/hooks/guard-main-branch.ts`). The bookmark `issue-{N}-{short-name}`
is created before push (`jj bookmark create issue-{N}-{short-name} -r @-`).

## Step 2: Implement (lane 1 and 2)

Lake is a Rust workspace (`crates/lake-meta`, `lake-manifest`,
`lake-catalog`, `lake-cli`) with a single Rust lane, so there is one
implementer with one quality gate. The implementer:

1. Reads `gh issue view <N>`. For lane 1, also reads
   `specs/issue-N-<slug>.spec.md`.
2. Translates the request into a one-sentence outcome to verify, sends it
   back to the parent, and waits for ACK before coding. (This catches
   misalignment for the cost of a round-trip.)
3. Reads the actual code it will touch.
4. Implements the smallest change that satisfies the spec / issue.
5. Runs the quality gate — `mise run gate`:
   - `mise run hooks` — `prek run --all-files` (cargo check,
     `cargo +nightly fmt --check`, clippy `-D warnings`,
     `cargo +nightly doc -D warnings`)
   - `mise run test` — `cargo test --workspace --all-targets`
   - `mise run e2e` — `cargo run -p lake-cli`, the end-to-end self-check
     (ingest → commit → SQL query)
   - `mise run site-check` — frozen Bun install, TypeScript typecheck,
     Vitest, and the GitHub Pages production build
6. **Lane 1 only**: runs `mise run spec-lifecycle specs/issue-N-<slug>.spec.md`
   (routed through `scripts/spec-lifecycle-guard.ts` — a `Test:` selector
   matching zero tests FAILS even if agent-spec reports green) and confirms
   every `Test:` selector binds to a real test that fails at `base_sha`
   and passes at `head_sha`.
7. Commits locally (`jj commit`). Conventional Commits subject +
   `Closes #N` in body (see [commit-style.md](commit-style.md)).
8. **Does NOT push.** Reports back to the parent with the workspace path,
   commit SHAs, outcome verification (concrete evidence), and any
   decisions surfaced.

### The gate — jj fires no git hooks, so it runs on push (not commit)

jj has no staging area and no commit-time hooks. Two layers cover this:

1. **`jj push`** (a global alias → [jj-pre-push](https://github.com/acarapetis/jj-pre-push))
   automatically runs the [prek](https://github.com/j178/prek) hooks in
   `.pre-commit-config.yaml` (fmt + clippy) on the to-be-pushed commits and
   only pushes if green. Raw `jj git push` bypasses it. This is the fast,
   automatic gate.
2. **`mise run ship`** runs the *comprehensive* local gate before pushing:
   `mise run ci` (gate + doc + spec-selftest + **integration/LocalStack**) +
   `mise run check-commits` + push. This is deliberately broader than CI —
   local has Docker and no ephemeral limits, so it covers more than the
   `main`-only CI backstop.

```bash
mise run gate        # fast: hooks + Rust tests + e2e + site
mise run ci          # comprehensive: + doc + spec-selftest + integration
mise run ship        # mise run ci + conventional-commit check + push
```

`mise run hooks` runs prek against all files:

- `cargo check --all-targets`
- `cargo +nightly fmt --all -- --check`
- `cargo clippy --all-targets --all-features --no-deps -- -D warnings`
- `RUSTDOCFLAGS="-D warnings" cargo +nightly doc --no-deps --document-private-items`

Conventional Commit messages are enforced locally by `mise run ship`
(`mise run check-commits`) and by the reviewer — not by a commit-msg hook
(jj fires none) and no longer by CI (CI is `main`-only now).

Tooling comes from `mise install` (the user installs only mise); check
the environment with `mise run doctor`.

The **final** commit must pass the gate. Intermediate commits during
development don't need to pass.

## Step 3: Verify (independent, fresh context)

The parent dispatches the `verifier` subagent against the workspace,
giving it ONLY the workspace path, issue number, lane, and spec path
(lane 1) / the issue's `Verify:` commands (lane 2) — never the
implementer's report or evidence. Only the verifier may emit `verified`;
implementer evidence is `self_check_only`. The verifier:

1. Re-runs the full quality gate (`mise run gate`) from clean state.
2. Re-runs `mise run spec-lifecycle <spec>` and the spec's bound tests
   (lane 1) or the issue's `Verify:` commands (lane 2).
3. Cold-boots the candidate build — `cargo run -p lake-cli` with a
   **fresh data dir** (`rm -rf data` first; never the checkout's `data/`
   or another run's dir) — and drives the changed behavior end-to-end,
   including both sides of any write→read wiring (e.g. commit a manifest,
   then resolve it through the catalog).
4. Runs 2–3 hostile probes (concurrent commits racing the CAS pointer,
   empty tables, missing/garbage manifest input, CJK table names).
5. Writes `verification/report.md` in the workspace — `base_sha`,
   `head_sha`, `score_authority`, raw command outputs, PASS/FAIL verdict.

On FAIL: exactly **one** structured repair round back to the implementer
(failing probe inputs must land as regression tests), one re-verify,
then escalate to human. The report path is attached to the PR body at
step 5.

Verify and review catch disjoint failure classes — verify runs the
artifact, review reads the diff. That is why both exist and verify runs
first. A rebase or any new commit moves `head_sha` and invalidates a
prior PASS — re-verify before shipping; a stale PASS never rides into
the PR.

## Step 4: Review (BEFORE push)

The parent dispatches the `reviewer` subagent against the workspace (not
the PR — the PR does not exist yet). The reviewer:

1. Reads `git -C <workspace> diff origin/main..HEAD` (read-only git works
   in the colocated repo).
2. For lane 1: runs the **critical spec review** — re-runs
   `mise run spec-lifecycle <spec>` itself, and keeps the manual
   diff-vs-Boundaries glob check as a complementary P0 check. Do the
   scenarios actually falsify the Intent? Are they non-vacuous? Are
   Boundaries narrow? Does the change respect the architecture invariants
   in `docs/architecture.md` (immutable manifests, pointer-only KV,
   manifest-then-CAS commit order, backend types confined to
   `crates/lake-meta`)?
3. Runs the **cross-file regression-decision check** —
   `git log --since=30.days` on every file the diff touches, looking
   for prior commits that removed / restructured the same area. This
   catches the pattern of re-introducing what a recent PR explicitly
   removed.
4. Runs the standard code-review checks (correctness, style per
   [rust-style.md](rust-style.md), scope creep).
5. Inspects the implementer's outcome verification — is the evidence
   concrete? Does it verify the outcome, or only a side-effect?

Verdict:

- **REQUEST_CHANGES (P0/P1)**: implementer fixes in the workspace (new
  commits, no amend), re-runs verification, hands back. Loop until APPROVE.
- **REQUEST_CHANGES on the spec itself (lane 1)**: escalate to spec-author
  via parent. Implementer does NOT silently fix the spec.
- **APPROVE**: implementer proceeds to step 5.

## Step 5: Push + Open PR + Watch CI

Only after reviewer APPROVE:

```bash
# in the workspace — mise run ship runs the full local gate (ci + integration
# + conventional-commit check) then pushes through the jj-pre-push fmt/clippy
# gate. It pushes the current bookmark; create it first.
jj bookmark create issue-{N}-{short-name} -r @-
mise run ship

gh pr create --base main \
  --title "<type>(<scope>): <description> (#N)" \
  --body "..." \
  --label "<type>" --label "<component>"
```

PR body must include the step-3 verification report path + verdict
(e.g. `Verification: PASS — <workspace>/verification/report.md`). Labels:

- **Type** (pick one): `bug`, `enhancement`, `refactor`, `chore`, `documentation`
- **Component** (pick one, matches commit scope): `meta`, `manifest`,
  `catalog`, `ci`, `docs`, `harness`

Commit message must include `Closes #N` so the issue auto-closes on merge.

**CI is `main`-only now (local-first).** `.github/workflows/ci.yml` triggers
on `push: [main]` (+ `workflow_dispatch`), NOT on PRs — the local gate
(`mise run ship`) is the pre-merge check, and CI is a post-merge backstop that
re-runs the parallel matrix (clippy / test / doc / fmt / e2e / integration /
cargo-deny / cargo-shear) on Linux to catch macOS-vs-Linux platform
differences. So there is no PR-level CI to watch; run `workflow_dispatch`
manually if you want a Linux run on a branch before merge.

If the **post-merge** CI backstop on `main` fails: read the failure log,
diagnose root cause, fix it as a new lane-2 change on `main`. Do not mark
tests `#[ignore]` to make CI green. For genuine flakes (same test failed
recently on `main`): `gh run rerun <id> --failed`. Cap reruns at 1. To catch
a Linux platform difference *before* merge, run the CI workflow on the branch
manually with `workflow_dispatch`.

**Why review-before-push:** the local gate (`mise run ship`) catches
functional regressions locally — with Docker and no ephemeral limits it
covers *more* than CI. Review catches design issues, regression-decision
reversals, and scope creep. The `main`-only CI backstop only adds Linux
platform coverage on top; a platform-only failure lands post-merge and is
typically a one-line lane-2 fix.

## Step 6: Merge

Local gate PASS (`mise run ship`) + already-APPROVE'd review = merge — but
always confirm with the user first (gate (a)). CI runs as a backstop *after*
the squash lands on `main`.

```bash
gh pr merge {N} --squash --delete-branch
```

Use `--squash` so the merged commit on `main` matches the Conventional
Commit subject. `--delete-branch` removes the remote branch; the local
bookmark and workspace are removed in step 7.

## Step 7: Cleanup

```bash
jj workspace forget issue-{N}-{short-name}
rm -rf .worktrees/issue-{N}-{short-name}
jj bookmark delete issue-{N}-{short-name}   # the remote side is gone via --delete-branch
```

## Parallel execution

When user requests involve multiple independent changes, split into
separate issues at step 0 and dispatch implementer subagents in parallel:

- Each subagent gets its own workspace, bookmark, and PR.
- PRs are verified, reviewed, and merged independently on GitHub.
- The verifier and reviewer run per-PR; neither shares context across
  parallel PRs.
- **Temp data dir per run** — a verifier's `cargo run -p lake-cli` never
  points at the checkout's `data/` RocksDB dir or another run's temp dir.
- **Non-overlapping boundaries** — two in-flight issues must not touch
  the same files. Overlap means they are not independent; serialize them
  (or merge them into one issue) instead of letting them race.
- **Everything keyed by issue number** — workspace, bookmark, temp data
  dir, report are all named `issue-N-<slug>`, never by timestamp or
  random suffix, so every piece of evidence is attributable to exactly
  one dispatch.
