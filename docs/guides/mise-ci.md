# Mise + GitHub CI Standards

This guide is the source of truth for local tool setup, Bun shell scripts, and
GitHub Actions. If an agent edits `mise.toml`, `.pre-commit-config.yaml`,
scripts under `scripts/`, or `.github/workflows/*.yml`, it MUST read this file
first.

## Goals

- A new contributor installs only `mise`; `mise install` installs project
  tools, and `mise run doctor` proves the workstation is usable.
- Local checks and CI execute the same named `mise` tasks. CI is not a
  second copy of cargo / formatter / linter commands.
- Rust stays under `rustup` and `rust-toolchain.toml`; mise does not manage
  Rust for this repo.
- Local mise tasks put Rust artifacts in the user's XDG cache at
  `lake/target`, rather than in each jj workspace. Cargo's normal fingerprints
  and target-directory lock keep concurrent revisions correct; the cache is
  untracked, contains no credentials or source data, and may be deleted to
  force a cold rebuild. CI remains ephemeral and sets its own incremental
  policy.
- **Local-first**: the comprehensive gate runs LOCALLY (`mise run ship`, which
  runs `mise run ci` — gate + dependency policy + doc + spec-selftest +
  LocalStack integration — then a conventional-commit check, then push through
  the jj-pre-push fmt/clippy gate). CI (`ci.yml`) triggers on `push: [main]` +
  `workflow_dispatch` only; it is a post-merge Linux backstop, NOT run on PRs
  and NOT the first place checks run. Local covers *more* than CI (Docker, no
  ephemeral limits).
- TypeScript scripts use Bun Shell deliberately: safe interpolation, explicit
  error handling, and structured parsing instead of fragile text pipelines.

## Ownership

- `mise.toml` owns tool versions, shared environment variables, and task names.
- `rust-toolchain.toml` owns the stable Rust toolchain and stable components.
- `scripts/*.ts` owns any task logic longer than one shell command.
- `.pre-commit-config.yaml` owns hook wiring, but hook commands should call the
  same underlying commands exposed by `mise.toml`.
- `.github/workflows/ci.yml` owns GitHub-specific orchestration only:
  checkout, Rust bootstrap, mise bootstrap, caching, permissions, concurrency,
  and calls to `mise run ...`.

## Tool Version Rules

- Pin CI-critical tools to concrete versions in `mise.toml`. Do not use
  `latest` for `bun`, `uv`, `jj`, `gh`, `prek`, `agent-spec`, `cargo-deny`,
  `cargo-shear`, `cargo-nextest`, or `protoc` unless the PR is explicitly a
  toolchain refresh and records the reason.
- Top-level `[tools]` is the base developer environment. Do not put deploy-only
  tools (cloud emulators, load-test tools) there; attach them to the deploy tasks
  that need them.
- Tool bumps are their own chore unless a feature genuinely requires them.
- After changing `[tools]`, run `mise install`, `mise ls --current`, and
  `mise run doctor`.
- Do not add Rust to `[tools]`. Install stable / nightly with `rustup`; keep the
  stable channel in `rust-toolchain.toml`.

## Task Rules

- `mise run doctor` is the first command in a new session. It may warn about
  optional GitHub workflow gaps, but it must fail for missing required tools or
  a broken Rust build.
- `mise run gate` is the fast local push gate. It must include hooks, Rust
  tests, the e2e self-check, and `site-check` for the Astro site and rendered
  documentation.
- `mise run test-adbc` serializes its test functions because each launches a
  bounded upstream-driver subprocess and Query fixture. This preserves the
  ADBC deadline while `gate` continues running independent tasks in parallel.
- `mise run test-integration` owns checkout-scoped LocalStack lifecycle;
  `mise run test-integration-external` runs the identical ignored-only package
  suite against a caller-managed endpoint and is the GitHub CI entry point.
  Both exclude the Apache Iceberg REST fixture test; its environment is not a
  LocalStack fallback.
- `mise run test-iceberg-integration` owns an Apache Iceberg REST Catalog plus
  MinIO lifecycle and runs the real ignored interoperability test. It belongs
  to the comprehensive `ci` task, not the fast `gate`; its external variant
  consumes caller-managed fixture endpoints.
- `mise run ci` is the full CI gate. It must include `gate`, both dependency
  policy tasks, Rustdoc warnings, and spec tooling self-tests.
- If a CI check protects a repo invariant, expose it as a `mise` task and run
  it from `ci`; include it in `gate` only when it belongs in the fast local
  loop.
- Lane-1 work also runs `mise run spec-lifecycle <spec>`.
- `spec-lifecycle` discovers changed paths through `jj diff`, not Git's
  worktree view, so its boundary check is scoped to the current colocated
  Jujutsu workspace.
- `site-check` must start from `bun install --frozen-lockfile`, then typecheck,
  lint, format-check, build, index, and smoke-test the static output. This keeps
  local and Pages builds on the same dependency graph.
- Task names are part of the agent contract. Rename a task only with matching
  updates to `AGENT.md`, `CLAUDE.md`, workflow docs, hooks, and CI.
- Parameterized tasks use mise's `usage` field and `${usage_name?}`
  environment variables. Do not use deprecated `{{arg(...)}}`,
  `{{option(...)}}`, or `{{flag(...)}}` templates.
- Keep `mise.toml` declarative. Move loops, parsing, and multi-step logic into
  `scripts/*.ts` and invoke those scripts from tasks.

## Bun Shell Rules

- Scripts start with `#!/usr/bin/env bun` and import `import { $ } from "bun";`
  only when they actually execute external commands.
- Prefer ``$`cmd ${arg}``` interpolation over string-built shell commands. Bun
  treats interpolated strings as single literal arguments, which prevents normal
  shell injection.
- Do not use `${{ raw: value }}` unless the input is a compile-time constant in
  the script. Raw interpolation is an escape hatch, not a convenience.
- Do not pass user or repo-derived strings through `bash -c`, `sh -c`, or
  another shell interpreter. If that is unavoidable, validate every argument
  before it crosses into that shell.
- Remember that escaping is not authorization. External programs can still
  treat a safe literal string as a flag, e.g. `--upload-pack=...`; validate
  values that become command arguments.
- Use `.text()`, `.json()`, or `.lines()` when consuming output. Avoid parsing
  human-oriented command output when a structured API or machine-readable flag
  exists.
- Use `.quiet()` for probes where output is not evidence. Keep noisy command
  output for failure evidence or user-facing reports.
- Use `.nothrow()` only when non-zero exit codes are part of the expected
  control flow, and check `exitCode` immediately.
- Use `Bun.spawn([...])` for pure pass/fail probes that do not need shell
  features. Use Bun Shell for pipelines, redirection, env assignment, and
  concise command composition.
- Prefer `.cwd(path)` and `.env({...process.env, KEY: value})` over global
  mutation of process state.

## GitHub Actions Rules

- Use `jdx/mise-action` to install mise-managed tools in CI. Do not hand-install
  Bun, uv, agent-spec, cargo-nextest, protoc, prek, jj, or gh in workflow YAML.
- Install Rust toolchains before `Swatinem/rust-cache`, because the cache key
  depends on the active Rust version.
- CI steps should call `mise run ci`, `mise run check-commits`, or another
  named task. Do not duplicate cargo command lines in YAML.
- Use least-privilege permissions. Normal CI uses `contents: read`; jobs that
  comment, label, publish, or upload need explicit extra permissions.
- Use workflow-level `concurrency` keyed by workflow + PR number/ref, with
  `cancel-in-progress: true`, so force-pushes do not burn runner time.
- Every hosted job declares an explicit `timeout-minutes` budget. The Apache
  Iceberg REST integration job uses the same 30-minute cold-run margin as the
  LocalStack integration job; do not leave a Docker fixture to the platform's
  default six-hour timeout.
- Do not make CI depend on local-only state such as installed hooks, local data
  directories, or untracked files.

## Conventional Commits

- jj does not run git hooks, so commit messages are enforced twice:
  local `commit-msg` hook for git users, and the PR-only CI commit job.
- The CI commit job should call a mise task that wraps
  `scripts/check-conventional-commit.ts --range <base>..HEAD`.
- The accepted format remains documented in `docs/guides/commit-style.md`.

## Repository releases

Release Please maintains one repository release for the entire lake workspace.
It does not publish the internal crates, which remain `publish = false`.

- `.github/workflows/release-please.yml` runs after a push to `main`. It also
  exposes `workflow_dispatch` as the recovery path when GitHub delayed or
  missed a push event; dispatch it on `main`, never by hand-editing release
  files or tags.
- `release-please-config.json` uses the `simple` strategy because upstream
  release-please cannot currently process Cargo members that inherit
  `version.workspace = true` through its `cargo-workspace` plugin
  ([upstream issue #2111](https://github.com/googleapis/release-please/issues/2111)).
- `version.txt` and `.release-please-manifest.json` track the repository
  release version. TOML extra-file updaters keep
  `workspace.package.version` and every lake package entry in `Cargo.lock`
  synchronized.
- When a new crate is added, add its exact `Cargo.lock` extra-file JSONPath
  using `.name.value` to `release-please-config.json`.

The workflow uses GitHub's short-lived built-in `GITHUB_TOKEN`; no long-lived
release credential is stored. This matches the repository's local-first model:
generated release PRs do not trigger a separate pull-request workflow.
Maintainers review the version/changelog diff and run `mise run gate` before
merge, while the existing main-only CI remains the post-merge backstop.

Release Please continuously updates one release PR from Conventional Commits.
Merging that PR updates the changelog and versions, creates `vX.Y.Z` without a
component prefix, and publishes the matching GitHub Release. When that root
release is created, the same short-lived token automatically dispatches the
existing image workflow with the exact release tag. The image workflow still
checks out and validates that published immutable tag before it publishes the
multi-platform GHCR image. Its 180-minute job budget leaves room for a cold
QEMU build while making a stalled release a finite, actionable failure instead
of relying on the GitHub Actions default.

The `release` event created by `GITHUB_TOKEN` intentionally does not trigger a
second workflow, so this `workflow_dispatch` handoff is required rather than
optional wiring. For a release made before the handoff existed, run the manual
image backfill with the published release tag:

```bash
gh workflow run release-image.yml --ref main -f tag=vX.Y.Z
```

Wait for the `Publish release image` run to complete, then resolve the
manifest-list digest before updating a production deployment.

## Review Checklist

Before approving a PR that changes the toolchain, scripts, or CI:

- `mise.toml` remains the single task registry.
- CI does not duplicate commands already represented by `mise` tasks.
- Tool versions are pinned or the PR explicitly explains why a moving version is
  acceptable.
- Bun Shell scripts use safe interpolation and explicit error handling.
- `mise run doctor`, `mise run gate`, and the relevant slower task
  (`mise run ci`, `mise run doc`, or `mise run spec-selftest`) pass locally.
- `mise tasks` output is still understandable to an agent reading `AGENT.md`.
