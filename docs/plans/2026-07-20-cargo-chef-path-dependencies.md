# Cargo-chef Path Dependencies Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make the current release Docker recipe build an immutable historical
source whose Cargo workspace has the local `datafusion-execution` patch crate.

**Architecture:** The planner continues to derive the dependency recipe from the
release-source build context. Before `cargo chef cook`, the builder receives
only the local path crate named by that recipe. This preserves the existing
dependency-layer cache and deliberately invalidates it when that crate's source
changes; application sources still arrive only after dependency cooking.

**Tech Stack:** Docker BuildKit multi-stage builds, Cargo-chef, Rust
release-artifact contract tests, agent-spec.

---

### Task 1: Bind the missing local-crate input as a regression contract

**Files:**
- Modify: `crates/lake-cli/tests/release_artifacts.rs`
- Create: `specs/issue-321-cargo-chef-path-dependencies.spec.md`

**Step 1: Write the failing test**

Add `release_image_hydrates_path_dependencies_before_cargo_chef_cook`. It must
locate the recipe transfer, the exact
`COPY --from=planner /src/third_party/datafusion-execution third_party/datafusion-execution`
transfer, `cargo chef cook`, and the final `COPY . .` in `Dockerfile`.

**Step 2: Run test to verify it fails**

Run: `mise exec -- cargo test -p lake-cli --test release_artifacts release_image_hydrates_path_dependencies_before_cargo_chef_cook`

Expected: FAIL because the current builder transfers only `recipe.json` before
running `cargo chef cook`.

**Step 3: Define the lane-1 task contract**

Scaffold and complete the issue #321 spec. Bind its scenario to the new test,
allow only the Dockerfile, release-contract test, release-process guide, plan,
spec, and verification artifact; prohibit release authority, runtime, SQL,
storage, and credential changes.

### Task 2: Hydrate the path dependency without broadening the cache input

**Files:**
- Modify: `Dockerfile:24-26`
- Modify: `crates/lake-cli/tests/release_artifacts.rs`

**Step 1: Implement the minimal Docker layer**

Place this instruction after the recipe transfer and before `cargo chef cook`:

```Dockerfile
COPY --from=planner /src/third_party/datafusion-execution third_party/datafusion-execution
```

Do not copy all application source before `cook`, and do not change the release
workflow, image labels, tags, platforms, credentials, or runtime stages.

**Step 2: Run the focused contract**

Run: `mise exec -- cargo test -p lake-cli --test release_artifacts release_image_hydrates_path_dependencies_before_cargo_chef_cook`

Expected: PASS; the exact local crate is transferred after `recipe.json`, before
`cook`, and before application source.

**Step 3: Prove the historical build stage**

Run a local native-platform `docker build --target builder` using the candidate
Dockerfile and an immutable v1.8.4 checkout as build context.

Expected: `cargo chef cook` finds the path crate and the builder stage completes.

### Task 3: Document recovery and run all quality gates

**Files:**
- Modify: `docs/guides/mise-ci.md`
- Create: `verification/issue-321-cargo-chef-path-dependencies.md`

**Step 1: Document the cache prerequisite**

State that a recipe used to rebuild an older source must hydrate every local
Cargo path dependency needed by `cook`, while retaining the tag checkout as the
only source authority and build context.

**Step 2: Validate locally**

Run:

```bash
mise run spec-lifecycle specs/issue-321-cargo-chef-path-dependencies.spec.md
mise run gate
```

Expected: all commands pass.

**Step 3: Commit locally**

Commit the bounded implementation with a Conventional Commit that closes #321.

### Task 4: Independently verify, review, ship, merge, and recover releases

**Files:**
- Modify: `verification/issue-321-cargo-chef-path-dependencies.md`

**Step 1:** Have the independent verifier re-run the lane-1 lifecycle and gate
from clean state and record the red-to-green contract plus historical builder
evidence.

**Step 2:** Have the reviewer check the committed diff, task boundaries,
historical decisions, and Docker cache ordering.

**Step 3:** After approval, run `mise run ship`, push, open and merge the PR,
then clean the #321 jj workspace.

**Step 4:** Dispatch the repaired `release-image.yml` on `main` for v1.8.4,
verify its two-platform manifest and labels, then recover v1.8.3 and v1.8.2 in
the same way.
