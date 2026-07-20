# Historical Release Image Recipe Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Let manual backfills compile an immutable historical release source with the current cached Docker recipe, without weakening release-source authority.

**Architecture:** The workflow receives two explicit checkouts. `release-source` is the trusted published tag and the sole Docker context; `build-recipe` is the immutable workflow revision selected by GitHub for the run. The OCI source label remains the former, while a second label makes the latter auditable.

**Tech Stack:** GitHub Actions, Docker Buildx, Cargo-chef, Rust workflow-contract tests, actionlint.

---

### Task 1: Bind the split-checkout contract

**Files:**
- Modify: `crates/lake-cli/tests/release_artifacts.rs`

**Step 1:** Add `release_image_workflow_separates_source_and_recipe_for_backfills` asserting the two checkout paths, source validation directory, Buildx context/file, and distinct recipe OCI label.

**Step 2:** Run `mise exec -- cargo test -p lake-cli --test release_artifacts release_image_workflow_separates_source_and_recipe_for_backfills`; expect failure because the old workflow has one checkout and context `.`.

### Task 2: Separate recipe from release source

**Files:**
- Modify: `.github/workflows/release-image.yml`

**Step 1:** Check out `github.sha` into `build-recipe` and the release tag into `release-source`.

**Step 2:** Move validation to `release-source`; record both immutable revisions, retain source revision as the existing OCI revision, and add the recipe-specific label.

**Step 3:** Point Buildx `context` at `release-source` and `file` at `build-recipe/Dockerfile`.

**Step 4:** Re-run the focused test; expect pass. Run `actionlint .github/workflows/release-image.yml`.

### Task 3: Document and validate recovery operations

**Files:**
- Modify: `docs/guides/mise-ci.md`

**Step 1:** Explain that manual historical backfills dispatched from `main` use the main workflow's Docker recipe while retaining tag source authority, and identify both OCI revision labels.

**Step 2:** Run `mise run spec-lint specs/issue-318-historical-release-image-recipe.spec.md` and `mise run spec-lifecycle specs/issue-318-historical-release-image-recipe.spec.md`.

### Task 4: Gate and ship

**Files:**
- Create: `verification/issue-318-historical-release-image-recipe.md`

**Step 1:** Run `mise run gate`, then `mise run ship` after independent verify/review approval.

**Step 2:** Push, open and merge the PR, dispatch v1.8.4 from `main`, and verify an amd64+arm64 manifest and source/recovery labels.

**Step 3:** Clean the jj workspace after merge.
