# Verification report — issue #318

- base_sha: `3e37a4a324d986b813479d8ece9af884cc20866e` (`git -C <workspace> merge-base HEAD origin/main`)
- head_sha: `3e37a4a324d986b813479d8ece9af884cc20866e` (`git -C <workspace> rev-parse HEAD`)
- jujutsu_candidate_sha: `1ff5d79bbef5e537dbd1d7a8ada54efd05828815` (`@-`, the committed candidate whose workspace contents were verified)
- jujutsu_comparison_base_sha: `3729455699c7d9ed28b7b57263ab8abf5a283a50` (`main@origin`)
- score_authority: verifier
- implementer_evidence: self_check_only

The colocated Git checkout remains detached at `3e37a4a` while Jujutsu
materializes the candidate from `@-`; the required Git commands above are
recorded verbatim, and the Jujutsu commits pin the actual compared contents.
The candidate was clean before verification and did not move during it.

## Commands

`mise run doctor`

```text
[doctor] $ bun scripts/doctor.ts
[ ok ] mise tools installed
[ ok ] nightly rustfmt
```

`jj status` before gate

```text
The working copy has no changes.
Working copy  (@) : vrzzpnwk f7f64bbf (empty) (no description set)
Parent commit (@-): trxyvwtu 1ff5d79b fix(release): recover historical image builds (#318)
```

`rm -rf data && mise run gate` (removed the pre-existing workspace `data/`
before the gate's e2e cold boot; exit 0)

```text
[hooks] $ prek run --all-files
[test] $ cargo test --workspace --all-targets
[e2e] $ cargo run -p lake-cli -- selftest
[adbc-install] $ uv sync --project interop/adbc --frozen
[test-adbc] $ cargo test -p lake-query --test adbc_interop -- --ignored --test-threads=1
[site-install] $ bun install --cwd site --frozen-lockfile
[site-check] $ bun run --cwd site check
[hooks] Finished in 65.8ms
[adbc-install] Finished in 80.9ms
[site-install] Finished in 152.9ms
test result: ok. 23 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
test result: ok. 113 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out
test result: ok. 1 passed; 0 failed; 3 ignored; 0 measured; 0 filtered out
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

`mise run spec-lifecycle specs/issue-318-historical-release-image-recipe.spec.md`

```text
=== Lifecycle Report (guarded) ===
Spec: historical-release-image-recipe  stage: complete  passed: true
  [PASS] Historical backfill builds immutable source with auditable current recipe
  [PASS] Split checkout preserves trusted multi-architecture publication
  [PASS] Historical recovery still rejects an untrusted release source
spec-lifecycle-guard: OK — every Test selector executed >=1 test
```

Each spec selector was then run independently:

```text
$ mise exec -- cargo test -p lake-cli --test release_artifacts release_image_workflow_separates_source_and_recipe_for_backfills
test release_image_workflow_separates_source_and_recipe_for_backfills ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 8 filtered out

$ mise exec -- cargo test -p lake-cli --test release_artifacts release_image_workflow_is_tag_pinned_and_multiarch
test release_image_workflow_is_tag_pinned_and_multiarch ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 8 filtered out

$ mise exec -- cargo test -p lake-cli --test release_artifacts release_image_workflow_rejects_mismatched_tags_and_preserves_digest_pinning
test release_image_workflow_rejects_mismatched_tags_and_preserves_digest_pinning ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 8 filtered out
```

`mise exec -- actionlint .github/workflows/release-image.yml` exited 0 with
no output.

The historical-source planner check used an isolated Jujutsu workspace whose
parent was tag `v1.8.4` (`1fbf3ace`):

```text
$ docker build --no-cache --target planner --progress=plain -f <candidate>/Dockerfile <v1.8.4-workspace>
#11 [planner 1/2] COPY . .
#11 DONE 0.1s
#12 [planner 2/2] RUN cargo chef prepare --recipe-path recipe.json
#12 DONE 11.2s
#13 writing image sha256:21021aa3bf03ecc33baaa4f5ff09d17090867044ea494e76f589721a858b8d76 done
#13 DONE 1.0s
docker-build-exit=0
```

The workflow has no runtime / SQL / storage surface. Its changed executable
surface is the release validation script; the positive control and hostile
inputs below ran that exact script extracted from the candidate workflow,
using an isolated tagged Git source and a fake `gh api` response.

## Transition matrix

- fail_to_pass: Observed. From `main@origin` (`37294556`), running the
  candidate spec through `mise run spec-lifecycle` exited 1 because
  `release_image_workflow_separates_source_and_recipe_for_backfills` matched
  zero tests. At the candidate, the guarded lifecycle exited 0 and that
  selector ran one passing test. The other two pre-existing selectors were
  pass-to-pass at base (one test each) and remain passing at head.
- pass_to_fail: 0. The full candidate quality gate, all three selectors,
  actionlint, planner execution, and workflow-script probes passed.

## Probes

- Control input: a local `v1.8.4` tag, non-draft published release JSON, and
  its exact checked-out target SHA. Expected: validation exports source and
  recipe revisions. Observed:

  ```text
  control=PASS
  VERSION=1.8.4
  RELEASE_REVISION=0d6360d63a227ce172d7b5565f090590a9176dfa
  BUILD_RECIPE_REVISION=37c64f75dffd985b736fa41ade25b70a895dbf88
  ```

  PASS.

- Input: malformed manual tag `v1.8`. Expected: reject before release lookup
  or publication. Observed: `release tag must be vX.Y.Z: v1.8` and
  `malformed=PASS`. PASS.

- Input: valid tag with a draft GitHub Release response. Expected: reject an
  unpublished source. Observed:
  `manual dispatch requires an already-published release for v1.8.4` and
  `draft=PASS`. PASS.

- Input: valid published release JSON with target SHA
  `0000000000000000000000000000000000000000`. Expected: reject the checked-out
  tag / trusted-release mismatch before Buildx. Observed:
  `checked-out tag does not match the trusted release revision` and
  `mismatch=PASS`. PASS.

## Verdict

PASS — the clean gate, guarded spec lifecycle, every selector, actionlint,
uncached historical `v1.8.4` Cargo-chef planner, and release-source trust
probes all passed, with the required zero-match-to-real-test transition and no
observed regressions.
