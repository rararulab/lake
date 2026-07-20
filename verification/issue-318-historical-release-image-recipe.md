# Verification report — issue #318 (current candidate)

- jj_base: `3729455699c7d9ed28b7b57263ab8abf5a283a50` (`main@origin`)
- jj_head: `45693ea8a52fe11653e7ded5c807864f1bcfabd9` (`@-`)
- score_authority: verifier
- implementer_evidence: self_check_only

Verified range, in order: `1ff5d79b` (release recipe), `15bddf38`
(previous verification record), and `45693ea8` (runtime workspace-path
repair). The working copy was clean before execution. The colocated Git
`HEAD` remains detached at `3e37a4a`; Jujutsu commits above identify the
materialized candidate contents.

## Commands

`mise run doctor`

```text
[doctor] $ bun scripts/doctor.ts
[ ok ] mise tools installed
[ ok ] nightly rustfmt
[ ok ] cargo check
[ ok ] jj repo: /Users/ryan/code/rararulab/lake/.worktrees/issue-318-historical-release-image-recipe
[ ok ] gh authenticated
[ ok ] git remote: origin
```

`rm -rf data && mise run gate` exited 0 from a cold e2e state.

```text
[hooks] $ prek run --all-files
[e2e] $ cargo run -p lake-cli -- selftest
[adbc-install] $ uv sync --project interop/adbc --frozen
[test] $ cargo test --workspace --all-targets
[test-adbc] $ cargo test -p lake-query --test adbc_interop -- --ignored --test-threads=1
[site-check] $ bun run --cwd site check
[hooks] Finished in 44.4ms
[test] Finished in 33.35s
Finished in 33.36s
```

`mise run spec-lifecycle specs/issue-318-historical-release-image-recipe.spec.md`

```text
=== Lifecycle Report (guarded) ===
Spec: historical-release-image-recipe  stage: complete  passed: true
  [PASS] Historical backfill builds immutable source with auditable current recipe
  [PASS] Split checkout preserves trusted multi-architecture publication
  [PASS] Historical recovery still rejects an untrusted release source
  [PASS] Workflow contract follows the invoking Jujutsu workspace
spec-lifecycle-guard: OK — every Test selector executed >=1 test
```

The four selectors were also invoked individually:

```text
$ mise exec -- cargo test -p lake-cli --test release_artifacts release_artifact_contract_uses_invocation_workspace -- --exact --nocapture
test release_artifact_contract_uses_invocation_workspace ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 9 filtered out

$ mise exec -- cargo test -p lake-cli --test release_artifacts release_image_workflow_separates_source_and_recipe_for_backfills -- --exact
test release_image_workflow_separates_source_and_recipe_for_backfills ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 9 filtered out

$ mise exec -- cargo test -p lake-cli --test release_artifacts release_image_workflow_is_tag_pinned_and_multiarch -- --exact
test release_image_workflow_is_tag_pinned_and_multiarch ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 9 filtered out

$ mise exec -- cargo test -p lake-cli --test release_artifacts release_image_workflow_rejects_mismatched_tags_and_preserves_digest_pinning -- --exact
test release_image_workflow_rejects_mismatched_tags_and_preserves_digest_pinning ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 9 filtered out
```

`mise exec -- actionlint .github/workflows/release-image.yml` exited 0 with
no output.

The historical-source planner was rebuilt without cache in an isolated
Jujutsu workspace at tag `v1.8.4` (`1fbf3ace`):

```text
$ docker build --no-cache --target planner --progress=plain -f <current>/Dockerfile <v1.8.4-workspace>
#11 [planner 1/2] COPY . .
#11 DONE 0.1s
#12 [planner 2/2] RUN cargo chef prepare --recipe-path recipe.json
#12 DONE 9.1s
#13 writing image sha256:e436d151dfe088a7abd5e614907d7797ff3b85db1ff29a46fc7bb63c36849064
#13 DONE 0.9s
docker-build-exit=0
```

## Four spec scenarios

1. **Historical backfill builds immutable source with auditable current
   recipe** — selector passed; the current workflow has separate
   `build-recipe` / `release-source` checkouts, Buildx uses
   `context: release-source` and `file: build-recipe/Dockerfile`, and records
   `io.rararulab.lake.build-recipe.revision`.
2. **Split checkout preserves trusted multi-architecture publication** —
   selector passed; pinned actions, `linux/amd64,linux/arm64`, cache scope,
   immutable image tags, digest summary, timeout, and non-cancelling
   concurrency remain present.
3. **Historical recovery still rejects an untrusted release source** —
   selector passed and the executable validation script was driven through
   hostile release inputs below.
4. **Workflow contract follows the invoking Jujutsu workspace** — selector
   passed under the shared project target and under a separately compiled
   target, described next.

## Runtime-workspace repair verification

The repair changes `root()` to begin at `std::env::current_dir()` and walk to
the first workspace `Cargo.toml`; it no longer embeds a compile-time checkout
path. I forced a fresh test build outside Lake's shared target:

```text
$ mise exec -- env CARGO_TARGET_DIR=/tmp/lake-issue-318-release-artifacts-target cargo test -p lake-cli --test release_artifacts --no-run --message-format=json
Finished `test` profile [unoptimized + debuginfo] target(s) in 4m 14s
/tmp/lake-issue-318-release-artifacts-target/debug/deps/release_artifacts-42f4a1e84b020bb8
```

I then invoked that newly built binary directly from a distinct temporary
directory containing only `Cargo.toml` with `[workspace]`, setting its probe
environment variable:

```text
$ (cd /tmp/lake-issue-318-runtime-root && LAKE_RELEASE_ARTIFACT_ROOT_PROBE=1 <fresh-binary> --exact release_artifact_contract_uses_invocation_workspace --nocapture)
running 1 test
test release_artifact_contract_uses_invocation_workspace ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 9 filtered out
```

Thus the assertion exercises the runtime current directory of the executing
test binary, not the checkout that compiled it. As a base control, a direct
selector invocation from a fresh `main@origin` Jujutsu workspace compiled the
base test source and produced `running 0 tests` / `0 passed; 8 filtered out`
for `release_image_workflow_separates_source_and_recipe_for_backfills`.
The current candidate executes the corresponding selector once. An
experimental base lifecycle invocation with an absolute candidate spec was
not used as transition evidence because `agent-spec` resolved its code context
alongside that external spec and returned green; direct base cargo output is
the authoritative base observation.

## Hostile probes

These probes executed the candidate's exact `Verify release source` shell
script with isolated tagged Git source and build-recipe repositories plus a
controlled `gh api` response.

```text
control=PASS
VERSION=1.8.4
RELEASE_REVISION=0e8d48f94ff96b5ffdbeba19027a50058fbfcf0f
BUILD_RECIPE_REVISION=e4b49aa62998e6c5303198b1126f2220f3f757f3

release tag must be vX.Y.Z: v1.8
malformed=PASS

manual dispatch requires an already-published release for v1.8.4
draft=PASS

checked-out tag does not match the trusted release revision
mismatch=PASS
```

The control exported both distinct immutable revisions. A malformed tag, a
draft release, and a published release whose target SHA differed from the
checked-out tag each failed before any Buildx step could run.

## Transition matrix

- fail_to_pass: observed. In direct `main@origin` testing, the historical
  recipe selector resolved to zero tests; the current selector executes one
  passing test. The new runtime-workspace selector is also absent from base
  and executes once at `45693ea8`.
- pass_to_fail: 0. Full gate, all four guarded scenarios, independent
  selectors, actionlint, uncached historical planner, and probes passed.

## Previous-report repair conclusion

The previous report is superseded by this one. At `45693ea8`, the release
artifact contract no longer binds filesystem reads to a Cargo compile-time
workspace. It was freshly compiled outside the shared target and passed when
run from a different runtime workspace, while the standard shared-target
spec lifecycle remains green. The stale-binary / wrong-workspace verification
failure is therefore repaired.

## Verdict

PASS — current JJ head `45693ea8` independently passes all four spec
scenarios, the clean quality gate, actionlint, historical `v1.8.4` planner,
runtime workspace isolation check, and release-source trust probes.
