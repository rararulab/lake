# Verification report — issue #318 (current candidate)

- jj_base: `3729455699c7d9ed28b7b57263ab8abf5a283a50` (`main@origin`)
- jj_head: `36997ca8ac86e0cb26be2072e78a42243e32e6a9` (`@-`)
- score_authority: verifier
- implementer_evidence: self_check_only

The committed candidate was clean before verification. Its range includes the
release-recipe change, the runtime-workspace repair, and the target-isolation
repair. Colocated Git remains detached at `3e37a4a`; the Jujutsu revisions
above identify the materialized candidate.

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

`mise run spec-lint specs/issue-318-historical-release-image-recipe.spec.md`

```text
Spec: historical-release-image-recipe
Quality: 100% (determinism: 100%, testability: 100%, coverage: 100%)
[WARN ] line 7: [output-mode-coverage] spec mentions output behavior but missing explicit scenario coverage for mode(s): file-output
[INFO ] line 58: [bdd-rule-grouping] 5 scenarios with no Rule grouping
```

`mise exec -- actionlint .github/workflows/release-image.yml` exited 0 with no
output.

`rm -rf data && mise run gate` exited 0 from cold e2e state:

```text
[hooks] $ prek run --all-files
[test] $ cargo test --workspace --all-targets
[e2e] $ cargo run -p lake-cli -- selftest
[adbc-install] $ uv sync --project interop/adbc --frozen
[test-adbc] $ cargo test -p lake-query --test adbc_interop -- --ignored --test-threads=1
[site-check] $ bun run --cwd site check
[hooks] Finished in 83.1ms
[test] Finished in 36.30s
Finished in 36.30s
```

## Default-environment lifecycle and target isolation

This was run directly from the candidate workspace, without overriding
`CARGO_TARGET_DIR` or any other environment value:

```text
$ mise run spec-lifecycle specs/issue-318-historical-release-image-recipe.spec.md
=== Lifecycle Report (guarded) ===
Spec: historical-release-image-recipe  stage: complete  passed: true
  [PASS] Historical backfill builds immutable source with auditable current recipe
  [PASS] Split checkout preserves trusted multi-architecture publication
  [PASS] Historical recovery still rejects an untrusted release source
  [PASS] Workflow contract follows the invoking Jujutsu workspace
  [PASS] Cargo target cache is isolated by Jujutsu workspace
spec-lifecycle-guard: OK — every Test selector executed >=1 test
```

Unmodified `mise env` resolved different target directories in the candidate
and root checkout:

```text
# candidate workspace
export CARGO_NET_RETRY=10
export CARGO_TARGET_DIR=/Users/ryan/Library/Caches/lake/target/a8b983c672dedc8e239a2e00ce66e430078358faa29e7c60e8e4b736fa47b221
export CARGO_TERM_COLOR=always

# root checkout (/Users/ryan/code/rararulab/lake)
export CARGO_NET_RETRY=10
export CARGO_TARGET_DIR=/Users/ryan/Library/Caches/lake/target
export CARGO_TERM_COLOR=always
```

The candidate's actual lifecycle selector binary and cold-gate e2e binary were
both loaded from the hashed target:

```text
/Users/ryan/Library/Caches/lake/target/a8b983c672dedc8e239a2e00ce66e430078358faa29e7c60e8e4b736fa47b221/debug/deps/release_artifacts-42f4a1e84b020bb8
/Users/ryan/Library/Caches/lake/target/a8b983c672dedc8e239a2e00ce66e430078358faa29e7c60e8e4b736fa47b221/debug/lake selftest
```

The five selectors also passed independently under this same default mise
environment:

```text
$ mise exec -- cargo test -p lake-cli --test release_artifacts release_image_workflow_separates_source_and_recipe_for_backfills -- --exact
test release_image_workflow_separates_source_and_recipe_for_backfills ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 10 filtered out

$ mise exec -- cargo test -p lake-cli --test release_artifacts release_image_workflow_is_tag_pinned_and_multiarch -- --exact
test release_image_workflow_is_tag_pinned_and_multiarch ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 10 filtered out

$ mise exec -- cargo test -p lake-cli --test release_artifacts release_image_workflow_rejects_mismatched_tags_and_preserves_digest_pinning -- --exact
test release_image_workflow_rejects_mismatched_tags_and_preserves_digest_pinning ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 10 filtered out

$ mise exec -- cargo test -p lake-cli --test release_artifacts release_artifact_contract_uses_invocation_workspace -- --exact --nocapture
test release_artifact_contract_uses_invocation_workspace ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 10 filtered out

$ mise exec -- cargo test -p lake-cli --test release_artifacts mise_target_directory_is_workspace_isolated -- --exact
test mise_target_directory_is_workspace_isolated ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 10 filtered out
```

The fifth contract verifies the configured template is exactly
`{{xdg_cache_home}}/lake/target/{{config_root | hash}}`. The observed,
different resolved paths show this is not merely a source-level assertion:
normal lane-1 execution cannot select the root checkout's global-target test
binary.

## Five scenarios

1. Historical backfill builds immutable source with auditable current recipe:
   passing selector confirms separate `build-recipe` and `release-source`,
   source build context, current recipe Dockerfile, and distinct recipe label.
2. Split checkout preserves trusted multi-architecture publication: passing
   selector retains SHA-pinned actions, amd64/arm64, cache scope, tags, digest,
   timeout, and non-cancelling concurrency.
3. Historical recovery rejects untrusted release source: passing selector and
   hostile script probes reject malformed, draft, and mismatched releases.
4. Workflow contract follows the invoking Jujutsu workspace: passing selector
   resolves contract files from runtime `current_dir()`.
5. Cargo target cache is isolated by Jujutsu workspace: passing selector plus
   direct `mise env` evidence proves the hashed candidate target is distinct.

## Historical planner and hostile probes

An isolated Jujutsu workspace at tag `v1.8.4` (`1fbf3ace`) was built using the
candidate Dockerfile with cache disabled:

```text
$ docker build --no-cache --target planner --progress=plain -f <candidate>/Dockerfile <v1.8.4-workspace>
#11 [planner 1/2] COPY . .
#11 DONE 0.3s
#12 [planner 2/2] RUN cargo chef prepare --recipe-path recipe.json
#12 DONE 11.5s
#13 writing image sha256:8f267a1749c6d1c843896f885dca8fb4c9a20a71b2a902d742513f8c780d4ea4
#13 DONE 2.6s
docker-build-exit=0
```

The candidate's exact `Verify release source` shell script was run against
isolated tagged Git fixtures with controlled `gh api` output:

```text
control=PASS
VERSION=1.8.4
RELEASE_REVISION=d449b7ea2c9a6c41b3b7c024f9a3ffdfc90b8e0f
BUILD_RECIPE_REVISION=07721ed0620359c1595df20bbb3e3a7074e842c8
release tag must be vX.Y.Z: v1.8
malformed=PASS
manual dispatch requires an already-published release for v1.8.4
draft=PASS
checked-out tag does not match the trusted release revision
mismatch=PASS
```

The valid control exported separate immutable source and recipe revisions.
Malformed tag, draft release, and target-SHA mismatch each rejected before a
Buildx step could execute.

## Transition matrix

- fail_to_pass: observed. The former global target allowed a lifecycle run to
  reuse an executable from another checkout. At `36997ca8`, default mise
  resolution uses the candidate's `config_root` hash and the original
  unoverridden lifecycle executes all five selectors from that target.
- pass_to_fail: 0. Gate, spec lint, unoverridden lifecycle, five selectors,
  actionlint, historical planner, and hostile probes passed.

## Prior-report repair conclusion

The previous report is superseded. `45693ea8` stopped a test binary from
reading its compiler workspace; `36997ca8` also stops Cargo from selecting a
binary built by another Jujutsu workspace. The prior lifecycle false-green is
therefore closed in normal, unmodified mise execution.

## Verdict

PASS — current JJ head `36997ca8` independently passes the cold quality gate,
five lane-1 scenarios, spec lint, actionlint, default target isolation,
historical `v1.8.4` planner, and release-source hostile probes.
