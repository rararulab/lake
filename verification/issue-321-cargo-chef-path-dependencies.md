# Verification Report — issue #321 cargo-chef path dependencies

## Result

**PASS** — the candidate hydrates the sole historical path dependency before
`cargo chef cook`, permits only the recipe plus that exact dependency as
planner-to-builder cook inputs, preserves the dependency-cache boundary, and
a native Docker builder build using the `v1.8.4` source tree completes
successfully.

| Field | Value |
| --- | --- |
| Score authority | `verifier` |
| Implementer evidence | `self_check_only` |
| Lane | 1 — `specs/issue-321-cargo-chef-path-dependencies.spec.md` |
| Git base SHA | `3e37a4a324d986b813479d8ece9af884cc20866e` |
| Git head SHA | `3e37a4a324d986b813479d8ece9af884cc20866e` |
| JJ content base | `b47110ed18e7e889b40cd8cee1dc577c69f0b4d5` (`main@origin`) |
| JJ content head | `45643a6df80d99be522e592ac180a713c4d7a30a` (`test(release): lock cargo-chef cache inputs (#321)`) |

The Git SHA pair is the required raw result of `git merge-base HEAD origin/main`
and `git rev-parse HEAD` in this colocated JJ checkout. It does not identify
the candidate content: the independently inspected JJ range
`main@origin..@-` ends at `45643a6d` and contains the original Dockerfile fix,
two verification-report commits, and the final contract-test repair.

## Candidate and environment

Before verification, `jj status` reported `The working copy has no changes`.
The required environment check passed:

```text
$ mise run doctor
[ ok ] mise tools installed
[ ok ] nightly rustfmt
[ ok ] cargo check
[ ok ] jj repo: /Users/ryan/code/rararulab/lake/.worktrees/issue-321-cargo-chef-path-dependencies
[ ok ] gh authenticated
[ ok ] git remote: origin
```

The inspected Dockerfile has this sequence in the builder stage:

```dockerfile
COPY --from=planner /src/recipe.json recipe.json
COPY --from=planner /src/third_party/datafusion-execution third_party/datafusion-execution
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
```

Thus the exact dependency is present before cooking, while application source
copy remains after the cook cache layer.

At the final repair head, a direct scan of the builder pre-cook interval found
exactly these two planner transfers and no others:

```text
$ rg -n '^COPY --from=planner ' Dockerfile
24:COPY --from=planner /src/recipe.json recipe.json
25:COPY --from=planner /src/third_party/datafusion-execution third_party/datafusion-execution
```

## Mandatory gates

All gate commands below were run independently by the verifier.  `data/` was
removed immediately before the full gate so the self-test used a cold local
state.

```text
$ rm -rf data && mise run gate
[hooks] $ prek run --all-files
[test] $ cargo test --workspace --all-targets
[e2e] $ cargo run -p lake-cli -- selftest
[site-install] $ bun install --cwd site --frozen-lockfile
[adbc-install] $ uv sync --project interop/adbc --frozen
[hooks] Finished in 66.7ms
[test] Finished in 34.32s
Finished in 34.33s
exit=0

$ mise run spec-lifecycle specs/issue-321-cargo-chef-path-dependencies.spec.md
=== Lifecycle Report (guarded) ===
Spec: cargo-chef-path-dependencies stage complete passed true
[PASS] Cargo-chef receives historical path dependency before cooking
[PASS] Path hydration retains dependency-cache boundary
[PASS] Missing path input is rejected before release build
spec-lifecycle-guard: OK — every Test selector executed >=1 test
exit=0
```

Every acceptance selector was then invoked exactly, including the deliberate
repeat in the specification:

```text
$ mise exec -- cargo test -p lake-cli --test release_artifacts release_image_hydrates_path_dependencies_before_cargo_chef_cook -- --exact
running 1 test
test release_image_hydrates_path_dependencies_before_cargo_chef_cook ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 11 filtered out
exit=0

$ mise exec -- cargo test -p lake-cli --test release_artifacts release_image_caches_rust_dependencies_before_copying_application_sources -- --exact
running 1 test
test release_image_caches_rust_dependencies_before_copying_application_sources ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 11 filtered out
exit=0

$ mise exec -- cargo test -p lake-cli --test release_artifacts release_image_hydrates_path_dependencies_before_cargo_chef_cook -- --exact
running 1 test
test release_image_hydrates_path_dependencies_before_cargo_chef_cook ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 11 filtered out
exit=0
```

There is no altered query, SQL, storage, or RPC runtime surface in this
change.  The full cold-state self-test covered the project runtime surface;
the direct Docker build below covers the changed release-build surface.

## Repair re-verification — `45643a6d`

The latest candidate commit changes only the release-artifact contract test:

```text
$ jj show --summary -r 45643a6d
M crates/lake-cli/tests/release_artifacts.rs
```

The Dockerfile is unchanged from the historical-builder proof above, so I did
not repeat its 16-minute native build. Instead, I directly checked the newly
added negative boundary and then re-ran the restored candidate's selector,
spec lifecycle, and cold-state full gate.

For the negative probe, I temporarily inserted the following third planner
transfer after the exact path transfer and before `cargo chef cook`; the
Dockerfile was immediately restored before all positive checks:

```dockerfile
COPY --from=planner /src/third_party third_party
```

The exact selector failed as required, demonstrating that the repair rejects a
broad copy even when the required exact copy remains present:

```text
$ mise exec -- cargo test -p lake-cli --test release_artifacts release_image_hydrates_path_dependencies_before_cargo_chef_cook -- --exact
running 1 test
test release_image_hydrates_path_dependencies_before_cargo_chef_cook ... FAILED
assertion `left == right` failed: the builder must receive only the recipe and its exact path dependency before cooking
  left: ["COPY --from=planner /src/recipe.json recipe.json", "COPY --from=planner /src/third_party/datafusion-execution third_party/datafusion-execution", "COPY --from=planner /src/third_party third_party"]
 right: ["COPY --from=planner /src/recipe.json recipe.json", "COPY --from=planner /src/third_party/datafusion-execution third_party/datafusion-execution"]
broad-planner-copy-probe-exit=101
```

After restoring the two-line Dockerfile interval, the focused selector and the
full Lane-1 lifecycle passed:

```text
$ mise exec -- cargo test -p lake-cli --test release_artifacts release_image_hydrates_path_dependencies_before_cargo_chef_cook -- --exact
running 1 test
test release_image_hydrates_path_dependencies_before_cargo_chef_cook ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 11 filtered out
exit=0

$ mise run spec-lifecycle specs/issue-321-cargo-chef-path-dependencies.spec.md
=== Lifecycle Report (guarded) ===
Spec: cargo-chef-path-dependencies  stage: complete  passed: true
  [PASS] Cargo-chef receives the historical path dependency before cooking
  [PASS] Path hydration retains the dependency-cache boundary
  [PASS] Missing path input is rejected before the release build
spec-lifecycle-guard: OK — every Test selector executed >=1 test
exit=0
```

The restored candidate also passed a new cold-state full gate:

```text
$ rm -rf data && mise run gate
[hooks] $ prek run --all-files
[test] $ cargo test --workspace --all-targets
[e2e] $ cargo run -p lake-cli -- selftest
[adbc-install] $ uv sync --project interop/adbc --frozen
[site-install] $ bun install --cwd site --frozen-lockfile
[test] Finished in 33.41s
Finished in 33.42s
exit=0
```

## Historical-source end-to-end proof

I created a separate, clean JJ workspace at the `v1.8.4` tag
(`1fbf3acee3e8994c6df433516a307cd381fb3c03`) and used the **candidate
Dockerfile** with that historical source as build context.  This runs the
actual builder stage (and therefore `cargo chef cook`), not merely the planner:

```text
$ docker build --no-cache --target builder --progress=plain \
    -f <candidate>/Dockerfile <clean-v1.8.4-workspace>
#17 [builder 3/4] RUN cargo chef cook --release --recipe-path recipe.json
#17 DONE
#18 [builder 4/4] RUN cargo build --locked --release --package lake-cli --bin lake ...
#18 983.9 Finished `release` profile [optimized] target(s) in 16m 13s
#18 exporting to image
#18 writing image sha256:d98e47d1eeb357b961ca33be9e02f8cf52d59dcdb11251b4b663269202b0d991 done
docker-builder-exit=0
```

## Transition matrix

| Contract | Base (`main@origin`) | Candidate | Verdict |
| --- | --- | --- | --- |
| Exact pre-cook transfer of `third_party/datafusion-execution` | Absent from Dockerfile; no regression selector source | Present before `cargo chef cook` | fail → pass |
| Selector `release_image_hydrates_path_dependencies_before_cargo_chef_cook` | Fresh base test invocation reported `running 0 tests`, `11 filtered out`, process exit 0 | Runs exactly 1 test and passes | fail → pass (zero-match is rejected by lifecycle guard) |
| Only recipe + exact path dependency may cross planner → builder before cook | No allow-list contract | Broad `third_party` probe fails with a three-item actual list against the two-item allow-list | regression guard added and demonstrated |
| Cook cache boundary before `COPY . .` | Not protected by this new contract | Exact selector passes; order retained | pass |
| Existing mandatory gate | Not re-scored | Green on original candidate and again at repair head | no pass → fail |

The base command was deliberately recorded rather than inferred:

```text
$ (cd <clean-main@origin-workspace> && mise exec -- cargo test -p lake-cli --test release_artifacts release_image_hydrates_path_dependencies_before_cargo_chef_cook -- --exact)
running 0 tests
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 11 filtered out
base-selector-exit=0
```

Cargo itself treats a zero-match filter as successful.  The Lane-1
`spec-lifecycle-guard` explicitly rejects that vacuous result; candidate
execution above demonstrates the required 1-test match.  This is the
relevant fail-to-pass transition.  No existing required contract regressed:
**pass-to-fail count: 0**.

## Hostile probes

Both probes used the unchanged clean `v1.8.4` source context and a temporary
copy of the candidate Dockerfile.  They were expected to fail and did fail.

| Probe | Mutation | Observed result | Assessment |
| --- | --- | --- | --- |
| Missing hydration | Remove only `COPY --from=planner /src/third_party/datafusion-execution third_party/datafusion-execution` | `cargo chef cook` exits 101: cannot read `/src/third_party/datafusion-execution/Cargo.toml`; Docker exits 1 | PASS — the path input is necessary before cook |
| Wrong source path | Replace the planner source with `/src/third_party/not-datafusion-execution` | Docker `COPY` fails: source not found; Docker exits 1 | PASS — the transfer is exact, not broad or accidental |
| Broad planner transfer (`45643a6d`) | Add `COPY --from=planner /src/third_party third_party` while retaining the exact transfer | Static selector exits 101 and reports three actual planner copies versus its two-copy allow-list | PASS — broad cache inputs are explicitly rejected |

Raw terminal endings:

```text
$ docker build --target builder -f <Dockerfile-with-transfer-removed> <clean-v1.8.4-workspace>
error: failed to load source for dependency `datafusion-execution`
Caused by: failed to read `/src/third_party/datafusion-execution/Cargo.toml`
ERROR: process "/bin/sh -c cargo chef cook --release --recipe-path recipe.json" did not complete successfully: exit code: 101
missing-path-exit=1

$ docker build --target builder -f <Dockerfile-with-wrong-source> <clean-v1.8.4-workspace>
ERROR: failed to compute cache key: "/src/third_party/not-datafusion-execution": not found
wrong-path-exit=1
```

## Scope and cleanup

The candidate changes only the Docker recipe, its contract test, specification,
and supporting documentation; the final repair changes only the contract test.
It does not introduce release authority, credentials, tags, or runtime
behavior. Verification created no candidate code changes. Temporary historical
and base JJ workspaces, Docker probe files, logs, and the successful temporary
image were removed after evidence capture; the temporary gate logs from the
repair re-check were also removed.

## Verdict

PASS. The regression is covered at the static contract boundary and by a
native, no-cache `v1.8.4` Docker builder build that actually executes
`cargo chef cook`; the latest repair additionally proves the contract rejects
a broad planner transfer. Mandatory project gates and every spec selector are
green at the repair head.
