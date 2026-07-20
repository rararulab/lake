# Verification report — issue #308

- base_sha: 3e37a4a324d986b813479d8ece9af884cc20866e
- head_sha: ec5454e45b667fa17d44f1654b030961609471a2
- score_authority: verifier
- implementer_evidence: self_check_only
- lane: 1
- spec: `specs/issue-308-episode-manifest-v1.spec.md`

The jj workspace has an empty working-copy commit `77e619b7` whose parent is
the candidate above; `jj diff --from @- --to @` reported zero files changed, so
the tested tree is exactly `ec5454e4`. This jj workspace is an ignored
subdirectory of the colocated Git checkout, so plain `git rev-parse HEAD` from
inside it resolves the parent checkout's `main` pointer rather than the jj
candidate. The pinned SHAs therefore come from `jj @-` plus an explicit
`git -C /Users/ryan/code/rararulab/lake merge-base <candidate> origin/main`.

No implementer hand-off or prior report was read. The pre-existing report was
replaced with evidence gathered in this verification run.

## Commands

### Environment and clean candidate state

`mise run doctor`

```text
[doctor] $ bun scripts/doctor.ts
[ ok ] mise tools installed
[ ok ] nightly rustfmt
[ ok ] cargo check
[ ok ] jj repo: /Users/ryan/code/rararulab/lake/.worktrees/issue-308-episode-manifest-v1
[ ok ] gh authenticated
[ ok ] git remote: origin
```

`jj st`

```text
The working copy has no changes.
Working copy  (@) : rnwsssxw 77e619b7 (empty) (no description set)
Parent commit (@-): mstprsrt ec5454e4 feat(robotics): define EpisodeManifest v1 contract (#308)
```

`jj diff --from @- --to @ --stat`

```text
0 files changed, 0 insertions(+), 0 deletions(-)
```

`jj log -r @- --no-graph -T 'commit_id ++ "\n"'`

```text
ec5454e45b667fa17d44f1654b030961609471a2
```

`git -C /Users/ryan/code/rararulab/lake merge-base ec5454e45b667fa17d44f1654b030961609471a2 origin/main`

```text
3e37a4a324d986b813479d8ece9af884cc20866e
```

The candidate diff contained only allowed paths:

```text
Cargo.lock
crates/lake-common/AGENT.md
crates/lake-common/Cargo.toml
crates/lake-common/src/episode_manifest.rs
crates/lake-common/src/episode_manifest_tests.rs
crates/lake-common/src/lib.rs
docs/architecture.md
docs/design/robot-training-lakehouse.md
specs/issue-308-episode-manifest-v1.spec.md
```

### Full quality gate from cold state

Before the gate, the workspace path was checked exactly, `lsof +D` found no
process using its `data/`, and that directory was removed.

```text
data_active_processes=0
removed_stale_data=yes
```

`mise run gate`

```text
exit_code: 0
[hooks] $ prek run --all-files
[test] $ cargo test --workspace --all-targets
[e2e] $ cargo run -p lake-cli -- selftest
[hooks] cargo fmt............................................(no files to check)Skipped
[hooks] cargo clippy.........................................(no files to check)Skipped
[hooks] Finished in 67.6ms
[e2e] created table robots.episodes
[e2e] committed robots.episodes at v2
[e2e] +----------+----------+------------+
[e2e] | robot_id | episodes | avg_reward |
[e2e] +----------+----------+------------+
[e2e] | alpha    | 2        | 0.8        |
[e2e] | beta     | 1        | 0.4        |
[e2e] +----------+----------+------------+
[e2e] self-check ok
[test] test result: ok. 62 passed; 0 failed; 3 ignored; 0 measured; 0 filtered out; finished in 11.63s
[test] Finished in 34.34s
Finished in 34.35s
```

Because `prek` inherited the parent Git checkout's subdirectory pathspec, both
hooks matched zero tracked paths. The exact hook commands were therefore run
directly against the candidate workspace instead of accepting that false-green
portion of the wrapper.

`cargo +nightly fmt --all -- --check`

```text
exit_code: 0
(no stdout)
```

`cargo clippy --workspace --all-targets --all-features --no-deps -- -D warnings`

```text
exit_code: 0
    Checking lake-common v1.8.4 (/Users/ryan/code/rararulab/lake/.worktrees/issue-308-episode-manifest-v1/crates/lake-common)
    Checking lake-cli v1.8.4 (/Users/ryan/code/rararulab/lake/.worktrees/issue-308-episode-manifest-v1/crates/lake-cli)
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 2m 19s
```

### Lane-1 lifecycle and bound selectors

`mise run spec-lifecycle specs/issue-308-episode-manifest-v1.spec.md`

```text
[spec-lifecycle] $ bun scripts/spec-lifecycle-guard.ts "${usage_spec?}"
=== Lifecycle Report (guarded) ===
Spec: episode-manifest-v1  stage: complete  passed: true
  [PASS] v1 manifest canonically round-trips two recording representations
  [PASS] binding derives the table summary and proves complete reachability
  [PASS] missing, extra, or mismatched ArtifactRefs fail closed
  [PASS] corrupt, future, and non-canonical manifest wires are rejected
spec-lifecycle-guard: OK — every Test selector executed >=1 test
```

`cargo test -p lake-common episode_manifest_v1_roundtrips_two_recording_formats`

```text
running 1 test
test episode_manifest_tests::episode_manifest_v1_roundtrips_two_recording_formats ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 8 filtered out; finished in 0.00s
```

`cargo test -p lake-common episode_manifest_v1_binds_complete_artifact_refs`

```text
running 1 test
test episode_manifest_tests::episode_manifest_v1_binds_complete_artifact_refs ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 8 filtered out; finished in 0.00s
```

`cargo test -p lake-common episode_manifest_v1_rejects_artifact_binding_mismatch`

```text
running 1 test
test episode_manifest_tests::episode_manifest_v1_rejects_artifact_binding_mismatch ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 8 filtered out; finished in 0.00s
```

`cargo test -p lake-common episode_manifest_v1_rejects_invalid_wire`

```text
running 1 test
test episode_manifest_tests::episode_manifest_v1_rejects_invalid_wire ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 8 filtered out; finished in 0.00s
```

### Base transition evidence

The base was exported from the main Git root to an isolated `mktemp` directory;
all four candidate selectors matched zero tests. Under Lake's
`spec-lifecycle-guard`, zero matches are an acceptance failure even though
Cargo itself exits zero.

The first archive attempt was invoked from the ignored jj subdirectory, so Git
implicitly filtered for `.worktrees/issue-308-episode-manifest-v1/**` and the
temporary archive had no `Cargo.toml`:

```text
error: manifest path `/var/folders/qk/93970_h952g3pmjljflsljt40000gn/T//lake-308-base.JtA66j/Cargo.toml` does not exist
```

It was discarded as setup evidence and rerun correctly with
`git -C /Users/ryan/code/rararulab/lake archive --format=tar 3e37a4a324d986b813479d8ece9af884cc20866e`.
Each of these base commands was then run with `--locked`:

```text
cargo test --manifest-path <base>/Cargo.toml --locked -p lake-common episode_manifest_v1_roundtrips_two_recording_formats
cargo test --manifest-path <base>/Cargo.toml --locked -p lake-common episode_manifest_v1_binds_complete_artifact_refs
cargo test --manifest-path <base>/Cargo.toml --locked -p lake-common episode_manifest_v1_rejects_artifact_binding_mismatch
cargo test --manifest-path <base>/Cargo.toml --locked -p lake-common episode_manifest_v1_rejects_invalid_wire
```

Raw result for each selector:

```text
running 0 tests
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 5 filtered out; finished in 0.00s
```

### Feature-specific runtime verification

N/A. This issue adds an I/O-free `lake-common` value/wire contract and
explicitly adds no CLI, server, Query, Metasrv, SDK, storage-engine, or other
application surface. There is therefore no candidate application endpoint to
boot and drive for EpisodeManifest itself. Inventing a CLI flow would exceed
the spec. The cold `mise run gate` e2e above still booted the existing real
system from a deleted `data/` directory and passed as a regression check, but
it is not claimed as feature-specific EpisodeManifest coverage.

### Hostile probes

A temporary integration test was added only for execution, run with the command
below, and deleted before the final clean-state check. It was never committed
and made no implementation change.

`cargo test -p lake-common --test issue_308_verifier_hostile -- --nocapture`

```text
running 3 tests
future_version_observed=UnsupportedVersion { version: 65535, supported: 1 }
duplicate_field_observed=Json { source: Error("duplicate field `format_version`", line: 1, column: 36) }
test wire_rejects_max_future_version_and_duplicate_top_level_field ... ok
balanced_multiset_observed=ArtifactBindingMismatch { expected: 3, observed: 3 }
test balanced_multiset_substitution_fails_even_when_ref_count_matches ... ok
stale_digest_observed=ManifestObjectMismatch { field: "sha256" }
test equal_length_stale_manifest_digest_hits_sha256_check ... ok
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
```

## Transition matrix

- fail_to_pass:
  - `episode_manifest_v1_roundtrips_two_recording_formats`: expected base
    selector absent / guarded acceptance failure, head one passing test;
    observed base `0 passed, 5 filtered out`, head `1 passed, 0 failed`.
  - `episode_manifest_v1_binds_complete_artifact_refs`: expected base selector
    absent / guarded acceptance failure, head one passing test; observed base
    `0 passed, 5 filtered out`, head `1 passed, 0 failed`.
  - `episode_manifest_v1_rejects_artifact_binding_mismatch`: expected base
    selector absent / guarded acceptance failure, head one passing test;
    observed base `0 passed, 5 filtered out`, head `1 passed, 0 failed`.
  - `episode_manifest_v1_rejects_invalid_wire`: expected base selector absent /
    guarded acceptance failure, head one passing test; observed base `0 passed,
    5 filtered out`, head `1 passed, 0 failed`.
- pass_to_fail: 0; full workspace gate, direct fmt, direct clippy, lifecycle,
  all bound selectors, and all hostile probes completed with exit code 0.

## Probes

1. Future/noncanonical wire matrix
   - Input: canonical CJK-bearing manifest changed to `format_version=65535`;
     separately, syntactically valid JSON with a duplicate top-level
     `format_version` field.
   - Expected: typed unsupported-version error for the former and strict JSON
     decode error for the latter.
   - Observed: `UnsupportedVersion { version: 65535, supported: 1 }` and JSON
     `duplicate field 'format_version'`.
   - Result: PASS.
2. Balanced ArtifactRef multiset substitution
   - Input: exact three non-manifest refs changed by removing `artifact-c` and
     duplicating `artifact-a`, preserving total observed count at three.
   - Expected: rejection based on multiset identity/multiplicity, not a length
     comparison.
   - Observed: `ArtifactBindingMismatch { expected: 3, observed: 3 }`.
   - Result: PASS.
3. Equal-length stale manifest digest
   - Input: manifest ArtifactRef with correct media type and byte length, but
     SHA-256 computed from a one-byte-mutated equal-length payload.
   - Expected: the isolated digest check rejects it.
   - Observed: `ManifestObjectMismatch { field: "sha256" }`.
   - Result: PASS.

## Issues

None in the candidate. The jj-subdirectory Git pathspec behavior caused the
wrapper's fmt/clippy hooks to skip and the first base archive attempt to be
empty; both were explicitly corrected with direct commands and a Git-root
archive before scoring.

## Verdict

PASS — the clean candidate gate, direct static gates, lane-1 lifecycle, all
bound selectors, observed base-to-head transitions, and three hostile probes
all meet the contract with zero regressions.
