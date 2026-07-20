# Verification report — issue #308 (final rebase audit)

- base_sha: 98e1667e7afc39279715e2001a20ea0e029970e1
- head_sha: 6aec163b249f27af08988bef02d222265802b93d
- pre_rebase_head: 39d4ebf645689e6af7c6e8e5dc62e8a8d917060b
- previous_review_head: 5b8167481ff9f49b68c70365c5b3c60942a6564f
- score_authority: verifier
- implementer_evidence: self_check_only
- lane: 1
- spec: `specs/issue-308-episode-manifest-v1.spec.md`

This report scores the exact post-rebase candidate above. No earlier PASS was
carried forward: the full gate, static checks, lane-1 lifecycle, every bound
selector, base selector transition, and hostile probes were rerun after the
rebase onto main issue #312.

The jj workspace has an uncommitted verifier-only change to this report.
The product candidate is its parent `@-`. Plain Git HEAD in this ignored jj
subdirectory points at the parent checkout, so both candidate and merge-base
were resolved with the explicit candidate commit ID.

## Commands

### Pinned post-rebase state

`jj log -r @- --no-graph -T 'commit_id ++ "\n"'`

```text
6aec163b249f27af08988bef02d222265802b93d
```

`git -C /Users/ryan/code/rararulab/lake merge-base 6aec163b249f27af08988bef02d222265802b93d origin/main`

```text
98e1667e7afc39279715e2001a20ea0e029970e1
```

`jj status` before verification:

```text
Working copy changes:
M verification/report.md
Working copy  (@) : ytkrzwyt 79a53d14 (no description set)
Parent commit (@-): xvnurqqz 6aec163b fix(robotics): enforce canonical manifest identity (#308)
```

`jj diff --summary` contained only:

```text
M verification/report.md
```

The exact workspace `data/` directory had no active `lsof +D` result and
was removed before the gate.

### Full quality gate from cold state

`mise run gate`

```text
exit_code: 0
[hooks] $ prek run --all-files
[test] $ cargo test --workspace --all-targets
[e2e] $ cargo run -p lake-cli -- selftest
[hooks] cargo fmt............................................(no files to check)Skipped
[hooks] cargo clippy.........................................(no files to check)Skipped
[hooks] Finished in 64.0ms
[test-adbc] test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out; finished in 1.71s
[site-check] Result (24 files):
[site-check] - 0 errors
[site-check] - 0 warnings
[site-check] - 0 hints
[e2e] created table robots.episodes
[e2e] committed robots.episodes at v2
[e2e] +----------+----------+------------+
[e2e] | robot_id | episodes | avg_reward |
[e2e] +----------+----------+------------+
[e2e] | alpha    | 2        | 0.8        |
[e2e] | beta     | 1        | 0.4        |
[e2e] +----------+----------+------------+
[e2e] self-check ok
[test] test result: ok. 62 passed; 0 failed; 3 ignored; 0 measured; 0 filtered out; finished in 11.74s
[test] Finished in 35.73s
Finished in 35.73s
```

The prek hooks inherited the jj-subdirectory Git pathspec and matched no files,
so they were not accepted as fmt/clippy evidence. Both commands were run
directly:

`cargo +nightly fmt --all -- --check`

```text
exit_code: 0
(no stdout)
```

`cargo clippy --workspace --all-targets --all-features --no-deps -- -D warnings`

```text
exit_code: 0
    Checking lake-cli v1.8.4 (/Users/ryan/code/rararulab/lake/.worktrees/issue-308-episode-manifest-v1/crates/lake-cli)
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.86s
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
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 10 filtered out; finished in 0.00s
```

`cargo test -p lake-common episode_manifest_v1_binds_complete_artifact_refs`

```text
running 1 test
test episode_manifest_tests::episode_manifest_v1_binds_complete_artifact_refs ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 10 filtered out; finished in 0.00s
```

`cargo test -p lake-common episode_manifest_v1_rejects_artifact_binding_mismatch`

```text
running 1 test
test episode_manifest_tests::episode_manifest_v1_rejects_artifact_binding_mismatch ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 10 filtered out; finished in 0.00s
```

`cargo test -p lake-common episode_manifest_v1_rejects_invalid_wire`

```text
running 3 tests
test episode_manifest_tests::episode_manifest_v1_rejects_invalid_wire_duplicate_artifact_identity ... ok
test episode_manifest_tests::episode_manifest_v1_rejects_invalid_wire ... ok
test episode_manifest_tests::episode_manifest_v1_rejects_invalid_wire_noncanonical_json_bytes ... ok
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 8 filtered out; finished in 0.00s
```

### New-base acceptance transition

Base `98e1667e7afc39279715e2001a20ea0e029970e1` was exported from the
Git root into an isolated `mktemp` tree. Each selector was run with
`--locked`:

```text
cargo test --manifest-path <base>/Cargo.toml --locked -p lake-common episode_manifest_v1_roundtrips_two_recording_formats
cargo test --manifest-path <base>/Cargo.toml --locked -p lake-common episode_manifest_v1_binds_complete_artifact_refs
cargo test --manifest-path <base>/Cargo.toml --locked -p lake-common episode_manifest_v1_rejects_artifact_binding_mismatch
cargo test --manifest-path <base>/Cargo.toml --locked -p lake-common episode_manifest_v1_rejects_invalid_wire
```

Every selector produced:

```text
running 0 tests
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 5 filtered out; finished in 0.00s
```

Under the spec-lifecycle zero-match guard this is an acceptance failure.
The isolated base tree was deleted.

### Post-rebase hostile probes

An isolated temporary binary crate depended on the pinned candidate's public
`lake-common` API. It used CJK identities and created its own manifest and
ArtifactRefs. No candidate implementation or test was edited.

`cargo run --manifest-path /tmp/lake-308-rebase-probe/Cargo.toml --quiet`

```text
pretty=Err(NonCanonical)
reordered=Err(NonCanonical)
trailing=Err(NonCanonical)
equivalent_number=Err(NonCanonical)
duplicate_identity=Err(DuplicateIdentity { kind: "Artifact binding", identity: "对象-1" })
canonical_digest_exact_multiset=Ok(episode=剧集-001, refs=2)
```

The temporary probe tree was deleted after the run.

### Reviewer P1 transition anchor

Before the rebase, this same verifier exported old review head
`5b8167481ff9f49b68c70365c5b3c60942a6564f`, copied only the repaired
regression tests over its old implementation, and observed both tests fail:

```text
episode_manifest_v1_rejects_invalid_wire_noncanonical_json_bytes:
test result: FAILED. 0 passed; 1 failed; 10 filtered out
exit_code: 101

episode_manifest_v1_rejects_invalid_wire_duplicate_artifact_identity:
test result: FAILED. 0 passed; 1 failed; 10 filtered out
exit_code: 101
```

Both exact tests are `ok` in the fresh post-rebase three-test selector above.

### Feature runtime judgment

Feature-specific runtime remains N/A. The changed surface is a pure, I/O-free
`lake-common` value/wire/binding contract with no CLI, SDK, Query, Metasrv,
object-store, or engine endpoint. The real-system e2e was still cold-booted by
`mise run gate` and passed as a baseline regression check.

## Transition matrix

- fail_to_pass:
  - New base `98e1667e` -> candidate: all four spec selectors changed from
    zero matches to the expected 1/1/1/3 passing tests.
  - Review head `5b816748` -> candidate: bytewise non-canonical JSON changed
    from regression-test exit 101 to four observed `NonCanonical` results.
  - Review head `5b816748` -> candidate: same Artifact identity with
    different binding semantics changed from exit 101 to observed
    `DuplicateIdentity { kind: "Artifact binding" }`.
- pass_to_fail: 0. The post-rebase full gate, direct fmt/clippy, lifecycle,
  every selector, and all hostile/success probes passed on the exact candidate.

## Probes

1. Bytewise non-canonical JSON matrix
   - Input: CJK manifest encoded as pretty JSON, reordered-map JSON, compact
     JSON plus a trailing newline, and `0.95` rewritten as equivalent
     `9.5e-1`.
   - Expected: all return `NonCanonical`.
   - Observed: four `Err(NonCanonical)` values.
   - Result: PASS.
2. Duplicate identity with different binding semantics
   - Input: second binding with `artifact_id = "对象-1"` but different role
     and selector.
   - Expected: `DuplicateIdentity` keyed by Artifact identity.
   - Observed: `DuplicateIdentity { kind: "Artifact binding", identity: "对象-1" }`.
   - Result: PASS.
3. Canonical digest and exact multiset success path
   - Input: exact manifest SHA-256/media type/length and one exact
     non-manifest ArtifactRef for the one logical binding.
   - Expected: successful CJK Episode bundle with two ArtifactRefs.
   - Observed: `Ok(episode=剧集-001, refs=2)`.
   - Result: PASS.

## Issues

None. The jj-subdirectory pathspec skip was closed by direct fmt/clippy.
The full workspace gate on the #312 baseline passed, so the rebase introduced
no observed regression.

## Verdict

PASS — exact post-rebase head `6aec163b249f` passes the complete gate,
lane-1 contract, direct static checks, both repaired P1 hostile cases, and the
original digest/exact-multiset success path with zero observed regressions.
