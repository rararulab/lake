# Verification report — issue #316

- base_sha: 3729455699c7d9ed28b7b57263ab8abf5a283a50
- head_sha: 680f2c2c206ee0d671d412e979772d9166ba08b6
- workspace_carrier_sha: abeaf5e9324557e6e230eadd035135ffde65a20d
- score_authority: verifier
- implementer_evidence: self_check_only

`head_sha` is the product commit (`@-`) whose tree was verified. The workspace
revision `@` was the empty carrier commit `abeaf5e9…`; `jj diff --from @- --to
@ --summary` produced no output, so both revisions had the same candidate tree.
The colocated repository's Git `HEAD` was
`3e37a4a324d986b813479d8ece9af884cc20866e`, which belonged to another
workspace and was deliberately not used as the candidate. The base is
`heads(::@ & ::main@origin)` at verification time.

## Commands

### Candidate identity and clean state

```text
$ jj st
The working copy has no changes.
Working copy  (@) : kmvmwukx abeaf5e9 (empty) (no description set)
Parent commit (@-): llqqkslz 680f2c2c feat(sdk): append generic typed Arrow batches (#316)

$ jj log -r '@|@-|heads(::@ & ::main@origin)' --no-graph -T '<change> <commit> <bookmarks> | <description>'
kmvmwukxlzss abeaf5e9324557e6e230eadd035135ffde65a20d  |
llqqkslzxusq 680f2c2c206ee0d671d412e979772d9166ba08b6  | feat(sdk): append generic typed Arrow batches (#316)
zpunyvupkmuy 3729455699c7d9ed28b7b57263ab8abf5a283a50 main | fix(release): schedule Release Please recovery (#313) (#317)

$ jj diff --from @- --to @ --summary
<no output>

$ git rev-parse HEAD
3e37a4a324d986b813479d8ece9af884cc20866e
```

All changed paths were within the spec's allowlist: `README.md`,
`crates/lake-sdk/**`, `docs/architecture.md`,
`docs/design/robot-training-lakehouse.md`, and the issue spec.

### Candidate quality gate

Before each runtime boot, the candidate workspace's exact `data/` directory
was removed after confirming that no process held it.

```text
$ rm -rf /Users/ryan/code/rararulab/lake/.worktrees/issue-316-typed-arrow-append/data && mise run gate
[hooks] $ prek run --all-files
[test] $ cargo test --workspace --all-targets
[e2e] $ cargo run -p lake-cli -- selftest
[site-check] Result (24 files):
[site-check] - 0 errors
[site-check] - 0 warnings
[site-check] - 0 hints
[test-adbc] test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out; finished in 1.66s
[e2e] created table robots.episodes
[e2e] committed robots.episodes at v2
[e2e] | alpha    | 2        | 0.8        |
[e2e] | beta     | 1        | 0.4        |
[e2e] self-check ok
[test] running 68 tests
[test] test tests::sdk_typed_arrow_append_rejects_invalid_batches_before_put ... ok
[test] test tests::sdk_typed_arrow_append_commits_episode_artifact_bundle ... ok
[test] test tests::sdk_typed_arrow_append_reuses_durable_idempotent_transport ... ok
[test] test result: ok. 65 passed; 0 failed; 3 ignored; 0 measured; 0 filtered out; finished in 11.59s
[test] Finished in 35.27s
Finished in 35.28s
```

The `prek` step reported both Rust hooks as skipped because its Git-based file
selection saw the unrelated shared Git `HEAD`. Direct candidate-tree checks
closed that multi-workspace false-green path:

```text
$ cargo +nightly fmt --all -- --check
<no output; exit 0>

$ cargo clippy -p lake-sdk --all-targets --all-features --no-deps -- -D warnings
    Checking lake-sdk v1.8.4 (/Users/ryan/code/rararulab/lake/.worktrees/issue-316-typed-arrow-append/crates/lake-sdk)
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 3.19s
```

### Candidate spec lifecycle and every selector

```text
$ mise run spec-lifecycle specs/issue-316-typed-arrow-append.spec.md
[spec-lifecycle] $ bun scripts/spec-lifecycle-guard.ts "${usage_spec?}"
=== Lifecycle Report (guarded) ===
Spec: typed-arrow-append  stage: complete  passed: true
  [PASS] Episode and ArtifactRef rows append through a Query-only SDK
  [PASS] invalid Arrow input fails before append side effects
  [PASS] an ambiguous Arrow append converges without a duplicate commit
spec-lifecycle-guard: OK — every Test selector executed >=1 test

$ cargo test -p lake-sdk sdk_typed_arrow_append_commits_episode_artifact_bundle
running 1 test
test tests::sdk_typed_arrow_append_commits_episode_artifact_bundle ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 67 filtered out; finished in 0.67s

$ cargo test -p lake-sdk sdk_typed_arrow_append_rejects_invalid_batches_before_put
running 1 test
test tests::sdk_typed_arrow_append_rejects_invalid_batches_before_put ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 67 filtered out; finished in 0.32s

$ cargo test -p lake-sdk sdk_typed_arrow_append_reuses_durable_idempotent_transport
running 1 test
test tests::sdk_typed_arrow_append_reuses_durable_idempotent_transport ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 67 filtered out; finished in 0.47s
```

### Fresh cold boot

```text
$ rm -rf /Users/ryan/code/rararulab/lake/.worktrees/issue-316-typed-arrow-append/data && cargo run -p lake-cli -- selftest
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 12.22s
     Running `target/debug/lake selftest`
created table robots.episodes
committed robots.episodes at v2
+----------+----------+------------+
| robot_id | episodes | avg_reward |
+----------+----------+------------+
| alpha    | 2        | 0.8        |
| beta     | 1        | 0.4        |
+----------+----------+------------+
self-check ok
```

The changed path itself was driven end to end by the first selector and the
hostile driver: public Query-only `LakeClient::append_batches` wrote one
operation through Query/Metasrv, and normal public SQL read the committed rows
back in the same fresh process. No managed stage or object-store credential was
constructed by that client.

### Hostile probe driver

The driver was a throwaway crate under `/tmp`, linked by path to the exact
candidate crates. Its source and RocksDB/Lance state were removed after the
run; no workspace implementation file was changed.

```text
$ cargo run --quiet
warning: linker stderr: ld: __eh_frame section too large (max 16MB) to encode dwarf unwind offsets in compact unwind table, performance of exception handling might be affected
  |
  = note: `#[warn(linker_messages)]` on by default

probe-cjk-multibatch: PASS version=2 rows_read=2 values=回合-一,回合-二
probe-exact-upper-bound: PASS version=3 appended_rows=10000 total_rows_read=10004
probe-schema-metadata-mismatch: PASS error=TableSchemaMismatch version_unchanged=3
```

### Base transition and regression baseline

A fresh temporary jj workspace was created with product parent exactly
`3729455699c7d9ed28b7b57263ab8abf5a283a50`; its generated empty workspace
carrier was `93f6eda64e1a3928b67d7a7a0e39beaf79560ab0` and had the base tree. The
temporary workspace and its runtime data were removed after verification.

```text
$ jj st
The working copy has no changes.
Working copy  (@) : tzwrxvsk 93f6eda6 (empty) (no description set)
Parent commit (@-): zpunyvup 37294556 main | fix(release): schedule Release Please recovery (#313) (#317)

$ rm -rf /Users/ryan/code/rararulab/lake/.worktrees/verify-issue316-base/data && mise run gate
[hooks] cargo fmt............................................(no files to check)Skipped
[hooks] cargo clippy.........................................(no files to check)Skipped
[site-check] Result (24 files):
[site-check] - 0 errors
[site-check] - 0 warnings
[site-check] - 0 hints
[test-adbc] test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out; finished in 22.64s
[e2e] created table robots.episodes
[e2e] committed robots.episodes at v2
[e2e] self-check ok
[test] running 65 tests
[test] test result: ok. 62 passed; 0 failed; 3 ignored; 0 measured; 0 filtered out; finished in 11.19s
[test] Finished in 315.69s
Finished in 315.70s

$ cargo test -p lake-sdk sdk_typed_arrow_append_commits_episode_artifact_bundle
running 0 tests
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 65 filtered out; finished in 0.00s

$ cargo test -p lake-sdk sdk_typed_arrow_append_rejects_invalid_batches_before_put
running 0 tests
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 65 filtered out; finished in 0.00s

$ cargo test -p lake-sdk sdk_typed_arrow_append_reuses_durable_idempotent_transport
running 0 tests
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 65 filtered out; finished in 0.00s

$ mise run spec-lifecycle /Users/ryan/code/rararulab/lake/.worktrees/issue-316-typed-arrow-append/specs/issue-316-typed-arrow-append.spec.md
=== Lifecycle Report (guarded) ===
Spec: typed-arrow-append  stage: complete  passed: true
  [PASS] Episode and ArtifactRef rows append through a Query-only SDK
  [PASS] invalid Arrow input fails before append side effects
  [PASS] an ambiguous Arrow append converges without a duplicate commit

spec-lifecycle-guard: FAIL — Test selector(s) matched ZERO tests (0 passed; filtered out):
  - Episode and ArtifactRef rows append through a Query-only SDK
  - invalid Arrow input fails before append side effects
  - an ambiguous Arrow append converges without a duplicate commit
Every lane-1 Test: selector must resolve to >=1 real test function — see specs/README.md.
[spec-lifecycle] ERROR task failed
```

The first attempt to run the base gate from an external `/tmp` jj workspace
failed before tests because `prek` could not find a Git ancestor. The exact
base was recreated under the repository's standard `.worktrees/` directory;
the successful clean gate above is the base evidence used for scoring.

## Transition matrix

- fail_to_pass:
  - `sdk_typed_arrow_append_commits_episode_artifact_bundle`: base matched zero
    tests and the guard failed; head executed 1 test and passed, including
    Query-only append plus public SQL readback.
  - `sdk_typed_arrow_append_rejects_invalid_batches_before_put`: base matched
    zero tests and the guard failed; head executed 1 test and passed all local
    and authoritative schema rejection cases without publishing a version.
  - `sdk_typed_arrow_append_reuses_durable_idempotent_transport`: base matched
    zero tests and the guard failed; head executed 1 test and passed checkpoint
    reload, identical payload reuse, ambiguous retry convergence, exactly one
    committed version, and checkpoint cleanup.
- pass_to_fail: 0. Base and head full gates both completed with zero failed
  tests; the head lake-sdk count is exactly the base's 62 passing tests plus
  the 3 new passing selectors (65 total), with the same 3 ignored tests.

## Probes

1. CJK plus multi-batch input
   - Input: two exact-schema Episode/ArtifactRef batches in one call, with
     `回合-一` / `回合-二`, `机械臂-甲` / `机械臂-乙`, and CJK task/layer values.
   - Expected: one version commits all rows and SQL returns the original UTF-8
     values.
   - Observed: `Version(2)`, two Episode rows read back exactly; PASS.
2. Exact aggregate upper boundary
   - Input: one exact-schema RecordBatch with exactly 10,000 rows.
   - Expected: accepted (the limit is inclusive), one new version, all rows
     queryable.
   - Observed: `Version(3)` and SQL `COUNT(*) = 10,004` after the prior four
     rows; PASS.
3. Schema metadata mismatch
   - Input: fields, order, data types, and arrays identical to the authoritative
     schema, but with one extra schema metadata key.
   - Expected: typed exact-schema rejection and no append side effect.
   - Observed: `SdkError::TableSchemaMismatch`; table stayed at `Version(3)`;
     PASS.

## Verdict

PASS — the exact product head passes the clean gate, guarded lane-1 lifecycle,
all selectors, fresh cold boot, end-to-end Query-only write/read drive, and all
three hostile probes; all three expected base failures became real passing
tests and `pass_to_fail` is 0.
