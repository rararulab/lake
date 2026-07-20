# Verification report — issue #316

- base_sha: 3729455699c7d9ed28b7b57263ab8abf5a283a50
- head_sha: 7947d1c29a9ff1d52d9d1ae541a43b813aba972b
- score_authority: verifier
- implementer_evidence: self_check_only
- lane: 1
- spec: `specs/issue-316-typed-arrow-append.spec.md`
- candidate_workspace: `/Users/ryan/code/rararulab/lake/.worktrees/issue-316-typed-arrow-append`
- candidate_revision_note: workspace `@` was empty carrier `8694924734d78bcdbf61c89730889d62c0bdf752`; every artifact/diff assertion was pinned to its exact parent repair commit `7947d1c29a9ff1d52d9d1ae541a43b813aba972b`. Plain Git HEAD was not used because it resolved to another colocated checkout.
- context_isolation: the prompt disclosed only prior product/report commit identifiers, not their evidence. No implementer/reviewer hand-off or prior report content was read or trusted.

## Commands

### Environment and revision pin

`mise run doctor`

```text
[ ok ] mise tools installed
[ ok ] nightly rustfmt
[ ok ] cargo check
[ ok ] jj repo: /Users/ryan/code/rararulab/lake/.worktrees/issue-316-typed-arrow-append
[ ok ] gh authenticated
[ ok ] git remote: origin
```

`jj st`

```text
The working copy has no changes.
Working copy  (@) : ppuyqrwz 86949247 (empty) (no description set)
Parent commit (@-): zpnkkqkr 7947d1c2 fix(sdk): bound typed Arrow append encoding (#316)
```

`jj log --no-graph -r '@|@-|7947d1c29a9ff1d52d9d1ae541a43b813aba972b|3729455699c7d9ed28b7b57263ab8abf5a283a50' -T 'commit_id ++ " " ++ change_id ++ " " ++ description.first_line() ++ "\n"'`

```text
8694924734d78bcdbf61c89730889d62c0bdf752 ppuyqrwzyquprvrskuxmzutvusqlwkpt
7947d1c29a9ff1d52d9d1ae541a43b813aba972b zpnkkqkrsynvvrkpvttzzrqlzsqxyqtu fix(sdk): bound typed Arrow append encoding (#316)
3729455699c7d9ed28b7b57263ab8abf5a283a50 zpunyvupkmuymuntvlpmlmyvosqolwqq fix(release): schedule Release Please recovery (#313) (#317)
```

`git merge-base 7947d1c29a9ff1d52d9d1ae541a43b813aba972b origin/main`

```text
3729455699c7d9ed28b7b57263ab8abf5a283a50
```

### Candidate quality gate

An initial attempt to set `CARGO_TARGET_DIR` outside `mise` was explicitly disqualified because project `[env]` overrode it; its raw executable path exposed the mistake:

`CARGO_TARGET_DIR=/tmp/lake-verify-316-candidate-target.1C33ii mise run gate`

```text
[e2e]      Running `/Users/ryan/Library/Caches/lake/target/debug/lake selftest`
Finished in 35.16s
```

No base build ever used that shared target. A temporary `cargo` wrapper was then verified with `cargo metadata` to force the isolated target for every nested `mise` cargo invocation.

Cold-build candidate run from an empty isolated target:

`LAKE_VERIFY_TARGET=/tmp/lake-verify-316-candidate-target.1C33ii PATH=/tmp/lake-verify-316-bin:$PATH mise run gate`

```text
[test-adbc] test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out; finished in 2.59s
[test] running 72 tests
[test] test result: ok. 69 passed; 0 failed; 3 ignored; 0 measured; 0 filtered out; finished in 11.64s
[test] Finished in 379.80s
Finished in 379.81s
```

That run proved a cold, isolated build but inherited data created by the disqualified attempt, so its e2e result was not counted as cold-boot evidence. The complete gate was rerun after deleting the workspace data directory:

`rm -rf /Users/ryan/code/rararulab/lake/.worktrees/issue-316-typed-arrow-append/data && test ! -e /Users/ryan/code/rararulab/lake/.worktrees/issue-316-typed-arrow-append/data && LAKE_VERIFY_TARGET=/tmp/lake-verify-316-candidate-target.1C33ii PATH=/tmp/lake-verify-316-bin:$PATH mise run gate`

```text
[site-check] Result (24 files):
[site-check] - 0 errors
[site-check] - 0 warnings
[site-check] - 0 hints
[site-check] All matched files use Prettier code style!
[site-check] Checked 8 generated site artifacts.
[test-adbc] test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out; finished in 1.74s
[e2e] created table robots.episodes
[e2e] committed robots.episodes at v2
[e2e] | alpha    | 2        | 0.8        |
[e2e] | beta     | 1        | 0.4        |
[e2e] self-check ok
[test] running 72 tests
[test] test result: ok. 69 passed; 0 failed; 3 ignored; 0 measured; 0 filtered out; finished in 11.59s
[test] Finished in 34.40s
Finished in 34.41s
```

All other `cargo test --workspace --all-targets` result lines in the same gate were `ok` with zero failures.

### Direct formatting and lint

`LAKE_VERIFY_TARGET=/tmp/lake-verify-316-candidate-target.1C33ii PATH=/tmp/lake-verify-316-bin:$PATH cargo +nightly fmt --all -- --check`

```text
exit 0; no stdout/stderr
```

`LAKE_VERIFY_TARGET=/tmp/lake-verify-316-candidate-target.1C33ii PATH=/tmp/lake-verify-316-bin:$PATH cargo clippy -p lake-sdk --all-targets --all-features --no-deps -- -D warnings`

```text
    Checking lake-sdk v1.8.4 (/Users/ryan/code/rararulab/lake/.worktrees/issue-316-typed-arrow-append/crates/lake-sdk)
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 2m 08s
```

### Lane 1 lifecycle and selectors

`LAKE_VERIFY_TARGET=/tmp/lake-verify-316-candidate-target.1C33ii PATH=/tmp/lake-verify-316-bin:$PATH mise run spec-lifecycle specs/issue-316-typed-arrow-append.spec.md`

```text
=== Lifecycle Report (guarded) ===
Spec: typed-arrow-append  stage: complete  passed: true
  [PASS] Episode and ArtifactRef rows append through a Query-only SDK
  [PASS] invalid Arrow input fails before append side effects
  [PASS] Arrow input memory and batch fan-out are bounded before schema lookup
  [PASS] encoded Flight collection stops at the exact payload ceiling
  [PASS] an ambiguous Arrow append converges without a duplicate commit
  [PASS] checkpointing accepts the same maximum batch partition as memory-only preparation
spec-lifecycle-guard: OK — every Test selector executed >=1 test
```

Each acceptance selector was then run directly:

`cargo test -p lake-sdk sdk_typed_arrow_append_commits_episode_artifact_bundle`

```text
running 1 test
test tests::sdk_typed_arrow_append_commits_episode_artifact_bundle ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 71 filtered out; finished in 0.35s
```

`cargo test -p lake-sdk sdk_typed_arrow_append_rejects_invalid_batches_before_put`

```text
running 1 test
test tests::sdk_typed_arrow_append_rejects_invalid_batches_before_put ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 71 filtered out; finished in 0.32s
```

`cargo test -p lake-sdk sdk_typed_arrow_append_rejects_unbounded_inputs_before_schema_rpc`

```text
running 1 test
test tests::sdk_typed_arrow_append_rejects_unbounded_inputs_before_schema_rpc ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 71 filtered out; finished in 0.00s
```

`cargo test -p lake-sdk sdk_typed_arrow_append_stops_encoding_at_payload_limit`

```text
running 1 test
test tests::sdk_typed_arrow_append_stops_encoding_at_payload_limit ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 71 filtered out; finished in 0.00s
```

`cargo test -p lake-sdk sdk_typed_arrow_append_reuses_durable_idempotent_transport`

```text
running 1 test
test tests::sdk_typed_arrow_append_reuses_durable_idempotent_transport ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 71 filtered out; finished in 0.47s
```

`cargo test -p lake-sdk sdk_typed_arrow_append_checkpoint_partition_boundary_is_consistent`

```text
running 1 test
test tests::sdk_typed_arrow_append_checkpoint_partition_boundary_is_consistent ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 71 filtered out; finished in 0.14s
```

### Fresh boot

`rm -rf /Users/ryan/code/rararulab/lake/.worktrees/issue-316-typed-arrow-append/data && test ! -e /Users/ryan/code/rararulab/lake/.worktrees/issue-316-typed-arrow-append/data && LAKE_VERIFY_TARGET=/tmp/lake-verify-316-candidate-target.1C33ii PATH=/tmp/lake-verify-316-bin:$PATH cargo run -p lake-cli -- selftest`

```text
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

### Additional candidate regressions

`cargo test -p lake-sdk durable_checkpoint_accepts_maximum_typed_append_partition`

```text
running 1 test
test append_checkpoint::tests::durable_checkpoint_accepts_maximum_typed_append_partition ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 71 filtered out; finished in 0.09s
```

`cargo test -p lake-sdk sdk_batch_insert`

```text
running 4 tests
test tests::sdk_batch_insert_flight_bound_uses_protobuf_size ... ok
test tests::sdk_batch_insert_rejects_empty_and_excessive_batches ... ok
test tests::sdk_batch_insert_validates_every_row_before_upload ... ok
test tests::sdk_batch_insert_commits_multiple_files_as_one_version ... ok
test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 68 filtered out; finished in 0.68s
```

### Throwaway public-API hostile driver

The uncommitted driver lived only under `/tmp`, depended on the exact candidate workspace, and was removed after execution.

`cargo run --manifest-path /tmp/lake-verify-316-probe/Cargo.toml`

```text
single zero-row batch: typed EmptyBatch(index=0), schema_rpcs=0
one row + 9999 zero-row batches: typed EmptyBatch(index=1), schema_rpcs=0
>64MiB Binary buffer: typed BatchInputSize, schema_rpcs=0
>64MiB Utf8 buffer: typed BatchInputSize, schema_rpcs=0
one-row lower boundary: committed v2
CJK multi-batch append: committed two batches atomically at v3
10000-row upper boundary: committed v4
schema metadata mismatch: typed TableSchemaMismatch, version remains v4
shared insert/insert_many + SQL reload: v6, 10006 exact rows, CJK preserved
all issue-316 throwaway hostile probes passed
```

### Exact-base transition

Temporary base workspace: `/tmp/lake-verify-316-base.Si93eZ/ws`, empty `@` with exact parent `3729455699c7d9ed28b7b57263ab8abf5a283a50`. All Cargo invocations used the initially empty, base-only target `/tmp/lake-verify-316-base-target.N8qsdk`; the candidate/shared target was never used.

Each of the six direct selector commands was run at exact base. Their identical result was:

```text
running 0 tests
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 65 filtered out; finished in 0.00s
```

The six commands were:

```text
cargo test -p lake-sdk sdk_typed_arrow_append_commits_episode_artifact_bundle
cargo test -p lake-sdk sdk_typed_arrow_append_rejects_invalid_batches_before_put
cargo test -p lake-sdk sdk_typed_arrow_append_rejects_unbounded_inputs_before_schema_rpc
cargo test -p lake-sdk sdk_typed_arrow_append_stops_encoding_at_payload_limit
cargo test -p lake-sdk sdk_typed_arrow_append_reuses_durable_idempotent_transport
cargo test -p lake-sdk sdk_typed_arrow_append_checkpoint_partition_boundary_is_consistent
```

The candidate guard/spec was then driven against exact-base code with absolute `bun` and `agent-spec` paths:

`bun scripts/spec-lifecycle-guard.ts specs/issue-316-typed-arrow-append.spec.md` (candidate guard/spec, `--code .` resolving the base workspace)

```text
=== Lifecycle Report (guarded) ===
Spec: typed-arrow-append  stage: complete  passed: true
  [PASS] Episode and ArtifactRef rows append through a Query-only SDK
  [PASS] invalid Arrow input fails before append side effects
  [PASS] Arrow input memory and batch fan-out are bounded before schema lookup
  [PASS] encoded Flight collection stops at the exact payload ceiling
  [PASS] an ambiguous Arrow append converges without a duplicate commit
  [PASS] checkpointing accepts the same maximum batch partition as memory-only preparation

spec-lifecycle-guard: FAIL — Test selector(s) matched ZERO tests (0 passed; filtered out):
  - Episode and ArtifactRef rows append through a Query-only SDK
  - invalid Arrow input fails before append side effects
  - Arrow input memory and batch fan-out are bounded before schema lookup
  - encoded Flight collection stops at the exact payload ceiling
  - an ambiguous Arrow append converges without a duplicate commit
  - checkpointing accepts the same maximum batch partition as memory-only preparation
Every lane-1 Test: selector must resolve to >=1 real test function — see specs/README.md.
```

Exit status: 1, expected rejection.

`cargo test -p lake-sdk` at exact base:

```text
running 65 tests
test result: ok. 62 passed; 0 failed; 3 ignored; 0 measured; 0 filtered out; finished in 11.73s
```

The exact base already had a committed verifier artifact at `14356d08badd9c035cbe898ec0c88649620ec9c6`; its contents were not read. Per the role contract and repair instructions, the full base gate was not redundantly rerun. The affected crate's complete old suite and all six guarded transitions were rerun independently.

## Transition matrix

- fail_to_pass:
  - Base: every one of the six spec selectors matched zero tests; the guard rejected all six with exit 1.
  - Repair head: every selector executed exactly one test and passed; the guarded lifecycle reported six PASS results and `spec-lifecycle-guard: OK`.
  - Expected vs observed: expected the new public typed append, local input bounds, incremental encoded bound, durable retry, and checkpoint partition behavior to be absent at base and present at repair; observed exactly that transition.
- pass_to_fail: 0.
  - Exact base `lake-sdk`: 62 passed, 0 failed, 3 ignored.
  - Repair `lake-sdk`: 69 passed, 0 failed, 3 ignored.
  - Repair full workspace gate: zero failures; direct fmt and clippy: exit 0.

## Probes

1. Single and fan-out zero-row batches before schema RPC
   - Input: one zero-row batch; then one valid row followed by 9,999 zero-row batches.
   - Expected: typed `EmptyBatch`, no schema RPC, no Flight encoding/put.
   - Observed: `EmptyBatch(index=0)` and `EmptyBatch(index=1)` respectively; schema RPC counter stayed 0.
   - PASS.

2. Oversized caller buffers before schema RPC/Flight encoding
   - Input: one-row Binary and Utf8 batches whose Arrow buffer memory exceeded 64 MiB.
   - Expected: typed `BatchInputSize`, schema RPC counter 0.
   - Observed: both returned `BatchInputSize`; schema RPC counter stayed 0.
   - PASS.

3. Incremental Flight collection
   - Input: three-message observable stream, with the second message crossing the exact protobuf-size ceiling.
   - Expected: reject on message two and never poll message three.
   - Observed: direct selector passed its `polled == 2` assertion.
   - PASS.

4. Memory/durable framing parity
   - Input: schema + 4,096 record messages with checkpointing off/on; separately the derived maximum 10,001 messages.
   - Expected: both modes accept, durable reload byte-for-byte exact, maximum framing accepted.
   - Observed: spec selector and 10,001-message checkpoint regression both passed.
   - PASS.

5. Public runtime boundaries and exact schema
   - Input: 1 row, two CJK batches, 10,000 rows, then identical fields with mismatched schema metadata.
   - Expected: valid boundaries commit atomically; CJK survives; metadata mismatch is typed and publishes nothing.
   - Observed: versions advanced v2, v3, v4; mismatch returned `TableSchemaMismatch` and stayed v4; SQL reload preserved CJK.
   - PASS.

6. Durable ambiguity and shared scalar append path
   - Input: lost first typed append result; then public `insert` and `insert_many` after typed batches.
   - Expected: retry reuses one durable identity and commits once; shared legacy path remains functional.
   - Observed: ambiguous selector retried twice and ended at one v2 commit with checkpoint removed; throwaway runtime advanced scalar writes to v6 and reloaded exactly 10,006 rows; `sdk_batch_insert` was 4/4.
   - PASS.

## Cleanup

- Temporary base workspace was forgotten with `jj workspace forget verify-316-base-Si93eZ`.
- Temporary base workspace, base target, candidate target, throwaway probe, cargo wrapper, and candidate `data/` were removed.
- No temporary base Cargo command used `/Users/ryan/Library/Caches/lake/target`.

## Verdict

PASS — exact repair head `7947d1c29a9ff1d52d9d1ae541a43b813aba972b` passes the isolated full gate, six guarded Lane 1 criteria, fresh boot, hostile bounds/reload probes, and exact-base fail-to-pass transition with `pass_to_fail = 0`.
