# Verification report — issue #316

- base_sha: `92e5fcfe32ac52de5807471e1c7f21802d64511f`
- head_sha: `ce42c81f8b42467bff820e06920152f5992a9a9b`
- verification_carrier: `c89a2e072531a8942cfc22941ab2d1c77d31e15f` (empty; exact parent is `head_sha`)
- score_authority: verifier
- implementer_evidence: self_check_only
- lane: 1
- spec: `specs/issue-316-typed-arrow-append.spec.md`

Fresh post-rebase verification. I did not read the previous report or use plain Git HEAD.
`jj diff --from ce42c81f8b42467bff820e06920152f5992a9a9b --to @ --stat`
reported zero files changed before verification.

## Commands

### Identity, environment, and rebase scope

```text
$ mise run doctor
[doctor] $ bun scripts/doctor.ts
[ ok ] mise tools installed
[ ok ] nightly rustfmt

$ jj st
The working copy has no changes.
Working copy  (@) : sowqtypo c89a2e07 (empty) (no description set)
Parent commit (@-): pkvvvvln ce42c81f fix(sdk): preserve exact nested Arrow schema (#316)

$ mise x -- cargo metadata --format-version 1 --no-deps | jq -r .target_directory
/Users/ryan/Library/Caches/lake/target/b19ea257adcb88b0cfa217bcfe9b8abd4c520146ff4a7c75ac43215bf463dbcb
```

The candidate used its workspace-isolated target. The fixed-base increment since the prior
base was independently inspected: it contains release workflow/contract, site theme, and mise
target-isolation changes only; no `lake-sdk` implementation path. The rebased issue diff remains
inside its allowed SDK/docs/spec/report boundaries, and the carrier was clean with no conflicts.

### Fresh-data full gate

```text
$ rm -rf -- /Users/ryan/code/rararulab/lake/.worktrees/issue-316-typed-arrow-append/data
$ mise run gate
[hooks] $ prek run --all-files
[test] $ cargo test --workspace --all-targets
[e2e] $ cargo run -p lake-cli -- selftest
[test-adbc] $ cargo test -p lake-query --test adbc_interop -- --ignored --test-threads=1
[site-check] $ bun run --cwd site check

[hooks] Finished in 77.6ms
[test-adbc] test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out; finished in 2.08s
[site-check] Result (25 files):
[site-check] - 0 errors
[site-check] - 0 warnings
[site-check] - 0 hints
[site-check] All matched files use Prettier code style!
[site-check] 91 page(s) built in 3.22s

[e2e] created table robots.episodes
[e2e] committed robots.episodes at v2
[e2e] +----------+----------+------------+
[e2e] | robot_id | episodes | avg_reward |
[e2e] +----------+----------+------------+
[e2e] | alpha    | 2        | 0.8        |
[e2e] | beta     | 1        | 0.4        |
[e2e] +----------+----------+------------+
[e2e] self-check ok

[test] running 75 tests
[test] test tests::sdk_typed_arrow_append_encodes_list_view_slices_lazily ... ok
[test] test tests::sdk_typed_arrow_append_preserves_large_list_and_union_metadata ... ok
[test] test tests::sdk_typed_arrow_append_preserves_dictionary_encoding ... ok
[test] test tests::sdk_typed_arrow_append_rejects_unbounded_inputs_before_schema_rpc ... ok
[test] test tests::sdk_typed_arrow_append_stops_encoding_at_payload_limit ... ok
[test] test tests::sdk_typed_arrow_append_checkpoint_partition_boundary_is_consistent ... ok
[test] test tests::sdk_typed_arrow_append_rejects_invalid_batches_before_put ... ok
[test] test tests::sdk_typed_arrow_append_commits_episode_artifact_bundle ... ok
[test] test tests::sdk_typed_arrow_append_reuses_durable_idempotent_transport ... ok
[test] test append_checkpoint::tests::durable_checkpoint_accepts_maximum_typed_append_partition ... ok
[test] test result: ok. 72 passed; 0 failed; 3 ignored; 0 measured; 0 filtered out; finished in 11.74s
[test] Finished in 42.95s
Finished in 42.95s
exit: 0
```

### Direct Rust checks

```text
$ cargo +nightly fmt --all -- --check
exit: 0

$ cargo clippy -p lake-sdk --all-targets --all-features --no-deps -- -D warnings
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.75s
exit: 0
```

The recurring macOS linker `__eh_frame section too large` warning did not affect status;
direct clippy with `-D warnings` was clean.

### Lane-1 lifecycle and direct selectors

```text
$ mise run spec-lifecycle specs/issue-316-typed-arrow-append.spec.md
=== Lifecycle Report (guarded) ===
Spec: typed-arrow-append  stage: complete  passed: true
  [PASS] Episode and ArtifactRef rows append through a Query-only SDK
  [PASS] invalid Arrow input fails before append side effects
  [PASS] Arrow input memory and batch fan-out are bounded before schema lookup
  [PASS] encoded Flight collection stops at the exact payload ceiling
  [PASS] compact Dictionary input preserves its exact Flight schema
  [PASS] nested Arrow types and field metadata remain exact on the wire
  [PASS] shared-child ListView slices are encoded lazily
  [PASS] an ambiguous Arrow append converges without a duplicate commit
  [PASS] checkpointing accepts the same maximum batch partition as memory-only preparation
  [PASS] checkpoint framing accepts the exact dictionary-aware message ceiling
spec-lifecycle-guard: OK — every Test selector executed >=1 test
exit: 0
```

Each selector was also run directly and selected exactly one test:

```text
$ cargo test -p lake-sdk sdk_typed_arrow_append_commits_episode_artifact_bundle
test tests::sdk_typed_arrow_append_commits_episode_artifact_bundle ... ok
$ cargo test -p lake-sdk sdk_typed_arrow_append_rejects_invalid_batches_before_put
test tests::sdk_typed_arrow_append_rejects_invalid_batches_before_put ... ok
$ cargo test -p lake-sdk sdk_typed_arrow_append_rejects_unbounded_inputs_before_schema_rpc
test tests::sdk_typed_arrow_append_rejects_unbounded_inputs_before_schema_rpc ... ok
$ cargo test -p lake-sdk sdk_typed_arrow_append_stops_encoding_at_payload_limit
test tests::sdk_typed_arrow_append_stops_encoding_at_payload_limit ... ok
$ cargo test -p lake-sdk sdk_typed_arrow_append_preserves_dictionary_encoding
test tests::sdk_typed_arrow_append_preserves_dictionary_encoding ... ok
$ cargo test -p lake-sdk sdk_typed_arrow_append_preserves_large_list_and_union_metadata
test tests::sdk_typed_arrow_append_preserves_large_list_and_union_metadata ... ok
$ cargo test -p lake-sdk sdk_typed_arrow_append_encodes_list_view_slices_lazily
test tests::sdk_typed_arrow_append_encodes_list_view_slices_lazily ... ok
$ cargo test -p lake-sdk sdk_typed_arrow_append_reuses_durable_idempotent_transport
test tests::sdk_typed_arrow_append_reuses_durable_idempotent_transport ... ok
$ cargo test -p lake-sdk sdk_typed_arrow_append_checkpoint_partition_boundary_is_consistent
test tests::sdk_typed_arrow_append_checkpoint_partition_boundary_is_consistent ... ok
$ cargo test -p lake-sdk durable_checkpoint_accepts_maximum_typed_append_partition
test append_checkpoint::tests::durable_checkpoint_accepts_maximum_typed_append_partition ... ok

For every command:
running 1 test
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 74 filtered out
```

### New fixed-base transition with a separate target

The temporary base workspace had an empty carrier whose exact parent was
`92e5fcfe32ac52de5807471e1c7f21802d64511f`; its diff from that base was zero.
Its target was a separate APFS CoW copy and the actual Cargo process confirmed the override:

```text
$ env CARGO_TARGET_DIR=/tmp/lake-verifier-316-base-92e5-target cargo metadata --format-version 1 --no-deps | jq -r .target_directory
/tmp/lake-verifier-316-base-92e5-target
```

All ten base commands below produced a real zero-match:

```text
$ env CARGO_TARGET_DIR=/tmp/lake-verifier-316-base-92e5-target cargo test -p lake-sdk sdk_typed_arrow_append_commits_episode_artifact_bundle
$ env CARGO_TARGET_DIR=/tmp/lake-verifier-316-base-92e5-target cargo test -p lake-sdk sdk_typed_arrow_append_rejects_invalid_batches_before_put
$ env CARGO_TARGET_DIR=/tmp/lake-verifier-316-base-92e5-target cargo test -p lake-sdk sdk_typed_arrow_append_rejects_unbounded_inputs_before_schema_rpc
$ env CARGO_TARGET_DIR=/tmp/lake-verifier-316-base-92e5-target cargo test -p lake-sdk sdk_typed_arrow_append_stops_encoding_at_payload_limit
$ env CARGO_TARGET_DIR=/tmp/lake-verifier-316-base-92e5-target cargo test -p lake-sdk sdk_typed_arrow_append_preserves_dictionary_encoding
$ env CARGO_TARGET_DIR=/tmp/lake-verifier-316-base-92e5-target cargo test -p lake-sdk sdk_typed_arrow_append_preserves_large_list_and_union_metadata
$ env CARGO_TARGET_DIR=/tmp/lake-verifier-316-base-92e5-target cargo test -p lake-sdk sdk_typed_arrow_append_encodes_list_view_slices_lazily
$ env CARGO_TARGET_DIR=/tmp/lake-verifier-316-base-92e5-target cargo test -p lake-sdk sdk_typed_arrow_append_reuses_durable_idempotent_transport
$ env CARGO_TARGET_DIR=/tmp/lake-verifier-316-base-92e5-target cargo test -p lake-sdk sdk_typed_arrow_append_checkpoint_partition_boundary_is_consistent
$ env CARGO_TARGET_DIR=/tmp/lake-verifier-316-base-92e5-target cargo test -p lake-sdk durable_checkpoint_accepts_maximum_typed_append_partition
running 0 tests
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 65 filtered out; finished in 0.00s
```

The guarded lifecycle used the candidate spec against the base tree and failed closed:

```text
$ env CARGO_TARGET_DIR=/tmp/lake-verifier-316-base-92e5-target PATH=<actual-tool-paths> bun scripts/spec-lifecycle-guard.ts <candidate>/specs/issue-316-typed-arrow-append.spec.md
spec-lifecycle-guard: FAIL — Test selector(s) matched ZERO tests (0 passed; filtered out):
  - Episode and ArtifactRef rows append through a Query-only SDK
  - invalid Arrow input fails before append side effects
  - Arrow input memory and batch fan-out are bounded before schema lookup
  - encoded Flight collection stops at the exact payload ceiling
  - compact Dictionary input preserves its exact Flight schema
  - nested Arrow types and field metadata remain exact on the wire
  - shared-child ListView slices are encoded lazily
  - an ambiguous Arrow append converges without a duplicate commit
  - checkpointing accepts the same maximum batch partition as memory-only preparation
  - checkpoint framing accepts the exact dictionary-aware message ceiling
Every lane-1 Test: selector must resolve to >=1 real test function — see specs/README.md.
exit: 1

$ env CARGO_TARGET_DIR=/tmp/lake-verifier-316-base-92e5-target cargo test -p lake-sdk
running 65 tests
test result: ok. 62 passed; 0 failed; 3 ignored; 0 measured; 0 filtered out; finished in 11.68s
exit: 0
```

## Transition matrix

- fail_to_pass: 10/10 selectors changed from zero-match at the new fixed base (guard exit 1)
  to exactly one passing test each at the rebased product head (guarded lifecycle 10/10 PASS).
- pass_to_fail: 0. Base SDK: 62 passed / 3 ignored. Candidate SDK: 72 passed /
  3 ignored—exactly ten new passes and no lost/failing old test. The full rebased workspace,
  ADBC, updated site, and cold-boot e2e were green.

## Probes

### Exact Arrow IPC — PASS

- Dictionary: 10,000 Int32 keys share one approximately 8 KiB CJK value. Physical buffers
  remain below 64 MiB while hydration would be 81,900,000 bytes. The low-level IPC generator
  uses `IpcDictionaryHandling::Resend`; decoded schema and RecordBatch equal the input exactly.
- Nested metadata: LargeList, sparse Union with outer field metadata, and schema metadata all
  survive encode/decode exactly; decoded schema and RecordBatch are directly equality-checked.
- ListView: 32 rows share one 64 KiB child and a 2 KiB target forces one-row slices. After
  schema plus first data message: `encoded_slices == 1`, pending queue empty, offset 1. No later
  slice is materialized before the collector can reject.

### Bounds and recovery — PASS

- Empty/zero-row/10,001 reject; one row reaches authoritative schema comparison; exact 10,000
  Dictionary rows encode and round-trip.
- A `64 MiB + 1` Binary buffer returns typed `BatchInputSize` before schema RPC; the generic
  saturating buffer-memory sum applies equally to Utf8.
- 17 Dictionary nodes return typed `actual: 17, maximum: 16` through an unreachable lazy Query
  channel. Nested traversal covers List/ListView/LargeList/LargeListView/Map/Struct/Union/
  RunEndEncoded; the exact rejection is `> 16`, so 16 is accepted and 17 rejected.
- Encoded overflow stops after polling the second message (`polled == 2`), never the third.
- 4,096 one-row batches emit 4,097 messages; memory-only and durable paths both succeed and
  checkpoint reload is byte-for-byte exact.
- Maximum is `1 + 10,000 * (1 + 16) = 170,001` messages. Framing is
  `4 + 170,001 * 4 = 680,008` bytes, below 1 MiB. Exact max save/load preserves all messages;
  collector/checkpoint encode/decode share the bound and reject greater counts.
- Ambiguous first-result loss retries the same durable identity/payload, converges at version 2
  with one committed version, and removes the checkpoint.
- Query-only Episode/ArtifactRef append commits one version and public SQL reads every row back;
  scalar `insert`/`insert_many` regressions remain green.

## Cleanup

Temporary base workspace, isolated base target, and generated candidate `data/` were removed.
Before report creation, candidate and main checkout were clean; candidate parent was exact
`ce42c81f8b42467bff820e06920152f5992a9a9b`.

## Verdict

PASS — rebased exact product head `ce42c81f8b42467bff820e06920152f5992a9a9b`
passes fresh gate/direct checks, all ten guarded behaviors and hostile boundaries, the new-base
fail-to-pass transition, and has `pass_to_fail = 0`.
