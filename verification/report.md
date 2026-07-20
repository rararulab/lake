# Verification report — issue #316

- base_sha: `3729455699c7d9ed28b7b57263ab8abf5a283a50`
- head_sha: `f2fd5ad379c01a111c893b631a04edfc718bc08b`
- verification_carrier: `6ee3d754658f7d7d7a46180d14102a7bad5ca2da` (empty; parent is `head_sha`)
- score_authority: verifier
- implementer_evidence: self_check_only
- lane: 1
- spec: `specs/issue-316-typed-arrow-append.spec.md`

The verifier did not read or rely on the implementer hand-off or the old/intermediate
verification report. All candidate commands below ran from
`.worktrees/issue-316-typed-arrow-append`; `jj diff --from f2fd5ad3 --to @`
reported `0 files changed`, so the tested tree is the exact product head rather than
plain Git HEAD from another checkout.

## Commands

### Environment and identity

```text
$ mise run doctor
[doctor] $ bun scripts/doctor.ts
[ ok ] mise tools installed
[ ok ] nightly rustfmt
[ ok ] cargo check
[ ok ] jj repo: /Users/ryan/code/rararulab/lake

$ jj st
The working copy has no changes.
Working copy  (@) : soqplzwm 6ee3d754 (empty) (no description set)
Parent commit (@-): kznsvwou f2fd5ad3 fix(sdk): preserve dictionary Arrow encoding (#316)

$ jj diff --from f2fd5ad379c01a111c893b631a04edfc718bc08b --to @ --stat
0 files changed, 0 insertions(+), 0 deletions(-)

$ mise x -- cargo metadata --format-version 1 --no-deps | jq -r .target_directory
/Users/ryan/Library/Caches/lake/target
```

The candidate shared target resolved to the documented project cache and was used only
for the exact candidate tree. Freshness below refers to runtime data/state, not a needless
dependency rebuild.

### Fresh-data full gate and cold boot

Before the gate, the workspace-local `data/` was removed. The gate then rebuilt runtime
state and drove the real CLI path.

```text
$ rm -rf -- /Users/ryan/code/rararulab/lake/.worktrees/issue-316-typed-arrow-append/data
$ mise run gate
[e2e] $ cargo run -p lake-cli -- selftest
[test] $ cargo test --workspace --all-targets
[hooks] $ prek run --all-files
[test-adbc] $ cargo test -p lake-query --test adbc_interop -- --ignored --test-threads=1
[site-check] $ bun run --cwd site check

[hooks] Finished in 66.6ms
[test-adbc] test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out; finished in 1.66s
[site-check] Result (24 files):
[site-check] - 0 errors
[site-check] - 0 warnings
[site-check] - 0 hints
[site-check] All matched files use Prettier code style!
[site-check] 90 page(s) built in 3.09s
[site-check] Tests:        107 passed, 107 total

[e2e] created table robots.episodes
[e2e] committed robots.episodes at v2
[e2e] +----------+----------+------------+
[e2e] | robot_id | episodes | avg_reward |
[e2e] +----------+----------+------------+
[e2e] | alpha    | 2        | 0.8        |
[e2e] | beta     | 1        | 0.4        |
[e2e] +----------+----------+------------+
[e2e] self-check ok

[test] Running unittests src/lib.rs (.../lake_sdk-1e4363291a75de51)
[test] running 73 tests
[test] test append_checkpoint::tests::durable_checkpoint_accepts_maximum_typed_append_partition ... ok
[test] test tests::sdk_typed_arrow_append_preserves_dictionary_encoding ... ok
[test] test tests::sdk_typed_arrow_append_rejects_unbounded_inputs_before_schema_rpc ... ok
[test] test tests::sdk_typed_arrow_append_stops_encoding_at_payload_limit ... ok
[test] test tests::sdk_typed_arrow_append_checkpoint_partition_boundary_is_consistent ... ok
[test] test tests::sdk_typed_arrow_append_rejects_invalid_batches_before_put ... ok
[test] test tests::sdk_typed_arrow_append_commits_episode_artifact_bundle ... ok
[test] test tests::sdk_typed_arrow_append_reuses_durable_idempotent_transport ... ok
[test] test result: ok. 70 passed; 0 failed; 3 ignored; 0 measured; 0 filtered out; finished in 11.73s
Finished in 36.04s
exit: 0
```

The macOS linker emitted its existing `__eh_frame section too large` warning; it did not
change command status and direct clippy with `-D warnings` remained clean.

### Direct Rust checks

```text
$ cargo +nightly fmt --all -- --check
exit: 0

$ cargo clippy -p lake-sdk --all-targets --all-features --no-deps -- -D warnings
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.66s
exit: 0
```

### Lane-1 lifecycle and zero-match guard

```text
$ mise run spec-lifecycle specs/issue-316-typed-arrow-append.spec.md
[spec-lifecycle] $ bun scripts/spec-lifecycle-guard.ts "${usage_spec?}"
=== Lifecycle Report (guarded) ===
Spec: typed-arrow-append  stage: complete  passed: true
  [PASS] Episode and ArtifactRef rows append through a Query-only SDK
  [PASS] invalid Arrow input fails before append side effects
  [PASS] Arrow input memory and batch fan-out are bounded before schema lookup
  [PASS] encoded Flight collection stops at the exact payload ceiling
  [PASS] compact Dictionary input preserves its exact Flight schema
  [PASS] an ambiguous Arrow append converges without a duplicate commit
  [PASS] checkpointing accepts the same maximum batch partition as memory-only preparation
  [PASS] checkpoint framing accepts the exact dictionary-aware message ceiling
spec-lifecycle-guard: OK — every Test selector executed >=1 test
exit: 0
```

Each selector was also run directly; every command selected exactly one real test.

```text
$ cargo test -p lake-sdk sdk_typed_arrow_append_commits_episode_artifact_bundle
running 1 test
test tests::sdk_typed_arrow_append_commits_episode_artifact_bundle ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 72 filtered out

$ cargo test -p lake-sdk sdk_typed_arrow_append_rejects_invalid_batches_before_put
running 1 test
test tests::sdk_typed_arrow_append_rejects_invalid_batches_before_put ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 72 filtered out

$ cargo test -p lake-sdk sdk_typed_arrow_append_rejects_unbounded_inputs_before_schema_rpc
running 1 test
test tests::sdk_typed_arrow_append_rejects_unbounded_inputs_before_schema_rpc ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 72 filtered out

$ cargo test -p lake-sdk sdk_typed_arrow_append_stops_encoding_at_payload_limit
running 1 test
test tests::sdk_typed_arrow_append_stops_encoding_at_payload_limit ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 72 filtered out

$ cargo test -p lake-sdk sdk_typed_arrow_append_preserves_dictionary_encoding
running 1 test
test tests::sdk_typed_arrow_append_preserves_dictionary_encoding ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 72 filtered out

$ cargo test -p lake-sdk sdk_typed_arrow_append_reuses_durable_idempotent_transport
running 1 test
test tests::sdk_typed_arrow_append_reuses_durable_idempotent_transport ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 72 filtered out

$ cargo test -p lake-sdk sdk_typed_arrow_append_checkpoint_partition_boundary_is_consistent
running 1 test
test tests::sdk_typed_arrow_append_checkpoint_partition_boundary_is_consistent ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 72 filtered out

$ cargo test -p lake-sdk durable_checkpoint_accepts_maximum_typed_append_partition
running 1 test
test append_checkpoint::tests::durable_checkpoint_accepts_maximum_typed_append_partition ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 72 filtered out
```

### Fixed-base transition

The fixed base was checked in an isolated workspace/target, never against the candidate's
shared cache. The actual `cargo metadata` target was verified as isolated. At
`3729455699c7d9ed28b7b57263ab8abf5a283a50`, all eight exact filters reported:

```text
running 0 tests
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; filtered out
```

The guarded lifecycle therefore failed closed on all eight selectors:

```text
spec-lifecycle-guard: FAIL — Test selector(s) matched ZERO tests (0 passed; filtered out):
Every lane-1 Test: selector must resolve to >=1 real test function — see specs/README.md.
exit: 1
```

No base workspace, base target, probe workspace, probe target, or generated `data/`
remained after verification. In particular, the isolated work did not write
`/Users/ryan/Library/Caches/lake/target`.

## Transition matrix

- fail_to_pass: 8/8 required selectors changed from zero-match at fixed base (and guarded
  lifecycle rejection) to one executed passing test each at product head; guarded lifecycle
  changed from FAIL to `stage: complete  passed: true`.
- pass_to_fail: 0. The fresh candidate gate passed the complete pre-existing workspace suite,
  ADBC interoperability suite, site checks, and real e2e self-check. Scalar `insert` and
  `insert_many` coverage remained green inside the 70-passing `lake-sdk` suite.

## Probes

### Compact hostile Dictionary wire

- Input: 10,000 `Int32` keys all reference one CJK UTF-8 dictionary value of approximately
  8 KiB (`字` repeated); physical Arrow buffers are below 64 MiB, while hydration would be
  `8,190 * 10,000 = 81,900,000` bytes, above 64 MiB.
- Expected: `DictionaryHandling::Resend`; compact schema/dictionary/record messages; decoded
  schema and `RecordBatch` exactly equal the input.
- Observed: `sdk_typed_arrow_append_preserves_dictionary_encoding` passed. The test asserts
  physical input `< 64 MiB`, hypothetical hydrated bytes `> 64 MiB`, successful bounded
  encoding, exact decoded Dictionary schema, and exact decoded batch. Source inspection pins
  the encoder to `DictionaryHandling::Resend`; the bounded collector accounts protobuf
  `encoded_len()` incrementally. PASS.

### Dictionary-node and message-count boundaries

- Input: 17 top-level Dictionary fields with an unreachable schema endpoint; recursive nested
  Dictionary shapes; boundary of 16 nodes.
- Expected: 17 rejects as typed `BatchDictionaryCount { actual: 17, maximum: 16 }` before schema
  RPC; 16 is accepted by local validation.
- Observed: the hostile selector passed with the unreachable endpoint and exact 17/16 typed
  error. Inspection of the exercised validator shows a bounded explicit traversal through
  List/ListView/FixedSizeList/LargeList/LargeListView/Map/Struct/Union/Dictionary/
  RunEndEncoded nodes and rejects only when `dictionaries > 16`, so nested 17 rejects and exact
  16 remains admissible before RPC. PASS.

- Formula: `1 + 10,000 * (1 + 16) = 170,001` Flight messages.
- Checkpoint framing: `4 + 170,001 * 4 = 680,008` bytes, below the 1 MiB overhead budget.
- Observed: `durable_checkpoint_accepts_maximum_typed_append_partition` saved and loaded all
  170,001 messages byte-for-byte. Both checkpoint encode and decode reject counts greater than
  `MAX_APPEND_FLIGHT_MESSAGES`; the bounded Flight collector applies the same maximum. PASS.

### Remaining hostile corpus and regressions

- Empty input and a later zero-row batch: typed reject; selector passed before schema RPC.
- One-row and exact 10,000-row boundaries: accepted; exact 10,000-row CJK Dictionary probe
  encoded/decoded, while ordinary one-row append coverage passed in the SDK suite.
- `Binary`/`Utf8` physical buffer over 64 MiB: the generic per-array
  `get_buffer_memory_size()` saturating sum rejects before schema lookup; the >64 MiB Binary
  reproducer passed and the same type-independent guard covers Utf8.
- Encoded overflow: second message rejected and observable third message was never polled
  (`polled == 2`); selector passed.
- Partition parity: 4,096 one-row batches encoded to 4,097 messages; memory-only and durable
  preparation both succeeded and reload was byte-for-byte exact.
- Schema metadata mismatch: exact `Schema` equality (including metadata) rejects before DoPut;
  schema-mismatch/table-mismatch selector passed without publication.
- Ambiguous retry: same operation identity, digest, payload, and checkpoint reused; two attempts
  converged on version 2, one commit, then removed the checkpoint.
- Episode/ArtifactRef e2e: Query-only public `append_batches` committed one version and public SQL
  read back exactly one Episode plus every ArtifactRef.
- Scalar `insert`/`insert_many`: full SDK/gate regression suite remained green. PASS.

## Verdict

PASS — exact product head `f2fd5ad379c01a111c893b631a04edfc718bc08b` passes the fresh full gate, direct Rust checks, all 8 guarded lane-1 criteria, cold-boot write/read path, hostile Dictionary and bound checks, with `pass_to_fail = 0`.
