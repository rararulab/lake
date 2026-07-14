# Independent verification report — issue #162

- verifier: independent S3 verifier (fresh context; implementer report/evidence not read)
- base_sha: `00535ac90fb608004cacc6548a0e0afec3bb99ba` (`origin/main`)
- head_sha: `c0e2d81d87b8cae1c33fb7e0cd6b4eb5258dfe5e` (clean candidate working-copy commit)
- implementation_commit: `8f503e6e37a4accffb9f14f231b6eddb424c54af` (`head_sha`'s parent; contains #162)
- score_authority: verifier
- implementer_evidence: self_check_only

The candidate was clean before verification (`jj st`: `The working copy has no
changes`). The checked-out jj working-copy commit is intentionally empty; the
implementation is its committed parent. `jj diff --summary -r @-` reports only
the four allowed paths: `lake-metasrv/src/control.rs`, `lake-query/src/flight.rs`,
`lake-query/tests/file_append_proxy.rs`, and this task spec.

## Commands

Raw command summaries below retain the executed command and terminal result.
The recurring macOS linker warning (`__eh_frame section too large`) is a warning
only; no command emitted a test or task failure.

```text
$ mise run doctor
[doctor] $ bun scripts/doctor.ts
[ ok ] mise tools installed
[ ok ] nightly rustfmt

$ rm -rf data && mise run gate
[hooks] $ prek run --all-files
[test] $ cargo test --workspace --all-targets
[test-adbc] $ cargo test -p lake-query --test adbc_interop -- --ignored
[e2e] $ cargo run -p lake-cli -- selftest
[site-check] $ bun run --cwd site check
... all observed `test result:` summaries were `ok`; ADBC: 3 passed;
site: `Test Files 2 passed (2)`, `Tests 5 passed (5)`, `✓ built`;
e2e: `self-check ok`.

$ mise run spec-lifecycle specs/issue-162-ipc-decode-memory.spec.md
=== Lifecycle Report (guarded) ===
Spec: ipc-decode-memory  stage: complete  passed: true
  [PASS] A declared body length mismatch fails before any table version changes
  [PASS] Compressed FILE append IPC fails before a table append
  [PASS] Query-forwarded compressed FILE append is rejected by Metasrv
  [PASS] Bounded uncompressed FILE append remains supported
spec-lifecycle-guard: OK — every Test selector executed >=1 test

$ cargo test -p lake-metasrv file_append_rejects_declared_body_mismatch_before_commit
running 1 test
test control::file_append_tests::file_append_rejects_declared_body_mismatch_before_commit ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 82 filtered out

$ cargo test -p lake-metasrv file_append_rejects_compressed_ipc_before_commit
running 1 test
test control::file_append_tests::file_append_rejects_compressed_ipc_before_commit ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 82 filtered out

$ cargo test -p lake-query --test file_append_proxy query_forwarded_file_append_rejects_compressed_ipc
running 1 test
test query_forwarded_file_append_rejects_compressed_ipc ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out

$ cargo test -p lake-metasrv file_append_commits_decoded_flight_batches
running 1 test
test control::file_append_tests::file_append_commits_decoded_flight_batches ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 82 filtered out

$ rm -rf data && cargo run -p lake-cli -- selftest
created table robots.episodes
committed robots.episodes at v2
| robot_id | episodes | avg_reward |
| alpha    | 2        | 0.8        |
| beta     | 1        | 0.4        |
self-check ok
```

The explicit CLI run is the required cold boot: it removed this workspace's
`data/`, then created, ingested, committed, and queried the candidate's local
RocksDB/Lance state in one session. It did not reuse the gate's later `data/`.

## Transition matrix

- fail_to_pass: At base `00535ac90`, the four #162 selector identities and the
  `validate_file_append_ipc` validation do not exist (observed with
  `jj diff -r @-` against the base). At head, each new selector matched exactly
  one test and passed under the guarded lifecycle and again when invoked
  directly. The negative cases exercise rejection before the version changes;
  the positive case commits the decoded uncompressed stream. This is the
  expected base-absent → head-verified transition for newly introduced
  regression tests; no zero-match was accepted as evidence.
- pass_to_fail: 0. The full workspace gate, ignored ADBC interop suite, cold
  boot, existing successful uncompressed append selector, and added selectors
  all passed.

## Hostile probes

```text
$ cargo test -p lake-metasrv configured_append_stream_limit_rejects_before_commit
test control::file_append_tests::configured_append_stream_limit_rejects_before_commit ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 82 filtered out

$ cargo test -p lake-metasrv concurrent_replays_execute_one_engine_append
test control::file_append_tests::concurrent_replays_execute_one_engine_append ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 82 filtered out

$ cargo test -p lake-metasrv same_operation_replay_returns_original_version
test control::file_append_tests::same_operation_replay_returns_original_version ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 82 filtered out
```

- Boundary control payload: a configured one-byte stream limit rejects before
  commit and leaves no partial version. PASS.
- Concurrency: two simultaneous replays perform one engine append. PASS.
- Idempotency: replay of the same operation returns the original version,
  preserving the append protocol after the new pre-decode validation. PASS.

## Verdict

PASS — the clean candidate passes the full gate, guarded spec lifecycle and all
four selectors; cold-boot ingest → commit → SQL succeeds; and the boundary,
concurrency, and replay probes preserve the append contract with no observed
regression.
