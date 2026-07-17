# Verification report — issue #185

- `base_sha`: `d1c622a5a50792a8c81e4805a0dc30a38d2e9b05`
- `head_sha`: `cca6dccd1db5fe289b1b39db538b48bed40043af`
- `score_authority`: verifier
- `implementer_evidence`: self_check_only
- `workspace`: `/Users/ryan/code/rararulab/lake/.worktrees/issue-185-guarded-operation-gc-test`

## Scope

The candidate changes only `crates/lake-metasrv/src/lib.rs` and adds
`specs/issue-185-guarded-operation-gc-test.spec.md`. The code change is in the
existing test: ordinary maintenance uses a positive, test-only operation
retention and asserts that it preserves the three operation records before the
explicit synthetic GC pass. No production GC default, protocol, or timing
semantics changed.

`git diff --check d1c622a5a507 cca6dccd1db5` exited successfully. `jj diff
--from d1c622a5a507 --to cca6dccd1db5 --summary` reported:

```text
M crates/lake-metasrv/src/lib.rs
A specs/issue-185-guarded-operation-gc-test.spec.md
```

## Commands and results

Candidate workspace was clean before verification:

```text
The working copy has no changes.
Parent commit (@-): vrvxvvon cca6dccd test(metasrv): use readable GC test retention (#185)
```

| Command | Result |
| --- | --- |
| `mise run doctor` | PASS — all checks passed. |
| `mise run spec-lifecycle specs/issue-185-guarded-operation-gc-test.spec.md` | PASS — `Spec guarded-operation-gc-test stage complete passed true`; `spec-lifecycle-guard OK every selector executed >=1`. |
| `cargo nextest run -p lake-metasrv -E 'test(production_metadata_mutations_use_guarded_store)'` | PASS — `Summary 1 test run: 1 passed, 87 skipped`. |
| `cargo clippy -p lake-metasrv --all-targets -- -D warnings` | PASS — exited 0; `Finished dev profile [unoptimized + debuginfo] target(s) in 1.01s`. |
| `rm -rf data && mise run gate` | PASS — fresh e2e created `robots.episodes`, committed a version, and reported `self-check: ok`; ADBC tests passed; workspace tests completed with `[test] Finished in 90.43s` and `Finished in 90.44s`. |

## Transition matrix

| Transition | Observation | Result |
| --- | --- | --- |
| expected fail-to-pass | At the base, forcing the pre-sweep wall clock into the last 70 ms of a second made the exact selector fail on the first attempt: `operation GC must exercise guarded deletes`. At the head, the same probe passed 8/8 attempts. | observed |
| pass-to-fail | Candidate target test, spec lifecycle, clippy, fresh-data gate, e2e, ADBC, and workspace test suite all passed. | 0 observed |

## Hostile probes

1. **Baseline boundary reproduction.** A separate workspace at
   `d1c622a5a50792a8c81e4805a0dc30a38d2e9b05` first ran the selector 16 times
   normally (all passed), confirming that the fault is timing-sensitive rather
   than a deterministic assertion failure. The following second-boundary probe
   then failed on attempt 1:

   ```text
   while true; do now=$(date +%N); if (( 10#$now >= 930000000 )); then break; fi; done
   target/debug/deps/lake_metasrv-2176cb5ef41e61c4 tests::production_metadata_mutations_use_guarded_store --exact --nocapture

   thread 'tests::production_metadata_mutations_use_guarded_store' panicked at crates/lake-metasrv/src/lib.rs:2022:9:
   operation GC must exercise guarded deletes
   test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 82 filtered out
   ```

2. **Candidate boundary repetition.** The identical eight-attempt probe against
   the candidate test binary, each attempt started at `>= 930 ms` in the
   current second, produced eight instances of:

   ```text
   test tests::production_metadata_mutations_use_guarded_store ... ok
   test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 82 filtered out
   ```

3. **Fresh-state integration.** `rm -rf data && mise run gate` exercised the
   cold-boot e2e route as well as ADBC and the complete workspace suite. The
   fresh e2e run completed `ingest -> commit -> SQL` with `self-check: ok`.

The root cause is confirmed by the production maintenance condition:
`now.saturating_sub(record.updated_at) <= operation_retention.as_secs()`. With
the old zero-second test retention, records written in one wall-clock second
became eligible if the ordinary sweep began in the next. A one-minute retention
keeps ordinary maintenance from consuming the records; the explicit
`u64::MAX` GC assertion still exercises guarded deletion deterministically.

## Environment note

macOS emitted the existing linker warning that the `__eh_frame` section is too
large for compact unwind encoding. It is a warning only; every command above
exited successfully.

## Verdict

**PASS.** The required fail-to-pass transition is reproduced against the base
and eliminated at the candidate head. The change stays within the test-only
boundary and the independent full quality gate is green.
