# Verification report — issue #149

- base_sha: 00535ac90fb608004cacc6548a0e0afec3bb99ba
- head_sha: 0eb44cb1cc22c2c4316facb83882482a60197dd6
- score_authority: verifier
- implementer_evidence: self_check_only

Target: the #149 delta over stacked parent 865ee761e3932049ea55a42d9dd947bf60fccd1d. The base SHA is the merge-base with origin/main. The colocated Git HEAD points at the root checkout and is intentionally not used as an anchor.

## Commands

```text
$ jj st
The working copy has no changes.
Working copy  (@) : woqyykwr b5f0af63 (empty) (no description set)
Parent commit (@-): ykwmlotv 0eb44cb1 issue-149-no-proxy-direct-reader | fix(sdk): bypass proxies for capability direct reads (#149)
```

```text
$ rm -rf data && jj st && mise run gate
[hooks] $ prek run --all-files
[e2e] $ cargo run -p lake-cli -- selftest
[test] $ cargo test --workspace --all-targets
[adbc-install] $ uv sync --project interop/adbc --frozen
[site-install] $ bun install --cwd site --frozen-lockfile
[hooks] Finished in 78.4ms
[test-adbc] test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out; finished in 1.53s
[site-check] Test Files  2 passed (2)
[site-check]       Tests  5 passed (5)
[site-check] ✓ built in 2.20s
[e2e] created table robots.episodes
[e2e] committed robots.episodes at v2
[e2e] self-check ok
[test] test result: ok. 56 passed; 0 failed; 2 ignored; 0 measured; 0 filtered out; finished in 11.54s
[test] Finished in 33.65s
Finished in 33.66s
exit: 0
```

```text
$ mise run spec-lifecycle specs/issue-149-no-proxy-direct-reader.spec.md
[spec-lifecycle] $ bun scripts/spec-lifecycle-guard.ts <spec>
=== Lifecycle Report (guarded) ===
Spec: no-proxy-direct-reader  stage: complete  passed: true
  [PASS] A configured proxy is bypassed for a direct capability request
  [PASS] Query-only full reads remain direct and integrity-verifying
  [PASS] Query-only range reads keep exact direct semantics
  [PASS] Capability failures remain fail-closed and redacted
spec-lifecycle-guard: OK — every Test selector executed >=1 test
exit: 0
```

```text
$ cargo test -p lake-sdk direct_read_client_bypasses_configured_proxy
running 1 test
test tests::direct_read_client_bypasses_configured_proxy ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 57 filtered out; finished in 0.01s
exit: 0

$ cargo test -p lake-sdk query_only_full_read_streams_and_verifies_without_stage_access
running 1 test
test tests::query_only_full_read_streams_and_verifies_without_stage_access ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 57 filtered out; finished in 0.11s
exit: 0

$ cargo test -p lake-sdk query_only_range_reader_requires_exact_partial_response
running 1 test
test tests::query_only_range_reader_requires_exact_partial_response ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 57 filtered out; finished in 0.11s
exit: 0

$ cargo test -p lake-sdk query_only_reader_fails_closed_and_redacts_capability
running 1 test
test tests::query_only_reader_fails_closed_and_redacts_capability ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 57 filtered out; finished in 0.21s
exit: 0
```

```text
$ rm -rf data && mise run e2e
[e2e] $ cargo run -p lake-cli -- selftest
created table robots.episodes
committed robots.episodes at v2
| robot_id | episodes | avg_reward |
| alpha    | 2        | 0.8        |
| beta     | 1        | 0.4        |
self-check ok
exit: 0
```

```text
$ cargo test -p lake-sdk query_only_range_reader_rejects_mismatched_partial_response
running 1 test
test tests::query_only_range_reader_rejects_mismatched_partial_response ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 57 filtered out; finished in 0.21s
exit: 0

$ cargo test -p lake-sdk sdk_remote_read_capability_uses_query_action_without_stage_store
running 1 test
test tests::sdk_remote_read_capability_uses_query_action_without_stage_store ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 57 filtered out; finished in 0.11s
exit: 0
```

The #149 selector does not exist in parent #143. A parent run matching zero tests was excluded from transition evidence. Instead, a verifier-only local harness was injected in an isolated temporary clone of the parent, ran, and deleted. It used the same local recording object/proxy topology while preserving the parent builder policy: redirect denial without no_proxy(). No candidate workspace file or candidate commit was modified.

```text
$ CARGO_TARGET_DIR=<candidate>/target cargo test -p lake-sdk verifier_proxy_policy_regression_baseline -- --nocapture
running 1 test
assertion `left == right` failed: a direct-read client must bypass its configured proxy
  left: 502
 right: 200
test tests::verifier_proxy_policy_regression_baseline ... FAILED
test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 57 filtered out; finished in 0.06s
exit: 101

$ cargo clean -p lake-sdk && cargo test -p lake-sdk direct_read_client_bypasses_configured_proxy
Removed 4326 files, 5.4GiB total
running 1 test
test tests::direct_read_client_bypasses_configured_proxy ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 57 filtered out; finished in 0.01s
exit: 0
```

## Transition matrix

- fail_to_pass: the isolated parent harness failed with recording-proxy HTTP 502 and exit 101. The candidate supplied-builder selector passed, observing a direct object response and zero proxy requests. The unavailable parent selector was not counted as a red result.
- pass_to_fail: 0. Full streamed identity verification, exact range semantics, and redirect/corrupt-response fail-closed redaction all passed.

## Probes

1. Off-by-one range response: request 3..7 with Content-Range bytes 3-7/10. Expected rejection after one Query capability action. Observed query_only_range_reader_rejects_mismatched_partial_response passed. PASS.
2. No managed-stage client: Query-only SDK with a remote capability service and no stage store. Expected exactly the capability action and no stage-client construction. Observed sdk_remote_read_capability_uses_query_action_without_stage_store passed. PASS.
3. Hostile object responses: redirect, then same-length corrupt bytes. Expected no redirect follow or Query fallback, EOF integrity failure, and redacted diagnostics. Observed query_only_reader_fails_closed_and_redacts_capability passed. PASS.

## Verdict

PASS — the clean cold-state gate, guarded spec lifecycle, every acceptance selector, cold end-to-end self-check, hostile probes, and the observable parent-502 to candidate-direct-bypass transition passed with pass_to_fail = 0.
