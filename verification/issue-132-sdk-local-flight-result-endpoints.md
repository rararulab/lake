# Verification report — issue #132

- base_sha: 990146066bbcca06640489cd995569887ec7636c
- head_sha: 682382dd66747b8b1af110e5686740fd3f3881b6
- score_authority: verifier
- implementer_evidence: self_check_only

This is a clean re-verification after the single repair commit. No
implementer hand-off or self-attested evidence was read.

## Candidate pin and repair scope

~~~text
$ jj st
The working copy has no changes.
Working copy  (@) : znpsslyu f1431f46 (empty) (no description set)
Parent commit (@-): nzkswklu 682382dd docs(sdk): document query result stream semantics (#132)

$ git rev-parse 990146066bbcca06640489cd995569887ec7636c
990146066bbcca06640489cd995569887ec7636c
$ git rev-parse 682382dd
682382dd66747b8b1af110e5686740fd3f3881b6
$ git merge-base 990146066bbcca06640489cd995569887ec7636c 682382dd
990146066bbcca06640489cd995569887ec7636c

$ git diff --numstat 977f7279 682382dd
30	8	crates/lake-sdk/src/lib.rs
249	0	verification/issue-132-sdk-local-flight-result-endpoints.md
~~~

The 30/8 SDK hunk changes only Rustdoc attached to the three public contract
surfaces: QueryResultStream, AsyncQueryResultStream, and LakeClient::query.
A raw source diff found no executable-token change. The documentation now
states complete declared-order local/reuse consumption, whole-result streaming
without collection, the deliberate FlightRecordBatchStream semver migration,
typed redacted pre-DoGet failure, no remote routing/credential forwarding, and
unchanged async-manifest behavior. Thus P1 is a public API-contract
documentation repair with no runtime change.

## Commands

### Clean full gate and cold boot

~~~text
$ rm -rf data && mise run gate
[hooks] $ prek run --all-files
[test] $ cargo test --workspace --all-targets
[test-adbc] $ cargo test -p lake-query --test adbc_interop -- --ignored
[e2e] $ cargo run -p lake-cli -- selftest
[site-check] $ bun run --cwd site check
[e2e] created table robots.episodes
[e2e] committed robots.episodes at v2
[e2e] | robot_id | episodes | avg_reward |
[e2e] | alpha    | 2        | 0.8        |
[e2e] | beta     | 1        | 0.4        |
[e2e] self-check ok
[e2e] Finished in 15.71s
[test-adbc] test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out; finished in 1.88s
[test] test tests::sdk_query_consumes_single_and_ordered_local_reuse_endpoints ... ok
[test] test tests::sdk_query_rejects_excessive_endpoint_metadata_before_doget ... ok
[test] test tests::sdk_query_rejects_external_location_before_doget ... ok
[test] test tests::sdk_query_rejects_missing_ticket_before_doget ... ok
[test] test tests::sdk_query_result_stream_supports_try_stream_consumption ... ok
[test] test result: ok. 49 passed; 0 failed; 2 ignored; 0 measured; 0 filtered out; finished in 11.75s
[test] Finished in 43.51s
Finished in 43.53s
exit: 0
~~~

The macOS compact-unwind linker warning was emitted but no command reported an
error or test failure. mise run ship was not run because it pushes and this
verification is forbidden to push.

### Lane-1 lifecycle

~~~text
$ mise run spec-lifecycle specs/issue-132-sdk-local-flight-result-endpoints.spec.md
=== Lifecycle Report (guarded) ===
Spec: sdk-local-flight-result-endpoints  stage: complete  passed: true
  [PASS] SDK consumes one and ordered local/reuse endpoint results
  [PASS] Missing endpoint ticket fails before any redemption
  [PASS] Endpoint count and ticket metadata are bounded before redemption
  [PASS] External endpoint locations fail closed without credential disclosure
  [PASS] QueryResultStream preserves normal streaming ergonomics
spec-lifecycle-guard: OK — every Test selector executed >=1 test
exit: 0
~~~

### Every bound selector

~~~text
$ cargo test -p lake-sdk sdk_query_consumes_single_and_ordered_local_reuse_endpoints
test tests::sdk_query_consumes_single_and_ordered_local_reuse_endpoints ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 50 filtered out; finished in 0.11s

$ cargo test -p lake-sdk sdk_query_rejects_missing_ticket_before_doget
test tests::sdk_query_rejects_missing_ticket_before_doget ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 50 filtered out; finished in 0.11s

$ cargo test -p lake-sdk sdk_query_rejects_excessive_endpoint_metadata_before_doget
test tests::sdk_query_rejects_excessive_endpoint_metadata_before_doget ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 50 filtered out; finished in 0.12s

$ cargo test -p lake-sdk sdk_query_rejects_external_location_before_doget
test tests::sdk_query_rejects_external_location_before_doget ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 50 filtered out; finished in 0.11s

$ cargo test -p lake-sdk sdk_query_result_stream_supports_try_stream_consumption
test tests::sdk_query_result_stream_supports_try_stream_consumption ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 50 filtered out; finished in 0.12s
all five selector commands exit: 0
~~~

## Hostile probe

Mixed local/reuse plus external capability location: the Flight mock gives one
endpoint both the exact reuse URI and
https://capability.example.invalid/credential=secret. Expected behavior is a
typed unsupported-location error before every DoGet, with neither URI nor
credential text in Display or Debug. The bound test asserts the zero-DoGet
counter and both redactions.

~~~text
$ cargo test -p lake-sdk tests::sdk_query_rejects_external_location_before_doget -- --exact
running 1 test
test tests::sdk_query_rejects_external_location_before_doget ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 50 filtered out; finished in 0.11s
exit: 0
~~~

## Transition matrix

- fail_to_pass: the previously live-observed base-to-candidate transition
  remains valid: base 990146... drained the same ordered empty/reuse mock as
  [2], while the predecessor runtime candidate drained [2, 3]. This repair
  changes only public Rustdoc, and the raw 977f7279..682382dd source diff has
  no runtime change; the authorized re-verification therefore did not repeat
  the costly dual-base harness.
- pass_to_fail: 0. The repaired candidate passed clean gate/cold-boot e2e,
  guarded lifecycle, every selector, and the renewed mixed-location probe.

## Verdict

PASS — head 682382dd has only the requested public-contract Rustdoc repair;
all fresh candidate verification is green, and no runtime behavior changed.
