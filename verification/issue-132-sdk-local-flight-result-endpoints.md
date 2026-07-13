# Verification report — issue #132

- base_sha: 990146066bbcca06640489cd995569887ec7636c
- head_sha: 977f72799eeeb80d118e21fe5f869f5fcbfb2ea2
- score_authority: verifier
- implementer_evidence: self_check_only

Workspace is a clean empty jj child of the candidate (@- = 977f7279); its
source tree matched the candidate. The supplied candidate descends from the
supplied base (merge_base = 990146066bbcca06640489cd995569887ec7636c). No
origin/main ref was used.

## Scope and artifact audit

~~~text
$ jj st
The working copy has no changes.
Working copy  (@) : ssmmrrlu b68115c7 (empty) (no description set)
Parent commit (@-): kvqmswpl 977f7279 fix(sdk): consume all local Flight result endpoints (#132)

$ git diff --name-status 990146066bbcca06640489cd995569887ec7636c 977f7279
M	crates/lake-sdk/src/lib.rs
A	specs/issue-132-sdk-local-flight-result-endpoints.spec.md
~~~

The candidate validates every endpoint before it constructs the sequential
try_flatten stream. It accepts only empty locations or the exact reuse URI;
ticket count, non-empty/per-ticket size, and overflow-safe aggregate-size
checks occur in that pre-stream pass. The stream retains only bounded
clients/tickets (at most 256), not result batches, and drives DoGet streams in
declared order. QueryResultStream exposes RecordBatch/FlightError, not raw
FlightData or per-DoGet headers/trailers. UnsupportedQueryResultLocation has
no URI, ticket, or credential field.

## Commands

### Environment and cold boot

~~~text
$ mise run doctor
[doctor] $ bun scripts/doctor.ts
[ ok ] mise tools installed
[ ok ] nightly rustfmt
[ ok ] cargo check
[ ok ] jj repo: /Users/ryan/code/rararulab/lake/.worktrees/issue-132-sdk-local-flight-result-endpoints
[ ok ] gh authenticated
[ ok ] git remote: origin

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
[e2e] Finished in 8.98s
[test-adbc] test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out; finished in 1.88s
[test] test tests::sdk_query_consumes_single_and_ordered_local_reuse_endpoints ... ok
[test] test tests::sdk_query_rejects_external_location_before_doget ... ok
[test] test tests::sdk_query_rejects_excessive_endpoint_metadata_before_doget ... ok
[test] test tests::sdk_query_rejects_missing_ticket_before_doget ... ok
[test] test tests::sdk_query_result_stream_supports_try_stream_consumption ... ok
[test] test result: ok. 49 passed; 0 failed; 2 ignored; 0 measured; 0 filtered out; finished in 11.79s
[hooks] Finished in 38.25s
[test] Finished in 38.25s
Finished in 38.25s
exit: 0
~~~

The linker emitted a macOS compact-unwind size warning; no command reported an
error or test failure. mise run ship was deliberately not run because it
pushes, and this verification was explicitly forbidden to push.

### Lane-1 lifecycle

~~~text
$ mise run spec-lifecycle specs/issue-132-sdk-local-flight-result-endpoints.spec.md
[spec-lifecycle] $ bun scripts/spec-lifecycle-guard.ts "${usage_spec?}"
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
running 1 test
test tests::sdk_query_consumes_single_and_ordered_local_reuse_endpoints ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 50 filtered out; finished in 0.11s
exit: 0

$ cargo test -p lake-sdk sdk_query_rejects_missing_ticket_before_doget
running 1 test
test tests::sdk_query_rejects_missing_ticket_before_doget ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 50 filtered out; finished in 0.10s
exit: 0

$ cargo test -p lake-sdk sdk_query_rejects_excessive_endpoint_metadata_before_doget
running 1 test
test tests::sdk_query_rejects_excessive_endpoint_metadata_before_doget ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 50 filtered out; finished in 0.12s
exit: 0

$ cargo test -p lake-sdk sdk_query_rejects_external_location_before_doget
running 1 test
test tests::sdk_query_rejects_external_location_before_doget ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 50 filtered out; finished in 0.11s
exit: 0

$ cargo test -p lake-sdk sdk_query_result_stream_supports_try_stream_consumption
running 1 test
test tests::sdk_query_result_stream_supports_try_stream_consumption ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 50 filtered out; finished in 0.11s
exit: 0
~~~

## Transition matrix

- fail_to_pass: observed. A standalone, throwaway black-box binary used one
  real Arrow Flight SQL mock with ordered=true and two result endpoints: the
  first has no locations and ticket first (batch value 2); the second has the
  exact arrow-flight-reuse-connection://? location and ticket second (batch
  value 3). It calls only the public LakeClient::connect_with_store and query
  APIs, then drains the returned stream. The real base returned [2] and
  redeemed only first. The real candidate returned [2, 3] and redeemed first
  then second. Both assertions passed.
- pass_to_fail: 0 observed. After the transition check, a clean candidate
  rerun of gate (including cold-boot e2e), lifecycle, and every bound selector
  exited 0 with no failing test.

## Base-to-candidate black-box transition

The base and candidate were compiled in separate temporary Cargo projects so
that each project resolves precisely one workspace's same-version path crates.
The first attempt to link both revisions in one Cargo lockfile failed with the
deterministic lake-common v0.0.1 package-collision diagnostic; it was replaced
by the two isolated projects, not by changing either Lake workspace. The
temporary harness source, Cargo lockfiles, target directories, and object
stores were outside the repository, were never committed or added to any Lake
diff, and were removed after their raw output was captured.

~~~text
$ HARNESS_LABEL=base cargo run --manifest-path /tmp/lake-issue-132-flight-base.CFDi7I/Cargo.toml -- 2
Finished dev profile [unoptimized + debuginfo] target(s) in 4.68s
Running /tmp/lake-issue-132-flight-base.CFDi7I/target/debug/issue-132-flight-transition-issue-132-verifier-base 2
base_batches=[2]
base_redeemed=["first"]
exit: 0

$ HARNESS_LABEL=candidate cargo run --manifest-path /tmp/lake-issue-132-flight-candidate.EfFOwY/Cargo.toml -- 2 3
Finished dev profile [unoptimized + debuginfo] target(s) in 1m 45s
Running /tmp/lake-issue-132-flight-candidate.EfFOwY/target/debug/issue-132-flight-transition-issue-132-sdk-local-flight-result-endpoints 2 3
candidate_batches=[2, 3]
candidate_redeemed=["first", "second"]
exit: 0
~~~

The only harness warning was an unused FlightData import; both binaries built
and executed successfully. The mock's endpoint set and the public draining
path are identical between runs.

### Fresh candidate rerun after transition

~~~text
$ rm -rf data && mise run gate
[e2e] created table robots.episodes
[e2e] committed robots.episodes at v2
[e2e] self-check ok
[e2e] Finished in 9.33s
[test-adbc] test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out; finished in 1.77s
[test] test result: ok. 49 passed; 0 failed; 2 ignored; 0 measured; 0 filtered out; finished in 11.94s
[test] Finished in 38.76s
Finished in 38.78s
exit: 0

$ mise run spec-lifecycle specs/issue-132-sdk-local-flight-result-endpoints.spec.md
Spec: sdk-local-flight-result-endpoints  stage: complete  passed: true
spec-lifecycle-guard: OK — every Test selector executed >=1 test
exit: 0

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
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 50 filtered out; finished in 0.11s
all selector commands exit: 0
~~~

## Probes

1. Malformed later endpoint: a valid first endpoint followed by a missing
   ticket. Expected typed MissingQueryTicket and zero DoGet calls. Observed:
   cargo test -p lake-sdk tests::sdk_query_rejects_missing_ticket_before_doget -- --exact
   ran 1 test and passed; 0 failures. PASS.
2. Aggregate boundary: 17 tickets of 512 KiB (8.5 MiB), exceeding 8 MiB.
   Expected typed invalid-endpoint error and zero DoGet calls. Observed:
   cargo test -p lake-sdk tests::sdk_query_rejects_excessive_endpoint_metadata_before_doget -- --exact
   ran 1 test and passed; 0 failures. PASS.
3. Mixed reuse and remote capability URI: endpoint locations include exact
   reuse plus https://capability.example.invalid/credential=secret. Expected
   unsupported-location, zero DoGet calls, and neither URI nor credential text
   in Display/Debug. Observed:
   cargo test -p lake-sdk tests::sdk_query_rejects_external_location_before_doget -- --exact
   ran 1 test and passed; the mock asserts both renderings and the DoGet
   counter. PASS.

Raw hostile-probe summaries:

~~~text
test tests::sdk_query_rejects_missing_ticket_before_doget ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 50 filtered out; finished in 0.17s
test tests::sdk_query_rejects_excessive_endpoint_metadata_before_doget ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 50 filtered out; finished in 0.12s
test tests::sdk_query_rejects_external_location_before_doget ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 50 filtered out; finished in 0.11s
all three exit: 0
~~~

## Verdict

PASS — the exact base-to-candidate black-box fail-to-pass transition is now
live-observed, and the clean candidate gate, cold-boot e2e, lifecycle, every
bound selector, and all hostile probes are green with pass_to_fail = 0.
