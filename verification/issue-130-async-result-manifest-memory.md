# Verification report — issue #130

- base_sha: `990146066bbcca06640489cd995569887ec7636c`
- head_sha: `2a0e2a23db287f0e6911c3dcb06bf2997ae8786d`
- score_authority: verifier
- implementer_evidence: self_check_only

This is an independent S3 re-verification. The workspace's `@` is the empty
commit `03fb0484`; its parent (`@-`) is the candidate above. The colocated
Git worktree resolves its top level to the repository's main checkout, so
`git HEAD` is deliberately not used as the artifact identity. Every range
command below names `base_sha` and `head_sha` explicitly, while every build
ran from this workspace, whose source tree is the empty child of `head_sha`.

## Commands

```text
$ mise run doctor
[ ok ] mise tools installed
[ ok ] nightly rustfmt
[ ok ] cargo check
[ ok ] jj repo: /Users/ryan/code/rararulab/lake/.worktrees/issue-130-async-result-manifest-memory
[ ok ] gh authenticated
[ ok ] git remote: origin

$ jj st
The working copy has no changes.
Working copy  (@) : ptkwuukt 03fb0484 (empty) (no description set)
Parent commit (@-): kosukpxr 2a0e2a23 fix(objects): validate S3 stage URI components (#130)

$ git merge-base 990146066bbcca06640489cd995569887ec7636c 2a0e2a23db287f0e6911c3dcb06bf2997ae8786d
990146066bbcca06640489cd995569887ec7636c

$ git rev-parse 2a0e2a23db287f0e6911c3dcb06bf2997ae8786d
2a0e2a23db287f0e6911c3dcb06bf2997ae8786d

$ git diff --name-status 990146066bbcca06640489cd995569887ec7636c 2a0e2a23db287f0e6911c3dcb06bf2997ae8786d
M       crates/lake-objects/src/s3.rs
M       crates/lake-query/src/async_query.rs
A       specs/issue-130-async-result-manifest-memory.spec.md
A       verification/issue-130-async-result-manifest-memory.md

$ git diff --check 990146066bbcca06640489cd995569887ec7636c 2a0e2a23db287f0e6911c3dcb06bf2997ae8786d
exit_status: 0
```

Before the gate, the workspace `data/` directory was absent (and would have
been relocated to `/tmp` if present), so the gate's e2e task booted without a
previous RocksDB, manifest, or object pointer.

```text
$ mise run gate
[hooks] Finished in 104.4ms
[site-install] Finished in 124.0ms
[site-check] Test Files  2 passed (2)
[site-check] Tests  5 passed (5)
[site-check] ✓ built in 2.19s
[site-check] Finished in 4.22s
[e2e] created table robots.episodes
[e2e] committed robots.episodes at v2
[e2e] | alpha    | 2        | 0.8        |
[e2e] | beta     | 1        | 0.4        |
[e2e] self-check ok
[e2e] Finished in 8.77s
[test-adbc] test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out; finished in 2.08s
[test] test result: ok. 27 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.75s
[test] test result: ok. 92 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 5.46s
[test] test result: ok. 44 passed; 0 failed; 2 ignored; 0 measured; 0 filtered out; finished in 11.69s
[test] Finished in 37.48s
Finished in 37.48s
exit_status: 0

$ mise run spec-lifecycle specs/issue-130-async-result-manifest-memory.spec.md
=== Lifecycle Report (guarded) ===
Spec: async-result-manifest-memory  stage: complete  passed: true
  [PASS] A part-sized manifest declaration is rejected before object I/O
  [PASS] Escaped URI bytes are rejected before manifest serialization
  [PASS] The maximum JSON-safe manifest structure fits the fixed ceiling
  [PASS] S3 stage construction rejects unsafe URI components before I/O
  [PASS] The existing async-query lifecycle remains valid
spec-lifecycle-guard: OK — every Test selector executed >=1 test
exit_status: 0
```

The five spec selectors and all four `async_result_manifest_*` selectors were
then run individually, using fully qualified exact names to show that each
selector bound precisely one test rather than a zero-match cargo success.

```text
$ cargo test -p lake-query async_query::tests::async_result_manifest_rejects_part_sized_location_before_read -- --exact
running 1 test
test async_query::tests::async_result_manifest_rejects_part_sized_location_before_read ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 91 filtered out; finished in 0.05s

$ cargo test -p lake-query async_query::tests::async_result_manifest_rejects_json_escaped_uri_before_serialization -- --exact
running 1 test
test async_query::tests::async_result_manifest_rejects_json_escaped_uri_before_serialization ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 91 filtered out; finished in 0.00s

$ cargo test -p lake-query async_query::tests::async_result_manifest_maximum_json_safe_structure_fits_ceiling -- --exact
running 1 test
test async_query::tests::async_result_manifest_maximum_json_safe_structure_fits_ceiling ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 91 filtered out; finished in 0.93s

$ cargo test -p lake-query async_query::tests::async_result_manifest_publishes_only_after_bounded_parts -- --exact
running 1 test
test async_query::tests::async_result_manifest_publishes_only_after_bounded_parts ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 91 filtered out; finished in 0.29s

$ cargo test -p lake-objects s3::pipeline_tests::s3_stage_rejects_unsafe_uri_components_before_io -- --exact
running 1 test
test s3::pipeline_tests::s3_stage_rejects_unsafe_uri_components_before_io ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 26 filtered out; finished in 0.28s
exit_status: 0
```

For a direct constructor probe outside the repository, an ephemeral Cargo
program linked to this candidate's `lake-objects`, used a client with the
unreachable endpoint `http://127.0.0.1:1`, and only called synchronous
`S3ObjectStore::new`/`stage_identity`—no upload, multipart, GET, or object
method was invoked.

```text
$ cargo run                 # /tmp/lake-issue-130-s3-probe
Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.18s
Running `target/debug/lake-issue-130-s3-probe`
constructor probes passed: rejected ?, #, %2, %GG, non-ASCII; preserved %7E and %bad
exit_status: 0
```

## Transition matrix

- fail_to_pass:
  - The explicit base source accepts any nonzero manifest declaration through
    `MAX_RESULT_PART_BYTES` (64 MiB), opens the verified reader, and reserves
    `Vec::with_capacity(capacity)`. The candidate checks
    `valid_manifest_location` before capacity conversion or `open_verified`;
    the part-sized candidate probe observed `InvalidJobSpec` and its counting
    object store observed `opens == 0`.
  - The explicit base S3 constructor rejected only empty bucket/prefix. The
    candidate's URI grammar rejects unsafe components before retaining the
    client binding. The direct constructor probe observed rejection of `?`,
    `#`, `%2`, `%GG`, and non-ASCII, while the S3 selector also covers space,
    quote, backslash, and the same no-I/O construction path.
  - Four manifest selectors and the S3 selector are absent at `base_sha`; all
    five exist at `head_sha`, were guard-checked by spec lifecycle, and each
    independently ran exactly one passing test.
- pass_to_fail: 0. The pre-existing compatible async lifecycle passed its
  selector, and the fresh full workspace gate passed.

## Probes

1. **Part-ceiling declaration before I/O.** A completed record declared a
   `67,108,864`-byte manifest and used an object store that counts opens.
   Expected: reject before reader open/allocation. Observed:
   `InvalidJobSpec`, `opens == 0`. **PASS**.
2. **Escaping amplification.** A part URI contains 4,096 U+0000 bytes.
   Expected: reject before JSON serialization/publication. Observed: the
   dedicated selector returned the expected `ResultBound`. **PASS**.
3. **Maximum valid structure.** 4,096 JSON-safe 4,096-byte URIs, a 1 MiB
   schema, maximum summary fields, and valid lower-case digests were encoded.
   Expected: at most 21,684,406 bytes, below the fixed 32 MiB manifest ceiling
   and 64 MiB part ceiling. Observed: selector passed its exact bound checks.
   **PASS**.
4. **S3 raw URI syntax and identity, before I/O.** `?`, `#`, `%2`, `%GG`, and
   non-ASCII prefixes were rejected by `new`; `%7E` produced exactly
   `s3://lake-managed/tenants/%7Etenant-a/objects`. `%bad` is correctly
   accepted as the valid `%ba` escape followed by the unreserved `d`, and is
   preserved without decoding. **PASS**.
5. **Cold e2e write→read.** A new local data store created
   `robots.episodes`, committed v2, then read SQL aggregate results with
   `self-check ok`. **PASS**.

## Findings

- P0: none.
- P1: none.

## Verdict

PASS — candidate `2a0e2a23` passes the clean full gate, all five guarded spec
criteria, every manifest selector, the S3 constructor selector, cold
write→commit→SQL read, and the requested hostile constructor probes. The
manifest reader and publisher enforce the separate 32 MiB structural limit;
the S3 stage rejects unsafe raw URI components before object I/O while
preserving valid raw `%7E` identity.
