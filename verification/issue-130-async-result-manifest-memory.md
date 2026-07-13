# Verification report — issue #130

- base_sha: `990146066bbcca06640489cd995569887ec7636c`
- head_sha: `c395f303b18e42abc18f58b82f435b129123a0c9`
- score_authority: verifier
- implementer_evidence: self_check_only

The workspace is a Jujutsu checkout with an empty `@` commit.  Its candidate
is `@-` (`c395f303`); that object has parent and `merge-base` with
`origin/main` `990146066`.  The role-prescribed raw `git rev-parse HEAD` is
the stale colocated Git checkout `179a37826e0c4c62ebe341ac42988bfcd3f07e50`,
not the artifact present in this workspace.  The base/head above therefore
pin the actual candidate that was built and tested.

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
Working copy  (@) : nstoqzvy 1fbafb2c (empty) (no description set)
Parent commit (@-): ooqyxtpn c395f303 fix(query): bound async result manifest memory (#130)

$ git merge-base c395f303b18e42abc18f58b82f435b129123a0c9 origin/main
990146066bbcca06640489cd995569887ec7636c

$ git rev-parse c395f303b18e42abc18f58b82f435b129123a0c9
c395f303b18e42abc18f58b82f435b129123a0c9

$ git diff-tree --no-commit-id --name-status -r c395f303b18e42abc18f58b82f435b129123a0c9
M       crates/lake-query/src/async_query.rs
A       specs/issue-130-async-result-manifest-memory.spec.md

$ mise run gate
[hooks] Finished in 103.5ms
[site-check] Finished in 3.93s
[e2e] self-check ok
[e2e] Finished in 8.59s
[test] test result: ok. 92 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 5.47s
[test-adbc] test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out; finished in 1.88s
exit_status: 0

$ mise run spec-lifecycle specs/issue-130-async-result-manifest-memory.spec.md
=== Lifecycle Report (guarded) ===
Spec: async-result-manifest-memory  stage: complete  passed: true
  [PASS] A part-sized manifest declaration is rejected before object I/O
  [PASS] Escaped URI bytes are rejected before manifest serialization
  [PASS] The maximum JSON-safe manifest structure fits the fixed ceiling
  [PASS] The existing async-query lifecycle remains valid
spec-lifecycle-guard: OK — every Test selector executed >=1 test

$ cargo test -p lake-query async_result_manifest_rejects_part_sized_location_before_read
running 1 test
test async_query::tests::async_result_manifest_rejects_part_sized_location_before_read ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 91 filtered out; finished in 0.05s

$ cargo test -p lake-query async_result_manifest_rejects_json_escaped_uri_before_serialization
running 1 test
test async_query::tests::async_result_manifest_rejects_json_escaped_uri_before_serialization ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 91 filtered out; finished in 0.00s

$ cargo test -p lake-query async_result_manifest_maximum_json_safe_structure_fits_ceiling
running 1 test
test async_query::tests::async_result_manifest_maximum_json_safe_structure_fits_ceiling ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 91 filtered out; finished in 0.94s

$ cargo test -p lake-query async_result_manifest_publishes_only_after_bounded_parts
running 1 test
test async_query::tests::async_result_manifest_publishes_only_after_bounded_parts ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 91 filtered out; finished in 0.24s

$ rm -rf data && cargo run -p lake-cli -- selftest
created table robots.episodes
committed robots.episodes at v2
| robot_id | episodes | avg_reward |
| alpha    | 2        | 0.8        |
| beta     | 1        | 0.4        |
self-check ok

$ git diff --check 990146066bbcca06640489cd995569887ec7636c c395f303b18e42abc18f58b82f435b129123a0c9
exit_status: 0
```

## Transition matrix

- fail_to_pass: The four selector tests are introduced by `head_sha`, so their
  selectors are absent at `base_sha`.  The base `load_manifest` guard was
  observed to accept every nonzero declaration through the 64 MiB part ceiling
  and then call `open_verified`/`Vec::with_capacity`; the candidate's
  part-sized hostile probe returns `InvalidJobSpec` with zero opens.  The
  candidate selectors all executed exactly one test and passed, including the
  maximum serialized-structure assertion (`<= 21,684,406`, `< 32 MiB`,
  `< 64 MiB`).
- pass_to_fail: 0.  Full workspace gate, spec lifecycle, all four acceptance
  selectors, and a separate cold boot passed.

## Probes

1. **Oversized declared manifest, no object I/O.** Input: a completed record
   with a valid-shaped manifest `DataLocation` declaring `67,108,864` bytes
   (the Arrow part ceiling), backed by a counting object store. Expected:
   `InvalidJobSpec` before opening the reader or reserving that capacity.
   Observed: `async_result_manifest_rejects_part_sized_location_before_read`
   passed; the test asserts `opens == 0`. **PASS**.
2. **JSON-escaping adversary.** Input: a maximum-length (4,096-byte) part URI
   comprising U+0000 bytes, which serde would sixfold-expand. Expected:
   reject before serialization/publication. Observed:
   `async_result_manifest_rejects_json_escaped_uri_before_serialization`
   returned the expected bounded error and passed. **PASS**.
3. **Maximum valid structure and compatibility route.** Input: 4,096 parts,
   each with a 4,096-byte JSON-safe ASCII URI, 1 MiB schema bytes, maximum
   summary fields; additionally the normal lifecycle creates and reloads its
   managed local `file://` result URI. Expected: serialized size stays within
   the derived 21,684,406-byte bound and the compatible managed URI remains
   publishable/readable. Observed: the maximum-structure selector passed, and
   `async_result_manifest_publishes_only_after_bounded_parts` passed; the
   independent cold boot also completed a write-to-read CLI selftest. **PASS**.

## Findings

- P0: none.
- P1: none.

## Verdict

PASS — the actual Jujutsu candidate is clean, all mandatory gates and
acceptance commands passed, malformed manifest declarations are stopped before
object I/O, and the bounded/compatible async-result lifecycle works from a
fresh data directory.
