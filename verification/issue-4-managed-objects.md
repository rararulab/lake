# Verification report — issue #4

- base_sha: d7c5a536037450aa77d4fc237f6315b49d928610
- head_sha: 128e75d9bfb016a2440e6ed5720caff78a446457 (`issue-4-managed-objects`)
- score_authority: verifier
- implementer_evidence: self_check_only

This is an independent re-verification of the repaired candidate. I did not
read the workspace's pre-existing `verification/report.md`, because it belongs
to a different issue. The jj workspace was clean before verification. Its
colocated Git `HEAD` remains `main`; the candidate is therefore pinned by the
explicit bookmark above, whose files are the jj working-copy parent.

## Commands

```text
$ jj st
The working copy has no changes.
Working copy  (@) : rkzwvrvp d749246b (empty) (no description set)
Parent commit (@-): vtywxtqv d43bbcbd issue-4-managed-objects | fix(objects): clean failed upload staging files (#4)

$ git rev-parse issue-4-managed-objects
d43bbcbd7f9fc85a0f9e2131f63f5145d55c8d6c

$ git merge-base issue-4-managed-objects origin/main
d27eb5d13eb31af7812e698c45d376ee8ef37f83

$ mise run gate
[hooks] $ prek run --all-files
[test] $ cargo test --workspace --all-targets
[e2e] $ cargo run -p lake-cli -- selftest
[site-install] $ bun install --cwd site --frozen-lockfile
[site-check] $ bun run --cwd site check
[hooks] Finished in 61.9ms
[site-check] Test Files  2 passed (2)
[site-check] Tests  5 passed (5)
[site-check] ✓ built in 1.99s
[e2e] self-check ok
[e2e] Finished in 8.70s
[test] test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.07s
[test] Finished in 12.04s
Finished in 12.04s

$ mise run spec-lifecycle specs/issue-4-managed-objects.spec.md
=== Lifecycle Report (guarded) ===
Spec: managed-objects  stage: complete  passed: true
  [PASS] SQL insert uploads an object before publishing its DataLocation row
  [PASS] failed object upload does not publish a partial row
  [PASS] unsupported INSERT syntax is rejected before any upload
spec-lifecycle-guard: OK — every Test selector executed >=1 test

$ cargo test -p lake-sdk insert_sql_uploads_object_and_queries_datalocation
running 1 test
test tests::insert_sql_uploads_object_and_queries_datalocation ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 2 filtered out; finished in 0.04s

$ cargo test -p lake-sdk failed_upload_does_not_publish_a_table_version
running 1 test
test tests::failed_upload_does_not_publish_a_table_version ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 2 filtered out; finished in 0.01s

$ cargo test -p lake-sdk unsupported_insert_syntax_never_starts_an_upload
running 1 test
test tests::unsupported_insert_syntax_never_starts_an_upload ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 2 filtered out; finished in 0.01s

$ rm -rf data && cargo run -p lake-cli -- selftest
created table robots.episodes
committed robots.episodes at v2
+----------+----------+------------+
| robot_id | episodes | avg_reward |
+----------+----------+------------+
| alpha    | 2        | 0.8        |
| beta     | 1        | 0.4        |
+----------+----------+------------+
self-check ok

$ cargo test -p lake-objects local_reader_rejects_locations_outside_the_managed_prefix
running 1 test
test tests::local_reader_rejects_locations_outside_the_managed_prefix ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 2 filtered out; finished in 0.00s

$ cargo test -p lake-objects put_file_streams_bytes_and_returns_verified_location
running 1 test
test tests::put_file_streams_bytes_and_returns_verified_location ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 2 filtered out; finished in 0.03s

$ cargo run --manifest-path /tmp/lake-issue-4-verifier/Cargo.toml
CJK upload/direct read: PASS
managed-root containment: PASS
interrupted reader leaves no published or .uploading file: PASS

$ cargo run --manifest-path /tmp/lake-issue-4-pre-repair-verifier/Cargo.toml
thread 'main' (...) panicked at src/main.rs:27:5:
interrupted upload left a .uploading file behind
```

The two `/tmp` harnesses are throwaway verifier programs using only the public
candidate API; no workspace source was changed by them. The pre-repair harness
is identical in the relevant probe, but points at detached `0132714`.

## Transition matrix

- fail_to_pass: observed. A reader that writes `prefix-before-interruption`
  and then returns an I/O error leaves a `.uploading` file at `0132714` (the
  pre-repair harness panics as shown). The same public-API probe passes at
  `d43bbcbd`: the operation errors, no final destination is published, and no
  `.uploading` entry remains. The SDK acceptance test additionally observes
  that the table version remains unchanged.
- pass_to_fail: 0. The complete quality gate, all three bound spec selectors,
  and a fresh-state CLI self-check are green.

## Probes

- CJK path: uploaded a file named `视频-例.mp4`, opened the returned
  `DataLocation` directly, and compared its bytes. Expected direct read and
  a single non-staging published entry; observed PASS.
- Managed-root containment: attempted direct and in-root-symlink `file://`
  references to a file outside the managed root. Expected
  `OutsideManagedPrefix`; observed PASS in the throwaway probe and the package
  regression test. This exercises canonical-path containment rather than only
  string-prefix matching.
- Real reader interruption: an `AsyncRead` implementation first supplies one
  block and then returns `intentional reader interruption`. Expected an error,
  no table-version advance, and an objects directory with neither final object
  nor `.uploading` residue. Observed PASS: the SDK test establishes unchanged
  version and empty objects directory; the independent public-API probe
  establishes no published/staging entry.

The durable publish path was also inspected and exercised: it writes to a
unique `.uploading` file, `sync_all`s that file, renames it to the final UUID,
then `sync_all`s the managed directory before returning `DataLocation`. The
successful streaming/direct-reader probe observed only the final object; the
interruption probe observed cleanup before publication.

## Verdict

PASS — the repaired candidate is clean, gated, spec-complete, cold-booted,
and independently demonstrates durable staged publication, canonical managed-
root containment, and cleanup after a real mid-stream reader interruption.

## FILE contract refinement

The post-verification API refinement keeps the stored `DataLocation` and
direct-reader behavior intact while making the public write vocabulary match
the SQL logical type: `InsertValue::File(FileUpload::from_path(...))`.

```text
$ cargo test -p lake-sdk insert_sql_file_uploads_and_queries_datalocation
error[E0432]: unresolved import `crate::FileUpload`
error[E0599]: no variant, associated function, or constant named `File`

$ cargo test -p lake-sdk insert_sql_file_uploads_and_queries_datalocation
running 1 test
test tests::insert_sql_file_uploads_and_queries_datalocation ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 2 filtered out

$ cargo test -p lake-sdk -p lake-objects
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out

$ mise run spec-lint specs/issue-4-managed-objects.spec.md
Quality: 100% (determinism: 100%, testability: 100%, coverage: 100%)

$ mise run spec-lifecycle specs/issue-4-managed-objects.spec.md
[PASS] SQL FILE insert uploads an object before publishing its DataLocation row
[PASS] failed object upload does not publish a partial row
[PASS] unsupported INSERT syntax is rejected before any upload

$ mise run gate
Finished successfully: hooks, workspace tests, CLI self-check, and site check
```

The first command is the required red proof: the intended `FILE` SDK API did
not exist. The second is the green proof. The current design names only the
logical value and upload binding; it does not store a signed URL or turn SQL
into an arbitrary object-store URI interface.

## README and runnable example

`README.md` now links the public `FILE` flow to
`crates/lake-sdk/examples/managed_file.rs`. The example creates an isolated
local table, streams a video through `InsertValue::File`, queries its
`DataLocation`, and verifies the SDK direct reader returns the original bytes.

```text
$ cargo run -p lake-sdk --example managed_file
FILE upload and direct read succeeded: file:///.../objects/<immutable-id>
```

## Query-mediated FILE write refactor

The production `lake-sdk` dependency graph now contains `lake-common`,
`lake-objects`, Arrow Flight/Tonic, and serialization/runtime crates only. It
has no normal dependency on `lake-metasrv`, `lake-engine`, or `lake-meta`.
`LakeClient::connect` receives a query endpoint plus managed-stage adapter;
metadata and engine crates remain dev-only fixtures.

The verified write route is:

```text
SDK -> managed stage: raw file stream
SDK -> query DoPut: DataLocation Arrow row
query -> metasrv DoPut: unchanged metadata stream
metasrv leader -> engine append -> registry CAS
```

TDD evidence included four red-to-green boundaries:

- `FileAppendRequest` command round-trip did not compile before the wire value
  existed, then passed in `lake-common`.
- metasrv rejected `DoPut` before the append decoder/leader forwarding path,
  then both the direct decoder test and live two-node forwarding test passed.
- query lacked `serve_with_metadata` before the stateless proxy, then the live
  query-to-metasrv integration test passed.
- `LakeClient::connect` did not exist before removal of in-process metasrv
  ownership, then the query-only SDK acceptance test passed.

Review also found an endpoint composition defect: query prepended `http://` to
an already-complete CLI metadata URI. The integration test was changed to use
`http://127.0.0.1:<port>` and failed with a broken transport; query now accepts
the complete tonic endpoint URI and the same test passes.

Final evidence:

```text
$ cargo run -p lake-sdk --example managed_file
FILE upload and direct read succeeded: file:///.../objects/<immutable-id>

$ cargo clippy -p lake-common -p lake-metasrv -p lake-query -p lake-sdk -p lake-cli --all-targets -- -D warnings
Finished successfully

$ mise run spec-lint specs/issue-4-managed-objects.spec.md
Quality: 100% (determinism: 100%, testability: 100%, coverage: 100%)

$ mise run spec-lifecycle specs/issue-4-managed-objects.spec.md
[PASS] SQL FILE insert uploads an object before publishing its DataLocation row
[PASS] failed object upload does not publish a partial row
[PASS] unsupported INSERT syntax is rejected before any upload
spec-lifecycle-guard: OK

$ mise run gate
Finished successfully: workspace all-target tests, CLI selftest, and site checks
```
