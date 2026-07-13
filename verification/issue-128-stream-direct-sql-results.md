# Verification report — issue #128

- lane: 1
- base: `main` at `990146066bbcca06640489cd995569887ec7636c`
- scope: direct in-process SQL result streaming

## Fail-to-pass contract

The initial test used `ShutdownPartition`, which yields one batch and blocks
before the second. It timed out around `QueryEngine::execute_sql`, so it
compiled against the old collected return type. Before the implementation:

```console
$ cargo test -p lake-query --lib direct_sql_results_stream_before_source_completion
test tests::direct_sql_results_stream_before_source_completion ... FAILED
opening a direct SQL result must not wait for its source to finish
```

The old `DataFrame::collect` waited for the blocked second batch. The final
test pulls the first batch (`1`), proves the second poll remains blocked, then
releases and observes the second batch (`2`):

```console
$ cargo test -p lake-query --lib direct_sql_results_stream_before_source_completion
test tests::direct_sql_results_stream_before_source_completion ... ok
```

`QueryEngine::execute_sql` now returns DataFusion's
`SendableRecordBatchStream` from `DataFrame::execute_stream`. The query CLI
and selftest use `try_next` and retain only their current batch; no direct
production call site collects the result stream.

## Boundary and consumer checks

```console
$ cargo test -p lake-query --lib public_sql_surface_is_read_only
test tests::public_sql_surface_is_read_only ... ok

$ cargo test -p lake-query --lib
test result: ok. 90 passed; 0 failed

$ cargo test -p lake-cli --all-targets
test result: ok. 36 passed; 0 failed
test result: ok. 1 passed; 0 failed
test result: ok. 4 passed; 0 failed

$ cargo test -p lake-sdk --all-targets
test result: ok. 44 passed; 0 failed; 2 ignored

$ mise run e2e
created table robots.episodes
committed robots.episodes at v2
self-check ok

$ cargo run -p lake-cli -- sql "SELECT robot_id, episode FROM lake.robots.episodes ORDER BY robot_id, episode"
| alpha    | 1       |
| alpha    | 2       |
| beta     | 1       |
```

The existing SDK FILE insert regression still opens its returned
`DataLocation` directly and passed after its test-only in-process consumer was
migrated to pull one batch.

## Specification and final gate

```console
$ mise run spec-lint specs/issue-128-stream-direct-sql-results.spec.md
Quality: 100% (determinism: 100%, testability: 100%, coverage: 100%)

$ mise run spec-lifecycle specs/issue-128-stream-direct-sql-results.spec.md
[PASS] Direct SQL exposes a live record-batch stream
[PASS] Direct SQL retains the public read-only boundary

$ mise run gate
[hooks] Finished
[test-adbc] test result: ok. 3 passed; 0 failed
[e2e] self-check ok
[site-check] Test Files  2 passed
[test] Finished
Finished
```

The complete gate transcript is retained during verification in
`/tmp/lake-gate-128.log`; it contains no failed test or task and ends after all
gate branches complete. The platform linker emits its existing compact-unwind
warning during macOS test links; it is non-fatal and appears on unchanged
packages as well.
