# Verification: durable asynchronous query results

## Required evidence

- Standard `PollFlightInfo` submissions authorize SQL and persist an encrypted
  exact-snapshot job outside the catalog authority.
- Any Query replica can poll compact CAS state without resolving current table
  pointers; poll and result capabilities are tenant/principal bound.
- Workers use renewable epoch-fenced leases and can publish completion only
  after every bounded Arrow IPC part and the immutable manifest exist.
- `CancelFlightInfo`, expiry fencing, delayed scoped cleanup, and worker
  heartbeat loss cannot publish partial or stale results.
- The Rust SDK polls and fans in exact `DoGet` endpoints through its Query-only
  connection.

## RED/GREEN evidence

- The first state-machine test failed before bounded queued/running/terminal
  records and CAS transitions existed. It now proves takeover increments the
  epoch and a stale worker cannot complete.
- The first Flight test failed because `TracedFlightSqlService` had no async
  coordinator. It now submits a snapshot-pinned job, polls it on a replica
  with a failing catalog store, runs the worker, and redeems the exact result.
- The result test failed before scoped object writes, Arrow part encoding, and
  manifest publication existed. It now decodes the stored IPC part and proves
  the state exposes only the final manifest.
- The SDK integration initially hung because `MetaStore::list_prefix` returns
  stripped keys. The worker scanner now uses bounded resumable prefix pages;
  the real network roundtrip completes and returns the expected Arrow value.
- The runnable `async_query` example initially exposed unordered SQL output;
  its statement now orders explicitly and the example completes with `[1, 2]`.

## Review corrections

- Durable job envelopes have a separate audience and a 24-hour maximum while
  interactive statement and poll/result capabilities retain the one-hour
  security ceiling.
- Result job, manifest, and part reads verify the exact `DataLocation` size and
  SHA-256 before use. State and manifest row/part/byte summaries must agree.
- Parts are capped at 65,536 rows and 64 MiB; manifests, state records, SQL,
  snapshots, leases, scan pages, result count, and total bytes are finite.
- Result `DoGet` shares Query admission/deadline limits instead of allowing
  unbounded concurrent 64 MiB redemption.
- Worker heartbeat runs during planning and object upload. Loss of the fenced
  lease cancels execution. Expiry first CASes `cleaning`, waits the maximum
  prior lease, deletes the whole tenant/query scope, then conditionally deletes
  state, avoiding a late-upload cleanup race.
- Async configuration is validated before catalog warmup or readiness. CLI and
  Kubernetes cloud mode use a dedicated DynamoDB table pair and S3 prefix,
  never the registry authority.
- Completed endpoints remain standard Flight `DoGet` tickets. Raw presigned
  object URLs are not mislabeled as Flight service locations and never enter
  the protocol.

## Current GREEN evidence

- `mise run spec-lifecycle specs/issue-110-async-query-results.spec.md`: all
  eight scenarios passed with non-zero selector matches.
- `cargo clippy -p lake-query -p lake-sdk -p lake-objects -p lake-cli
  --all-targets -- -D warnings`: passed.
- `cargo run -p lake-sdk --example async_query`: passed and printed the ordered
  durable result `[1, 2]`.
- Kubernetes reference contract test and the LocalStack S3 scoped-result
  wiring test passed; external LocalStack probes remain CI-owned ignored tests.

## Final gate

- `mise run gate` passed on the reviewed tree in 113.61 seconds with exit code
  0: workspace all-target tests, e2e self-test, hooks, and site
  install/typecheck/tests/build completed with zero failures.
