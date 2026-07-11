# Issue 29 verification — idempotent FILE append recovery

## Candidate

- Fixed commit: `a4a76c2646eff8080a1a720b017af0a833907ed7`.
- The independent verification checkout was clean and its parent was the fixed
  commit above.

## Acceptance evidence

- `mise run gate`: passed in 83.53 seconds on the fixed candidate (hooks,
  workspace Rust tests, end-to-end selftest, and site checks).
- `mise run spec-lifecycle specs/issue-29-idempotent-append.spec.md`: boundary
  validation and all 15/15 guarded scenarios passed; every selector executed.
- Rustdoc with warnings denied and `git diff --check` passed.
- The preceding candidate passed all 13/13 LocalStack integration tests. The
  fixed candidate's incremental changes are limited to SDK retry identity,
  operation migration/recovery, documentation, and acceptance selectors; they
  do not change an S3 or Dynamo backend.

## Independent focused probes

- `cargo test -p lake-sdk sdk_ -- --nocapture`: 9 passed, 2 explicitly ignored
  LocalStack tests, 0 failed. This includes the real SDK lost-result retry,
  resumable retry-horizon, and ungraceful two-node leader-failover tests.
- `cargo test -p lake-metasrv replay_after_drop_recreate_fails_closed --
  --nocapture`: 1 passed, 0 failed.
- `cargo test -p lake-engine-lance
  new_append_does_not_scan_transaction_history -- --nocapture`: 1 passed,
  0 failed.
- `cargo test -p lake-query --test file_append_proxy
  query_forwards_authenticated_append_operation_scope -- --nocapture`: 1
  passed, 0 failed.

## Properties inspected

- `LakeClient::insert` uploads and encodes once, then retries `do_put` with the
  same UUIDv7 operation identity and payload. Retry expiry returns a
  `PendingAppend` containing that identity and encoded messages;
  `resume_append` neither allocates a new identity nor uploads the object
  again.
- The lost-result SDK test exercises an actual object upload, Flight append,
  replay, and SQL read. It observes one object, one row, and version 2.
- The failover test blocks the first leader's result after commit, terminates
  it without lease resignation, waits for the production ten-second lease to
  expire, and lets the same `LakeClient::insert` converge through the standby.
  It asserts one uploaded object, one SQL row, and version 2.
- Append records bind an immutable table incarnation. Replaying an operation
  after drop/recreate fails with `FailedPrecondition`, while the replacement
  table remains at version 1.
- A newly reserved Lance append calls `append_reserved` and the focused test
  observes zero transaction-history scans on that normal path.
- Query authenticates two user tenants, delegates to an independently
  authenticated metadata connection, and scopes the same operation ID by
  trusted tenant: tenant A commits version 2, tenant B version 3, and tenant A
  replays version 2.

## Verdict

PASS. The fixed candidate satisfies the issue 29 acceptance contract and the
reviewed crash, retry, tenancy, incarnation, and normal-path performance
invariants.
