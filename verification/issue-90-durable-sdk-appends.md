# Verification: durable SDK append recovery

## Fixed scope

- Issue: #90
- Base: `2e64a5a3cbf061ae01427bf38adf09ace47b2546`
- Contract: `specs/issue-90-durable-sdk-appends.spec.md`
- Allowed production code: `crates/lake-sdk/**` plus the workspace `libc`
  dependency declaration used for no-follow file opens.

## RED

The restart scenarios were added before the recovery API and durable format.
`cargo test -p lake-sdk durable_append_checkpoint_survives_client_restart`
could not compile because `LakeClient::pending_append_ids` and
`LakeClient::load_pending_append` did not exist. This established that current
memory-only `PendingAppend` state could not satisfy restart recovery.

During GREEN, the terminal-cleanup scenario initially failed because the test
injected damaged Arrow metadata, which the service correctly classified as an
ambiguous transport failure and retained. The test was corrected to send an
explicitly invalid FILE command (`InvalidArgument`), after which it proved the
intended conclusive-rejection cleanup boundary.

## GREEN evidence

- `mise run spec-lint specs/issue-90-durable-sdk-appends.spec.md`: PASS, quality 100%.
- `mise run spec-lifecycle specs/issue-90-durable-sdk-appends.spec.md`: PASS, 9/9 selectors executed.
- `cargo test -p lake-sdk durable_append_checkpoint`: PASS, 4/4.
- `cargo test -p lake-sdk append_without_checkpoint_directory_remains_memory_only`: PASS, 1/1.
- `cargo test -p lake-sdk --all-targets`: PASS, 39/39; two LocalStack tests ignored by design.
- `cargo clippy -p lake-sdk --all-targets -- -D warnings`: PASS.
- `mise run gate`: PASS after the response-ambiguity, no-follow bounded-read,
  and post-publish ownership fixes; workspace tests, e2e, hooks, and site
  checks all passed.
- `mise run doc`: PASS with rustdoc warnings denied.

## Safety properties inspected

- A versioned protobuf checkpoint is file-synced, atomically renamed, and its
  parent directory is synced before the first append RPC.
- Filename and content use a canonical UUIDv7 operation ID; directory joining
  accepts no caller-controlled path components.
- Directory inspection is capped at 1,024 entries, including unrelated files.
- Recovery opens the exact file without following symbolic links, checks
  metadata on that handle, and limits the actual read to the byte ceiling plus
  one even if the file grows concurrently. Flight messages use bounded length
  framing with a 4,096-message cap.
- Load validates stage identity, checkpoint integrity, descriptor operation,
  and the existing server payload digest before any network append.
- Flight protocol, decode, Arrow, missing-result, and ambiguous transport
  failures retain the file because the server may already have committed.
  Conclusive success/rejection attempts
  synced removal; cleanup failure is logged but cannot turn a committed result
  into an application-visible failure that invites a new logical insert.
- If final rename succeeds but parent-directory sync fails, the typed error
  returns the exact `PendingAppend` and published path instead of losing
  operation ownership behind an ordinary preparation error.
- A replay after commit returns the original version through existing
  tenant/table/operation/digest idempotency and uploads no object bytes.
- README, design docs, example, and public recovery API document the finite
  `LAKE_APPEND_OPERATION_RETENTION_SECS` horizon (seven days by default) and
  terminal cleanup after an explicit expired-operation rejection.

## Remaining gates

Independent reviewer and fixed-head verifier are recorded after the candidate
commit is frozen.
