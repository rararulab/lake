# Metadata Hardening and SQL-over-S3 Verification

Date: 2026-07-10

Workspace: `.worktrees/issue-0-meta-hardening`

## Requirement evidence

| Area | RED evidence | GREEN evidence |
|---|---|---|
| Snapshot authority | `table_provider_reads_requested_snapshot` read 3 rows from requested v1 because the provider ignored the version. | Lance checks out the exact registry version; the focused test and `lake-engine-lance` suite pass. |
| Maintenance publication | `sweep_advances_registry_to_maintenance_version` left the registry at v4 while compaction advanced the engine to v6. | `maintain` returns its commit version and maintenance publishes it with registry CAS; metasrv suite passes. |
| Conditional drop | A stale registry delete removed a replacement; a create racing an in-flight drop returned early. | Compare-and-delete plus per-table async serialization preserve the replacement; registry and metasrv concurrency tests pass. |
| Lease safety | The write gate remained true beyond a 20ms lease; a hanging metastore campaign was not cancelled within a 50ms lease. | Leadership uses a monotonic local deadline and campaign I/O is capped at 40% of TTL; expiry, cancellation, and two-node forwarding tests pass. |
| Catalog shield | Two consecutive queries caused two full scans; a failed registry `get` became `Ok(None)`. | Refreshes coalesce behind a 5s bounded-staleness window, registrations have TTL, the server refreshes in the background, and backend errors propagate. |
| Query streaming | `DoGet` did not return until a delayed second input batch completed. | `execute_stream` feeds `FlightDataEncoderBuilder` directly; the delayed-producer test passes. |
| Append streaming | The append test failed with `consumer retained every prior batch` because all batches were collected into a `Vec`. | Lance `InsertBuilder::execute_stream` consumes batches incrementally; the release-before-end test passes. |
| S3 lifecycle | LocalStack recreation failed with `DatasetAlreadyExists` after all dataset objects had been deleted. | Drop conditionally clears external-manifest history; LocalStack drop/recreate starts at v1 and passes. |
| Read-only SQL | Public `INSERT` returned a successful one-row result. | All public planning uses `SQLOptions` with DDL, DML, and statements disabled; `SELECT`/`EXPLAIN` pass and arbitrary external locations are rejected. |

## Final commands

### Full local gate

```text
mise run gate
exit 0
```

The gate ran `cargo test --workspace --all-targets` and the local e2e. Results:

- `lake-engine-lance`: 9 passed
- `lake-meta`: 4 passed
- `lake-metasrv`: 7 unit tests passed
- two-node forwarding: 1 passed
- `lake-query`: 3 passed
- local e2e: `self-check ok`
- LocalStack-only tests were correctly ignored by the ordinary workspace run

### Formatting and lint

```text
mise run fmt-check
exit 0

mise run clippy
exit 0
```

The clippy command checked the full workspace, all targets, all features, with
`-D warnings`.

### DynamoDB + S3 integration

```text
mise run test-integration
exit 0
```

LocalStack results: 2 tests run, 2 passed:

- `lake-meta::dynamo_localstack::dynamo_meta_roundtrip`
- `lake-engine-lance::s3_lance_localstack::lance_engine_on_s3_with_dynamo_external_manifest`

The S3 test includes append/read, manifest-pointer verification, drop, complete
object and pointer cleanup, and recreate-at-v1.

### Fresh-directory e2e

```text
cargo run -p lake-cli -- --data-dir /tmp/lake-e2e-final.wzFvVE selftest
exit 0
```

Observed a new table, commit at v2, and the expected aggregate rows:
`alpha = 2`, `beta = 1`; output ended with `self-check ok`. The temporary
directory was removed afterward.

## Architecture review

The final implementation matches `goal.md`:

- query fan-out reads exact registered snapshots directly from storage;
- registry traffic is bounded by per-node cache refresh/miss traffic rather
  than query count;
- metadata mutations and maintenance are leader/deadline gated and serialized
  per table;
- versions become visible only through registry CAS;
- Flight SQL and append paths preserve batch streaming;
- Lance remains confined to `lake-engine-lance`;
- public SQL is read-only and cannot introduce an arbitrary S3 source.

The interactive SQL-over-S3 path is implemented. Async large-result
materialization (`PollFlightInfo` plus presigned HTTPS result endpoints) is a
documented next phase, not claimed as current functionality.

## Delivery state

All changes are local conventional commits in the isolated jj workspace. No
remote issue, push, or pull request was created, per the local-only instruction.
