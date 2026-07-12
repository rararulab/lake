# Verification: pinned statement snapshots

## Required evidence

- `GetFlightInfo` and `DoGet` use the same exact table generations after a
  concurrent commit.
- A dropped/recreated SQL name cannot redirect an issued ticket to the new
  location or incarnation.
- Every physical reference in joins, subqueries, and CTEs is present exactly
  once; claims and ciphertext allocation are bounded.
- DoGet reconstructs providers on another stateless replica without resolving
  mutable current pointers or falling forward to latest.
- Old SQL-only ticket protocol versions fail closed.

## RED/GREEN evidence

- The first codec test failed to compile because `StatementTicket`,
  `StatementTableSnapshot`, bounded snapshot claims, and the statement-aware
  codec methods did not exist. It now round-trips encrypted exact claims and
  rejects a 65th table.
- The catalog regression failed to compile because `TableSnapshot` and
  `provider_for_snapshot` did not exist. It now proves a reclaimed version
  requests only the claimed version even when the handle reports a newer
  latest value, without a metastore read.
- The SQL-reference regression failed to compile because authorization did
  not return physical references. It now proves join, nested subquery, and CTE
  aliases resolve to one canonical three-table set.
- The append drift regression issues a real standard Flight SQL ticket on one
  replica at version 1, advances the registry to version 2, and executes on a
  fresh replica; only version-1 rows are returned.
- The recreate regression issues against an old location, replaces the SQL
  name, reclaims the old provider, and executes on a fresh replica. Execution
  fails and the probe records only the old claimed location, never the
  replacement.
- A cryptographically valid version-1 SQL-only envelope is rejected by the
  version-2 codec with the uniform invalid-ticket class.

## Review corrections

- Snapshot resolution and exact-provider opening are inside the existing
  planning deadline rather than an unbounded pre-timeout phase.
- Direct SQL-only codec helpers are test-only; production Flight issuance can
  only call the snapshot-aware seal/open methods.
- The previous DoGet early-return test treated an immediate authorization
  error as success. It now uses a real pinned streaming provider and requires
  a valid stream response before declaring early return.
- Ticket count and every claim field are bounded before sealing and after
  decryption; pre-decrypt ciphertext allocation is capped at SQL bytes plus
  the computed finite 320 KiB snapshot envelope allowance.
- Snapshot resolution/loading statuses use fixed client text, so an unavailable
  encrypted claim cannot disclose its object-store location through gRPC.
- Request-local DataFusion contexts share the process runtime and spill pool;
  no plan, ticket, or provider identity is stored as replica-local durable
  state.

## Current GREEN evidence

- `cargo test -p lake-query --lib`: 47 passed, zero failed.
- `cargo test -p lake-catalog --lib`: 22 passed, zero failed.
- `mise run spec-lifecycle specs/issue-108-pinned-statement-snapshots.spec.md`:
  all six scenarios passed with non-zero test selector matches.
- `cargo clippy -p lake-query -p lake-catalog --all-targets -- -D warnings`:
  passed.
- `cargo clippy --workspace --all-targets -- -D warnings`: passed.
- `cargo doc --workspace --no-deps`: passed.

## Final gate

- `mise run gate` passed on the reviewed production tree in 215.45 seconds
  with exit code 0: workspace all-target tests, e2e self-test, hooks, and site
  install/typecheck/tests/build completed with zero failures.
