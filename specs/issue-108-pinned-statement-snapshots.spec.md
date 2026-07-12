spec: task
name: "pinned-statement-snapshots"
inherits: project
tags: [query, flight-sql, snapshots, consistency, catalog, tickets]
---

## Intent

Make a Flight SQL statement ticket describe one immutable query input. A
catalog refresh, append, drop, or recreate between `GetFlightInfo` and `DoGet`
must not change the schema or rows represented by the issued capability.

## Decisions

- Resolve every physical lake table referenced by the SQL before planning,
  including references inside joins, subqueries, and CTEs.
- Represent each input by namespace, table, engine, location, incarnation, and
  exact version. Refuse issuance for legacy registrations without an
  incarnation or for references that cannot be pinned.
- Plan both RPC phases through an isolated in-memory catalog populated only
  with providers opened at those exact snapshots. Never re-resolve a current
  registry pointer during `DoGet` and never fall back to latest.
- Encrypt the bounded snapshot list inside the existing tenant/principal-bound
  ticket. Bump the inner protocol version so an older replica cannot silently
  ignore snapshot claims; incompatible rolling upgrades fail closed.
- Keep Query stateless: another replica reconstructs providers from the
  self-contained ticket using shared object storage and the shared key ring.

## Boundaries

### Allowed Changes
Cargo.toml
Cargo.lock
crates/lake-catalog/**
crates/lake-query/**
README.md
docs/architecture.md
docs/design/sql-api-over-s3.md
docs/plans/2026-07-12-pinned-statement-snapshots.md
specs/issue-108-pinned-statement-snapshots.spec.md
verification/issue-108-pinned-statement-snapshots.md

### Forbidden
server-local ticket or plan state
re-resolving current table versions during DoGet
falling back to latest when an exact snapshot is unavailable
unbounded table count, claim fields, or ticket allocation
accepting table-bearing legacy tickets without snapshot claims
metadata traffic proportional to streamed rows
serializing credentials or raw SQL outside the encrypted payload
changing the standard outer Flight SQL ticket type

## Completion Criteria

Scenario: Ticket carries a bounded exact snapshot set
  Test:
    Package: lake-query
    Filter: statement_ticket_roundtrips_bounded_table_snapshots
  Given exact table identities and versions resolved during planning
  When Query seals and opens a statement ticket
  Then the encrypted payload round-trips every claim and rejects malformed or unbounded sets

Scenario: A concurrent append cannot change an issued statement
  Test:
    Package: lake-query
    Filter: statement_ticket_executes_original_snapshot_after_commit
  Given GetFlightInfo planned table version one and version two commits before DoGet
  When the client executes the issued ticket
  Then its schema and rows still come only from version one

Scenario: Drop and recreate cannot redirect an issued statement
  Test:
    Package: lake-query
    Filter: statement_ticket_never_redirects_to_recreated_table
  Given a table is dropped and recreated under the same SQL name after ticket issuance
  When DoGet executes or the old snapshot has been reclaimed
  Then it reads the old incarnation or fails explicitly without touching the replacement

Scenario: Every physical reference is pinned
  Test:
    Package: lake-query
    Filter: statement_ticket_pins_joins_subqueries_and_ctes
  Given SQL with a join, nested subquery, and CTE
  When GetFlightInfo issues a ticket
  Then each distinct physical lake table appears exactly once and no reference is silently omitted

Scenario: A missing historical snapshot never falls forward
  Test:
    Package: lake-catalog
    Filter: missing_pinned_snapshot_never_falls_back_to_latest
  Given an exact claimed location and version no longer exists
  When a replica reconstructs the pinned provider
  Then loading fails and never opens the current registration or latest version

Scenario: Mixed protocol versions fail closed
  Test:
    Package: lake-query
    Filter: legacy_statement_ticket_fails_closed_after_snapshot_upgrade
  Given an older SQL-only statement ticket and a snapshot-aware replica
  When DoGet opens the ticket
  Then it returns the uniform invalid-ticket class instead of replanning against current state

## Out of Scope

- Cross-table atomic commit or a globally consistent multi-table timestamp.
- Extending storage retention; an expired/reclaimed engine snapshot is an
  explicit execution failure.
- Persisting or serializing DataFusion logical/physical plans.
- Async result materialization and PollFlightInfo capabilities.
