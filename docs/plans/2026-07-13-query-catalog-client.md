# Query Catalog Client Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make production Query consume the catalog through authenticated,
bounded Metasrv reads while preserving the existing local cache and SQL
semantics.

**Architecture:** `lake-catalog` gains a narrow read-only `CatalogSource`
instead of depending internally on raw registry KV operations. A local adapter
keeps development/tests simple; a Query-side Flight client implements the same
contract. Metasrv adds one versioned conditional snapshot action that performs
generation fencing at the authority and keeps refresh traffic to one bounded
RPC.

**Tech Stack:** Rust, async-trait, Arrow Flight `DoAction`, tonic,
serde/JSON, moka, DataFusion, jj, agent-spec.

---

### Task 1: Freeze the source contract with local parity tests

**Files:**
- Create: `crates/lake-catalog/src/source.rs`
- Modify: `crates/lake-catalog/src/lib.rs`
- Modify: `crates/lake-catalog/src/catalog.rs`
- Test: `crates/lake-catalog/src/catalog.rs`

1. Add failing tests that construct `LakeCatalog` from a fake source and prove
   generation `not_modified`, full snapshot publication, point resolve, and
   source failure behavior without exposing `MetaStore` methods.
2. Run `cargo test -p lake-catalog catalog_source -- --nocapture`; expect
   compile failure because `CatalogSource` does not exist.
3. Define `CatalogSource`, `CatalogSourceRef`, `CatalogSourceError`, bounded
   `CatalogDirectoryRequest/Response`, and `CatalogTableRegistration`. Provide
   a `LocalCatalogSource` adapter over the existing registry functions.
4. Replace `CatalogState.meta` with `CatalogSourceRef`; make refresh consume a
   conditional coherent response and registration fills call `resolve`.
5. Run the focused tests, the existing catalog suite, strict clippy, and
   `git diff --check`; expect all pass.

### Task 2: Add the bounded Metasrv snapshot action

**Files:**
- Modify: `crates/lake-metasrv/src/control.rs`
- Modify: `crates/lake-metasrv/src/lib.rs`
- Test: `crates/lake-metasrv/src/control.rs`

1. Add failing `remote_catalog_snapshot_is_generation_coherent_and_bounded`
   coverage for conditional not-modified, a mutation between scan/generation
   reads, response entry/schema/byte limits, role authorization, and redacted
   errors.
2. Run the exact selector; expect failure because `catalog_snapshot` is not an
   action.
3. Implement an authority helper that observes the directory marker, scans,
   rechecks the generation up to three times, sorts entries canonically, and
   accounts each entry before retaining it and holds a process-local admission
   permit through response disposal. Legacy non-authoritative state fails
   closed; only the local development adapter preserves full-scan compatibility.
4. Dispatch `catalog_snapshot` only for QueryService, MetadataPeer, and Admin;
   add it to `list_actions` and keep user principals denied.
5. Run the selector, all control tests, strict clippy, and `git diff --check`.

### Task 3: Implement the authenticated remote source

**Files:**
- Create: `crates/lake-query/src/catalog_client.rs`
- Modify: `crates/lake-query/src/lib.rs`
- Modify: `crates/lake-query/Cargo.toml`
- Test: `crates/lake-query/src/catalog_client.rs`

1. Add failing real-listener parity and RPC-count tests named
   `remote_catalog_source_matches_local_catalog_resolution` and
   `remote_catalog_cache_hit_uses_zero_metadata_rpcs`.
2. Run both selectors; expect compile failure because the remote source does
   not exist.
3. Implement a cloneable client using `ClientSecurity`, strict request/result
   byte ceilings, exactly-one-result decoding, stable redacted status mapping,
   conditional `catalog_snapshot`, and namespace-delegated `resolve`.
4. Add `QueryEngine` constructors that accept `CatalogSourceRef`; keep an
   explicitly named local constructor for tests/in-process commands.
5. Run parity/counting tests, the Query suite, check, and strict clippy.

### Task 4: Preserve outage and append invalidation semantics

**Files:**
- Modify: `crates/lake-query/src/catalog_client.rs`
- Modify: `crates/lake-query/src/flight.rs`
- Modify: `crates/lake-catalog/src/catalog.rs`
- Test: `crates/lake-query/src/catalog_client.rs`
- Test: `crates/lake-query/src/flight.rs`

1. Add failing selectors
   `remote_catalog_outage_serves_last_good_generation` and
   `remote_catalog_append_invalidation_observes_committed_version`.
2. Prove a stale warm cache returns immediately during one failed refresh, and
   an append acknowledgement increments the local registration epoch before a
   delegated resolve of the new version.
3. Fix only source error mapping, coalescing, and invalidation wiring required
   by those tests; do not add polling or push invalidation.
4. Run the selectors plus existing refresh health, shutdown, snapshot pinning,
   and FILE append tests.

### Task 5: Remove direct registry wiring from served Query

**Files:**
- Modify: `crates/lake-cli/src/commands/serve.rs`
- Modify: `crates/lake-cli/src/commands/selftest.rs`
- Modify: `crates/lake-cli/src/commands/sql.rs`
- Modify: `crates/lake-cli/src/commands/limits.rs`
- Test: `crates/lake-cli/src/commands/serve.rs`

1. Add failing `query_catalog_wiring_requires_remote_metadata_source` coverage
   proving server-mode Query requires a valid metadata endpoint and client
   security before bind and has no direct-registry fallback.
2. Build the remote source before `QueryEngine`; retain the local adapter only
   for in-process selftest/SQL commands whose metadata authority is in the same
   process.
3. Keep the storage engine's physical manifest store unchanged and label that
   distinction in code/docs; do not claim credential separation.
4. Run CLI configuration tests, real secured Query/Metasrv tests, check, and
   strict clippy.

### Task 6: Document and verify the authority boundary

**Files:**
- Modify: `README.md`
- Modify: `docs/architecture.md`
- Modify: `specs/issue-122-query-catalog-client.spec.md`
- Create: `verification/issue-122-query-catalog-client.md`

1. Document the conditional snapshot protocol, cache/RPC complexity, failure
   behavior, auth roles, bounds, and the physical manifest-KV non-goal.
2. Run `cargo +nightly fmt --all -- --check`, `git diff --check`, affected
   strict clippy/check suites, and every focused selector.
3. Run `mise run spec-lifecycle specs/issue-122-query-catalog-client.spec.md`;
   require 6/6 selectors executed.
4. Run `mise run gate`, record exact counts/timing, request independent review,
   fix every P0/P1, then commit `refactor(query): use metadata catalog client
   (#122)`, push, open the PR, and merge only after APPROVE.
