# Query Manifest Authority Isolation Plan

**Goal:** Make served Query start and operate without catalog DynamoDB access,
while keeping Lance physical manifest metadata directly readable from a
separate least-privilege table.

**Architecture:** Split the current all-in-one CLI `Context` into a full
metadata/admin context and a minimal `QueryContext`. A pure validated cloud
storage plan names independent registry and manifest tables before I/O. The
metadata context opens both; Query opens only the manifest store and never
provisions it. Local Query constructs local Lance plus its managed stage without
opening Rocks catalog state.

**Tech Stack:** Rust, Tokio, DynamoDB-backed `MetaStore`, Lance external
manifests, clap CLI, Kubernetes YAML, agent-spec, jj.

---

### Task 1: Freeze the cloud authority plan

**Files:**
- Modify: `crates/lake-cli/src/commands/mod.rs`
- Test: `crates/lake-cli/src/commands/mod.rs`

1. Add failing selectors `cloud_manifest_table_alias_fails_before_connect` and
   `cloud_storage_wiring_separates_registry_and_manifest_authority` against a
   pure cloud table/storage plan.
2. Run both selectors and require real failures because no manifest-specific
   table or Query/metadata authority plan exists.
3. Implement bounded non-empty table-name validation, default names, alias
   rejection, and explicit metadata-vs-Query connection/provisioning plans.
4. Run the focused tests, fmt, `git diff --check`, and strict CLI clippy.

### Task 2: Split served Query startup context

**Files:**
- Modify: `crates/lake-cli/src/main.rs`
- Modify: `crates/lake-cli/src/commands/mod.rs`
- Modify: `crates/lake-cli/src/commands/serve.rs`
- Modify: `crates/lake-engine-lance/src/lib.rs`
- Modify: `crates/lake-engine-lance/src/manifest_store.rs`
- Test: `crates/lake-cli/src/commands/mod.rs`
- Test: `crates/lake-cli/src/main.rs`

1. Add failing `query_context_has_no_catalog_authority`: local Query startup
   must not create the Rocks `meta` directory or expose registry/Metasrv state.
2. Introduce `QueryContext` with only engine and managed stage. Dispatch Query
   before the full `Context::open` path and adapt serve/async configuration to
   the minimal context.
3. In cloud mode connect Query only to the manifest table and open the
   pre-provisioned pair to load its v2 authority marker; never call any
   ensure/create API. Use a read-only manifest adapter that rejects write/delete
   and legacy latest-pointer migration before KV mutation. Keep metadata/admin
   context opening both distinct pairs.
4. Run focused CLI tests plus existing query wiring, selftest, and cloud wiring
   tests; then strict clippy.

### Task 3: Update deployment and operator contract

**Files:**
- Modify: `deploy/kubernetes/lake.yaml`
- Modify: `crates/lake-cli/tests/kubernetes_manifests.rs`
- Modify: `README.md`
- Modify: `docs/architecture.md`
- Modify: `docs/guides/kubernetes.md`
- Modify: `crates/lake-cli/AGENT.md`

1. Add `LAKE_MANIFEST_DYNAMODB_TABLE` to the reference ConfigMap and make the
   Kubernetes selector require it.
2. Document four authorities separately: catalog registry, Lance manifests,
   async jobs, and object storage. Specify Query Dynamo actions/tables as
   read-only manifest access and explicitly exclude registry access.
3. Document migration as an operator-controlled copy/cutover; do not add a
   runtime fallback to the old shared table.
4. Run the Kubernetes selector, doc checks, fmt, and strict CLI clippy.

### Task 4: Verify, review, and ship

**Files:**
- Create: `verification/issue-124-query-manifest-authority.md`

1. Run all completion selectors and
   `mise run spec-lifecycle specs/issue-124-query-manifest-authority.spec.md`.
2. Run affected strict clippy/check, `git diff --check`, then `mise run gate`.
3. Record exact evidence and request independent correctness/security and
   deployment review; resolve every P0/P1.
4. Commit `refactor(cli): isolate query manifest authority (#124)`, push, open
   the PR, merge only after APPROVE, and verify #124 closes.
