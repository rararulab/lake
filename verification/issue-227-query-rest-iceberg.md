# Issue #227 verification

## Delivered contract

- `QueryEngine::execute_sql` now avoids a Lake registry refresh only when
  DataFusion parsing proves that every physical table reference is a fully
  qualified `iceberg.<namespace>.<table>` reference.
- A loopback REST catalog test connects the real `IcebergCatalog` adapter,
  reads its external table through `QueryEngine`, and proves no Lake metadata
  listing is consulted.
- Parse failures, statements without table references, unqualified tables,
  Lake tables, and mixed Lake/Iceberg SQL remain conservative and refresh the
  Lake catalog as before.
- The temporary fixture uses `file://` table locations, matching the URI
  contract required by the OpenDAL storage factory used by a REST-connected
  catalog.

## Red/green evidence

- Before the implementation, the new REST-through-QueryEngine test failed
  after reading the external row because the unconditional `plan_sql` refresh
  performed one Lake catalog scan.
- The initial fixture also demonstrated why absolute filesystem paths are not
  a valid external Iceberg object URI for the REST/OpenDAL route; using the
  protocol-correct `file://` location made the scan reach the intended
  metadata-boundary assertion.
- After the guarded bypass, the same test passes with zero Lake scans, while
  the mixed/unknown-reference test retains the existing refresh behavior.

## Verification

- `mise run spec-lint specs/issue-227-query-rest-iceberg.spec.md` — PASS
  (100%).
- `mise run spec-lifecycle specs/issue-227-query-rest-iceberg.spec.md` — PASS
  (all three selectors matched real tests).
- `cargo +nightly fmt --check --all` — PASS.
- `cargo clippy -p lake-query --lib --tests --all-features -- -D warnings` — PASS.
- `cargo test -p lake-query --lib` — PASS (107 tests).
