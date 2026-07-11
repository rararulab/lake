# Verification: immutable catalog generations

Issue: #49
Head: `150f627a`

## Result

PASS. Independent correctness review approved the immutable generation
publication, request pinning, refresh interleavings, and Flight filter
semantics. Independent verification passed all spec selectors, boundaries,
and repository gates.

## Evidence

- `mise run spec-lifecycle specs/issue-49-catalog-generation.spec.md`: 5/5
  passed.
- `cargo test -p lake-catalog --lib`: 16 passed.
- `cargo test -p lake-query --lib`: 26 passed.
- Strict clippy for catalog/query, all targets and features: passed.
- `mise run gate`: workspace tests, e2e, hooks, and site
  typecheck/test/build passed.
- Boundary audit: every changed path is allowed; forbidden paths: zero.

## Generation regressions

- Readers clone only the published `Arc<CatalogGeneration>` and cannot mutate
  its private listing or schema maps.
- A reader pinned to generation A continues to return A's names and schemas
  after refresh publishes generation B.
- A failed full refresh preserves the exact published Arc identity.
- Flight table discovery resolves names and schemas from one request-pinned
  generation without authority I/O or a deep catalog clone.
- Catalog, schema, table, type, and authorization filters run before schema
  lookup and row allocation. A filtered-out legacy table with no cached schema
  cannot fail a matching request.

## Release notes

No wire or durable metadata format changes. Generations are process-local and
replace the previous process-local snapshot representation, so rolling deploy
and rollback are safe.
