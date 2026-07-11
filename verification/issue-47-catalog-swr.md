# Verification: catalog stale-while-revalidate

Issue: #47
Head: `81edbb15`

## Result

PASS. Independent correctness review approved the refresh publication,
singleflight, failure fallback, and task-lifetime interleavings. Independent
verification passed all spec selectors, boundaries, and repository gates.

## Evidence

- `mise run spec-lifecycle specs/issue-47-catalog-swr.spec.md`: 8/8 passed.
- `cargo test -p lake-catalog`: 13 passed.
- `cargo test -p lake-query --lib`: 23 passed.
- Strict clippy for all targets/features: passed.
- `mise run gate`: workspace tests, e2e, two-node tests, hooks, and site
  typecheck/test/build passed.
- Boundary audit: every changed path is allowed; forbidden paths: zero.

## Availability and lifecycle regressions

- Warm stale checks return before a paused authority scan and coalesce onto one
  revalidation.
- Failed revalidation preserves last-good and bounded health; recovery
  atomically publishes the replacement.
- Initial warm remains synchronous and propagates authority failure.
- Invalid startup configuration creates no refresher.
- Aborting the serve future releases both scheduled and request-triggered
  refresh tasks; graceful shutdown still aborts/joins them.

## Release notes

No wire or durable metadata format changes. Refresh state is process-local, so
rolling deployment and rollback are safe. The first gate attempt hit host
`errno=28`; after removing old merged-workspace build artifacts, the unchanged
candidate passed full gate twice, including independent verification.
