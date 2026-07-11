# Verification: provider generation cache

Issue: #45
Head: `3c4d932807133574f290e55d69d371ceadbbff23`

## Result

PASS. Independent correctness review approved the generation-keyed provider
cache and the registration invalidation epoch fence. Independent verification
confirmed all six spec scenarios and repository gates.

## Evidence

- `cargo test -p lake-catalog`: 8 passed.
- `cargo test -p lake-query`: 21 passed, including the integration test.
- `cargo clippy -p lake-catalog --all-targets --all-features -- -D warnings`:
  passed.
- `mise run spec-lifecycle specs/issue-45-provider-generation-cache.spec.md`:
  6/6 scenarios passed.
- `mise run gate`: workspace tests, self-test e2e, two-node tests, and site
  typecheck/test/build passed.
- Boundary audit: all changed paths allowed; no forbidden path changed.

## Race regression

`invalidation_fences_an_inflight_stale_registration_fill` pauses an old v1
registry read, publishes v2 and completes query-local invalidation, then
releases the old fill. The old value is confined to its prior epoch; the next
lookup builds the v2 provider.

## Release notes

No wire or durable storage format changes. Both caches and the invalidation
epoch are process-local to each Query replica, so rolling deploy and rollback
are safe; restart simply cold-starts the caches.
