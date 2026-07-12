# Per-tenant Query admission plan

## Goal

Prevent one authenticated tenant from consuming every planning, execution,
discovery, or async-result download slot on a Query replica while retaining
the existing aggregate process ceiling.

## Invariants

- This policy is per replica. It adds no metadata-authority traffic and makes
  no cluster-global quota claim.
- A request acquires its tenant permit before joining the global queue. Tenant
  waiters therefore cannot reserve global capacity.
- Tenant and global acquisition share one absolute queue deadline.
- The returned RAII permit owns both levels through the existing Flight stream
  lifecycle; every error, cancellation, timeout, and drop releases both.
- Tenant gate state is bounded. Inactive gates are weakly referenced and
  reclaimed synchronously when another tenant arrives; no cleanup task exists.
- Metrics record only finite outcome classes, never tenant identity.

## Configuration

- `LAKE_QUERY_MAX_CONCURRENT_PER_TENANT`, default 8, must be within the global
  concurrency limit.
- `LAKE_QUERY_MAX_TRACKED_TENANTS`, default 4096, must be within 1..=65536.
- Existing constructor callers retain their API and default to a valid tenant
  policy derived from the global limit.

## Sequence

1. Add concurrency and lifecycle tests that fail against the global-only gate.
2. Add the bounded weak tenant-gate registry and dual RAII permit.
3. Thread authenticated principals through every admission call.
4. Add startup validation, deployment defaults, metrics, and documentation.
5. Run the spec lifecycle and full gate before merge.

