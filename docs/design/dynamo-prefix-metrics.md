# Dynamo prefix-layout metrics

## Goal

Operators must be able to prove that every runtime pod has observed v2
authority and that prefix work is proportional to returned metadata. Metrics
must not expose logical keys, prefixes, tenants, table names, endpoints, or
migration cursors.

## Contract

`lake-meta` exports only finite labels:

| Metric | Labels | Meaning |
|---|---|---|
| `lake_dynamo_v2_authoritative` | none | This process reads v2 (`1`) or v1 (`0`) |
| `lake_dynamo_finalize_barrier_held` | none | Durable migration write barrier observed at startup/finalize |
| `lake_dynamo_prefix_requests_total` | `layout`, `api`, `outcome` | Physical Scan/Query requests |
| `lake_dynamo_prefix_items_total` | `layout`, `api`, `kind` | Evaluated and returned items |

Allowed label values are compile-time constants. Prefix/key text appears only
in errors returned to the trusted caller, never in metric names or labels.
The one-shot migration CLI reports page and finalize outcomes in its JSON
response; it does not start a long-lived scrape endpoint. Runtime metrics
therefore expose only durable migration state (authority and barrier).

## Rollout signals

After finalization and the required runtime restart:

- `max(lake_dynamo_v2_authoritative) by (service, instance)` must be `1` for
  every Query and Metasrv target.
- The rate of `lake_dynamo_prefix_requests_total{layout="v1"}` must fall to
  zero.
- `rate(lake_dynamo_prefix_items_total{kind="evaluated"}) /
  rate(lake_dynamo_prefix_items_total{kind="returned"})` exposes prefix-read
  amplification without key-cardinality risk.
- A held barrier with any v1-authoritative runtime is a rollout page: keep
  write admission paused and finish the v2 rollout.
