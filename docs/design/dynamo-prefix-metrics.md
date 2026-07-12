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
Runtime nodes refresh the barrier strongly consistently at most once per 30
seconds with a 100 ms telemetry-only timeout. Failure retains the last-known
gauge and never changes startup or metastore error semantics.

## Rollout signals

After finalization and the required runtime restart:

- `max by (service, instance) (lake_dynamo_v2_authoritative)` must be `1` for
  every Query and Metasrv target; also alert on
  `absent_over_time(lake_dynamo_v2_authoritative[5m])`.
- `sum by (service, instance)
  (rate(lake_dynamo_prefix_requests_total{layout="v1"}[5m]))` must fall to
  zero.
- Filter amplification is:
  `sum by (service, instance, layout, api)
  (rate(lake_dynamo_prefix_items_total{kind="evaluated"}[5m])) /
  sum by (service, instance, layout, api)
  (rate(lake_dynamo_prefix_items_total{kind="returned"}[5m]))`.
- Physical fan-out per returned item is:
  `sum by (service, instance, layout, api)
  (rate(lake_dynamo_prefix_requests_total{outcome="success"}[5m])) /
  sum by (service, instance, layout, api)
  (rate(lake_dynamo_prefix_items_total{kind="returned"}[5m]))`. A positive
  request rate with zero returns intentionally becomes infinite and should
  page sustained empty-shard traffic.
- A held barrier with any v1-authoritative runtime is a rollout page: keep
  write admission paused and finish the v2 rollout.
