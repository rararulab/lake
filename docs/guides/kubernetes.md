# Kubernetes reference deployment

[`deploy/kubernetes/lake.yaml`](../../deploy/kubernetes/lake.yaml) is a
production-oriented reference, not a turnkey cluster installer. It keeps the
stateless Query tier separate from the bounded metadata authority and maps the
runtime's existing security, health, metrics, resource, and shutdown contracts
into Kubernetes.

## Build and pin the image

The root `Dockerfile` is multi-stage and runs the final process as numeric user
`65532`. It also copies the versioned `grpc_health_probe` binary used for TLS
and bearer-authenticated exec probes. Build for the target platforms, publish
to your registry, then replace both `ghcr.io/rararulab/lake:0.1.0` references
with an immutable digest:

```bash
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  --tag registry.example/lake@sha256:<digest> \
  --push .
```

Do not deploy a mutable tag in production. The health-probe stage is version
pinned independently so its updates remain visible in review.

## Supply cloud configuration

Edit the `lake-runtime` ConfigMap before applying it. `LAKE_S3_BUCKET` must be
the production bucket; `AWS_REGION`, prefixes, and the DynamoDB table must
match pre-provisioned infrastructure. The manifest deliberately does not
contain static AWS credentials. Annotate the separate `lake-query` and
`lake-metasrv` ServiceAccounts for the cluster's workload-identity mechanism:

- Query receives read/list access to the registry and table/object prefixes.
- Query receives conditional read/write access to the separate async-query
  tables and read/write/delete access to `LAKE_ASYNC_RESULT_PREFIX`.
- Metasrv receives registry conditional-write and table/object read/write
  access.

The exact IAM resources are deployment-specific and are intentionally not
created by this repository. Query and Metasrv both use DynamoDB/S3; neither
StatefulSet identity nor an `emptyDir` is authoritative data.

Provision both `$LAKE_DYNAMODB_TABLE` (HASH key `pk`) and its companion
`$LAKE_DYNAMODB_TABLE_prefix_v2` (HASH `bucket`, RANGE `pk`) with on-demand
billing, or grant the one-shot migration identity `CreateTable`. Roll all
Query and Metasrv pods to a dual-capable image before running
`lake dynamo-migrate`. Do not set `--acknowledge-dual-rollout` while an old
v1-only writer exists. Pause metadata write admission before acknowledging
`--acknowledge-write-quiescence`; after finalization, roll Query and Metasrv so
they observe v2 authority before resuming writes. Runtime identities need
`DescribeTable` plus their normal data-plane permissions, not `CreateTable`.
Keep both table ARNs in the runtime IAM policy and retain v1 for at least one
append-operation retention horizon.

When `LAKE_ASYNC_QUERIES=true`, also provision
`$LAKE_ASYNC_DYNAMODB_TABLE` and
`$LAKE_ASYNC_DYNAMODB_TABLE_prefix_v2` with the same key schemas. These tables
hold only bounded async coordination records and are deliberately separate
from the registry tables. Result jobs, Arrow IPC parts, and manifests live
under `LAKE_ASYNC_RESULT_PREFIX`; lifecycle cleanup requires List, Get, Put,
and DeleteObject on that exact prefix.

A failed finalize leaves its durable barrier held. Keep admission paused,
finish backfill, and rerun finalize. Do not delete the barrier as a routine
rollback mechanism; doing so can re-admit stale dual writers during parity
verification.

After the Dynamo v2 migration and writer rollout are complete, keep metadata
write admission paused and activate catalog directory generations once:

```bash
lake catalog-finalize \
  --acknowledge-writer-rollout \
  --acknowledge-write-quiescence \
  --json
```

Repeat execution is safe and reports `finalized: false` once authority already
exists. Resume writes only after the command succeeds. Do not roll any registry
writer back to an older image afterward; old Query replicas are compatible,
but old writers do not publish the required atomic generation signal.

## Create required secrets

Create two Secrets rather than editing credentials into the manifest. Secret
files are first mounted read-only, then a non-root init container copies them
to a memory-backed volume with mode `0600`; this is required by Lake's
principal-map permission check.

`lake-query-runtime` must contain:

- `tls.crt`, `tls.key`, and `ca.crt`;
- `principals.json` for inbound SDK users and the health principal;
- `ticket-keys.json`, shared byte-for-byte by every Query replica;
- `health-token`, matching a principal-map entry;
- `metadata-token`, matching a `query_service` principal accepted by Metasrv.

`lake-metasrv-runtime` must contain:

- `tls.crt`, `tls.key`, and `ca.crt`;
- `principals.json` for Query, metadata peers, administrators, and health;
- `health-token`, matching a principal-map entry;
- `peer-token`, matching a `metadata_peer` principal.

Create them from protected local files:

```bash
kubectl -n lake-system create secret generic lake-query-runtime \
  --from-file=tls.crt=query/tls.crt \
  --from-file=tls.key=query/tls.key \
  --from-file=ca.crt=ca.crt \
  --from-file=principals.json=query/principals.json \
  --from-file=ticket-keys.json=query/ticket-keys.json \
  --from-file=health-token=query/health-token \
  --from-file=metadata-token=query/metadata-token

kubectl -n lake-system create secret generic lake-metasrv-runtime \
  --from-file=tls.crt=metasrv/tls.crt \
  --from-file=tls.key=metasrv/tls.key \
  --from-file=ca.crt=ca.crt \
  --from-file=principals.json=metasrv/principals.json \
  --from-file=health-token=metasrv/health-token \
  --from-file=peer-token=metasrv/peer-token
```

The ticket key file is protected JSON. Secrets are high-entropy strings of at
least 32 bytes; they are never key identifiers and must not be reused as bearer
credentials. The active secret seals new statement tickets and up to three
verification secrets decrypt tickets created during a rollout:

```json
{
  "active": "replace-with-at-least-32-random-bytes",
  "verification": []
}
```

Rotate without breaking requests crossing Query replicas:

1. **preload** — add the new secret to `verification` while the old secret
   remains `active`, update the Secret, and finish the full Query rollout.
2. **activate** — make the new secret `active`, retain the old secret in
   `verification`, update the Secret, and finish a second full rollout.
3. **retire** — wait longer than `LAKE_QUERY_TICKET_TTL_SECS` after the second
   rollout, remove the old secret, and roll Query once more.

Skipping preload lets a new replica issue tickets that old replicas cannot
decrypt. Keep the file identical across all replicas behind one Flight
endpoint; a remotely exposed Query refuses startup when it is absent.

The first release that introduces encrypted statement tickets is not
wire-compatible with an older Query binary that emits raw SQL handles. Adopt
it with a blue/green Query Service cutover, or stop admission, drain all old
Query connections, replace every replica, and then resume. Do not perform that
one-time binary transition as an ordinary mixed-version rolling update. After
every replica understands this envelope, later key changes use the staged
rolling procedure above without downtime.

The Query certificate must cover
`lake-query.lake-system.svc.cluster.local`; the Metasrv certificate must cover
`lake-metasrv.lake-system.svc.cluster.local`. Replace these names consistently
if the namespace or Services change.

## Health, metrics, and lifecycle

Kubernetes' native gRPC probe cannot attach the bearer metadata and TLS
server-name override required by Lake. The reference therefore uses
`grpc_health_probe` through an exec probe. Liveness checks the empty standard
Health service; readiness checks `arrow.flight.protocol.FlightService`. Do not
replace these with anonymous TCP or native gRPC probes.
An authenticated startup probe grants up to 150 seconds for cold cloud/client
initialization before liveness enforcement begins.

Prometheus listens at `127.0.0.1:9090` and no Service exposes that port. Run a
collector as a sidecar or node agent that can scrape pod loopback; do not
change Lake's listener to a wildcard address.

OTLP tracing remains opt-in. The reference assigns distinct
`OTEL_SERVICE_NAME` values and a five-second owned shutdown bound, but does not
guess a collector. To enable export, add an in-cluster collector origin such as
`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: http://otel-collector:4317` to the
`lake-runtime` ConfigMap. Use a NetworkPolicy to restrict that egress. The
collector is not part of Lake's availability path: an unavailable collector
cannot stop either service, and shutdown remains bounded.

Lake drains for at most 30 seconds after SIGTERM. Kubernetes grants 45 seconds,
leaving time for probe withdrawal and process/container cleanup. The Query
spill `emptyDir` is capped at 16 GiB and is disposable. Tune its memory/spill
budgets and pod limits together: the configured 6 GiB query pool sits below
the 8 GiB container limit.

Metasrv append-operation cleanup defaults to 16 metadata pages of 128 records
per one-minute maintenance tick. Monitor the finite-label
`append_operations/budget_exhausted` and `append_operations/time_exhausted`
maintenance counters and the deleted-item rate. If either ceiling stays
exhausted and deletion trails sustained append throughput, raise
`LAKE_MAINTENANCE_OPERATION_GC_MAX_PAGES`,
`LAKE_MAINTENANCE_OPERATION_GC_MAX_MS`, or
`LAKE_APPEND_OPERATION_GC_PAGE_SIZE` while keeping the product and duration
within the node's Dynamo request, table-maintenance, and shutdown budgets.

## Availability model

- Query is a three-replica Deployment with zero-unavailable rolling updates,
  topology spreading, and a disruption budget. It remains stateless and can be
  autoscaled externally.
- Metasrv is exactly three replicas with stable StatefulSet pod identity,
  topology spreading, and `minAvailable: 2`. It still stores authority in
  DynamoDB. Each pod advertises its downward-API pod IP for leader forwarding;
  advertising `0.0.0.0` would make the elected leader unreachable.
- Flight Services expose only ports 50051/50052. Metrics stay pod-private.

Apply only after replacing the image, cloud values, identities, certificates,
and Secrets:

```bash
mise run k8s-validate
kubectl apply --server-side --dry-run=server -f deploy/kubernetes/lake.yaml
kubectl apply --server-side -f deploy/kubernetes/lake.yaml
kubectl -n lake-system rollout status deployment/lake-query
kubectl -n lake-system rollout status statefulset/lake-metasrv
```

`mise install` provides the pinned kubeconform release used by the first
command. It validates strictly against the pinned Kubernetes 1.32 schema;
server-side dry-run then covers cluster admission and policy.
