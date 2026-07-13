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

- Query receives read access to `$LAKE_MANIFEST_DYNAMODB_TABLE` and its
  `_prefix_v2` companion plus table/object prefixes.
  Query must have no access to `$LAKE_DYNAMODB_TABLE` or its companion;
  catalog reads use authenticated Metasrv Flight RPCs.
- Query receives conditional read/write access to the separate async-query
  tables and read/write/delete access to `LAKE_ASYNC_RESULT_PREFIX`.
- Metasrv receives registry conditional-write, manifest conditional-write, and
  table/object read/write access.

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

Separately provision `$LAKE_MANIFEST_DYNAMODB_TABLE` (HASH key `pk`) and its
`_prefix_v2` companion. The name must differ from `$LAKE_DYNAMODB_TABLE`.
Metasrv mutates these pre-provisioned physical Lance pointers; Query startup never
creates tables and its workload identity should receive only
`dynamodb:DescribeTable` and `dynamodb:GetItem` for the manifest pair. The
read-only adapter rejects missing latest pointers before any prefix enumeration
or mutation.

For an existing shared-table deployment, cut over the manifest authority
independently from the registry migration:

1. While the old authority is live, ensure every dataset has a fixed
   `lance-manifest-latest/` pointer. Query will not perform this migration.
2. Pause metadata writes. Copy `lance-manifest/`, `lance-manifest-latest/`, and
   `lance-manifest-cleanup/` from the old registry table into the new manifest
   v1 table, then verify exact key/value equality for those three families.
3. With a one-shot migration identity, override `LAKE_DYNAMODB_TABLE` only in
   the migrator shell (never in the runtime ConfigMap), and run bounded pages
   until `page.complete` is `true`:

   ```bash
   export LAKE_DYNAMODB_TABLE="$LAKE_MANIFEST_DYNAMODB_TABLE"
   lake dynamo-migrate --page-size 500 --json
   ```

4. Keep writes paused and finalize the manifest pair independently:

   ```bash
   lake dynamo-migrate --page-size 500 --finalize \
     --acknowledge-dual-rollout --acknowledge-write-quiescence --json
   ```

   Require `verification.finalized=true` and equal `legacy_items`/`v2_items`.
   The migration identity needs create/scan/query/read/write permissions on the
   manifest pair; runtime Query does not.
5. Restore the ordinary registry environment, deploy the distinct manifest
   table name to every Metasrv and Query process, wait for readiness, then
   resume writes. Retain the old manifest keys for at least one append-operation
   retention horizon. Rollback requires another write pause and exact reverse
   synchronization; never point Query at the registry table as a live fallback.

The Query adapter rejects all manifest mutations, including legacy
latest-pointer installation. Lake intentionally does not fall back to registry
storage when the manifest table is missing.

When `LAKE_ASYNC_QUERIES=true`, also provision
`$LAKE_ASYNC_DYNAMODB_TABLE` and
`$LAKE_ASYNC_DYNAMODB_TABLE_prefix_v2` with the same key schemas. These tables
hold only bounded async coordination records and are deliberately separate
from the registry tables. Result jobs, Arrow IPC parts, and manifests live
under `LAKE_ASYNC_RESULT_PREFIX`; lifecycle cleanup requires List, Get, Put,
and DeleteObject on that exact prefix.

The reference ConfigMap also sets four total async workers, one worker per
tenant, and a 30-minute execution deadline. Tune
`LAKE_ASYNC_WORKER_CONCURRENCY`,
`LAKE_ASYNC_WORKER_CONCURRENCY_PER_TENANT`, and
`LAKE_ASYNC_EXECUTION_TIMEOUT_MS` together with pod CPU, memory, spill, and
result-prefix capacity. Replica autoscaling multiplies aggregate worker
capacity; these settings are not cluster-global quotas.

The ConfigMap also sets `LAKE_ASYNC_MAX_OUTSTANDING_PER_TENANT=8` and
`LAKE_ASYNC_MAX_RESULT_BYTES=17179869184`. Unlike worker concurrency, these
limits are enforced by shared durable state: every new job reserves one tenant
entry before object upload, and its result ceiling is immutable in the job
record. Allowed ranges are 1..=128 jobs and 64 MiB..=256 GiB. A failed replica
may temporarily over-count until the five-minute reservation grace is
reconciled by bounded point reads; it never admits capacity by under-counting a
live record. Size storage and lifecycle policies for the result prefix from
these retained-object bounds; they do not reserve pod memory or CPU.

### Async schema-v2 rollout

Async schema-v2 records are intentionally not readable by a schema-v1 Query
binary. Do not use a rolling rollout, do not mix images, and do not mix durable
quota values against one shared async authority. The reference `lake-query`
Deployment therefore uses `strategy: Recreate`.

For the first v2 enablement, pause `PollFlightInfo` async submission at the
edge, leave one schema-v1 worker fleet running until every existing v1 async
record has expired and fenced cleanup has removed its `async-query/` state and
scoped result objects, then replace the entire Query fleet and resume
admission. The same drain is required before changing either durable quota
value. Once any v2 record exists, an old image must not be rolled back into the
async fleet: forward-fix the v2 image, or pause admission and drain all v2
records/objects before a full old-image replacement. This procedure is
necessary because v1 jobs have no durable tenant reservation and must never be
silently counted as v2 jobs.

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

The reference gives each Query replica 64 aggregate slots and 8 slots per
authenticated tenant, with 4096 bounded tenant trackers. Because these limits
are replica-local, autoscaling changes cluster aggregate capacity. Keep the
per-tenant value proportional to pod size and use a tenant-aware load balancer
or a future distributed quota service when a strict cluster-wide entitlement
is required.

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
