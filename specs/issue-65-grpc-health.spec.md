spec: task
name: "grpc-health-readiness"
inherits: project
tags: [query, metasrv, grpc, health, readiness, operations]
---

## Intent

Production schedulers currently know only whether the Flight TCP port accepts a
connection. That is insufficient: Query must not receive traffic before its
first complete catalog generation is warm, and a Metasrv node must not claim
write readiness before it holds a live lease or knows a different leader to
which it can forward. During graceful shutdown both tiers must withdraw before
connection drain begins.

Expose those existing authority boundaries through the standard gRPC Health
Checking protocol on the same secured server port.

## Decisions

- Add `tonic-health` to Query and Metasrv. Register the standard empty service
  name as process liveness and `arrow.flight.protocol.FlightService` as traffic
  readiness.
- Keep health behind the same TLS and bearer interceptor as Flight. Health is
  operational metadata, not an authentication bypass; probes must use the
  configured server identity.
- Query becomes reachable and reports Flight `SERVING` only after its strict
  initial catalog refresh succeeds. A startup refresh failure never binds.
- Metasrv liveness is `SERVING` once the server is reachable. Flight readiness
  starts `NOT_SERVING` and becomes `SERVING` only while the node has an
  unexpired local lease or knows a different leader address for forwarding.
  A remembered self address after local lease expiry is not ready.
- Leadership publications wake the readiness monitor; the local lease deadline
  also drives an exact timer so readiness cannot outlive authority while a
  renewal is stuck.
- On injected shutdown, set both liveness and Flight readiness to
  `NOT_SERVING` before cancelling the Tonic server. Health `Watch` clients may
  observe withdrawal during the existing bounded drain window.
- Health tasks are owned, cancelled, and joined under the existing total
  shutdown deadline. No detached monitor survives serve return.

## Boundaries

### Allowed Changes
Cargo.toml
Cargo.lock
crates/lake-query/**
crates/lake-metasrv/**
docs/architecture.md
docs/design/meta-server.md
docs/guides/cli.md
docs/plans/2026-07-12-grpc-health-readiness.md
specs/issue-65-grpc-health.spec.md
verification/issue-65-grpc-health.md

### Forbidden
crates/lake-cli/**
crates/lake-sdk/**
crates/lake-meta/**
unauthenticated health bypass
separate HTTP or probe listener
claiming Metasrv write readiness with no usable route
detached health-monitor tasks
changing lease or Flight wire formats

## Completion Criteria

Scenario: Query exposes authenticated readiness after strict warmup
  Test:
    Package: lake-query
    Filter: query_grpc_health_requires_auth_and_reports_serving
  Given a secured Query server whose first catalog refresh completed
  When health is checked with and without its bearer identity
  Then the unauthenticated check is rejected and the authenticated empty and Flight services report SERVING

Scenario: Query withdraws health before graceful drain
  Test:
    Package: lake-query
    Filter: query_health_watch_observes_not_serving_on_shutdown
  Given an authenticated health Watch stream on a serving Query node
  When injected shutdown fires
  Then the stream observes NOT_SERVING before the server returns

Scenario: Metasrv readiness follows a usable write route
  Test:
    Package: lake-metasrv
    Filter: metasrv_health_tracks_leader_route_and_lease_expiry
  Given a serving Metasrv with campaign progress under test control
  When it has no leader, learns a remote leader, and later holds then expires a local lease
  Then Flight readiness transitions NOT_SERVING, SERVING, SERVING, and NOT_SERVING while liveness stays SERVING

Scenario: Metasrv health withdrawal is owned by shutdown
  Test:
    Package: lake-metasrv
    Filter: metasrv_health_watch_withdraws_and_monitor_joins
  Given a ready Metasrv and an authenticated health Watch stream
  When injected shutdown fires
  Then liveness and Flight readiness withdraw before return and no health task remains

## Out of Scope

- Prometheus metrics, OTLP tracing, or HTTP probes.
- Kubernetes manifests or load-balancer configuration.
- Unauthenticated health endpoints or a second listener.
- Changing catalog staleness policy or metadata election TTL.
