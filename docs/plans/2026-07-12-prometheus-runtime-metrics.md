# Prometheus runtime metrics implementation plan

**Goal:** Make Query and Metasrv saturation, authority, and background health
measurable through a bounded, privately scraped Prometheus surface.

**Architecture:** Domain crates emit low-cardinality metrics through the
`metrics` facade. The CLI owns one recorder, a loopback-only Axum endpoint, and
an upkeep task under the same cancellation lifecycle as the selected server.

## Task 1: Establish the owned exporter

1. Add failing configuration and endpoint lifecycle tests.
2. Parse optional `LAKE_METRICS_ADDR` and reject non-loopback/hostname values.
3. Install the recorder, bind before service startup, and own HTTP/upkeep tasks.
4. Refactor CLI server shutdown to cancel and join metrics on every exit path.

## Task 2: Instrument Query

1. Wrap admission permits so the inflight gauge follows the real permit
   lifetime, including streaming responses.
2. Count admitted, saturated, shutting-down, and SQL-size rejection outcomes.
3. Count initial/background catalog refresh results and publish readiness.
4. Test rendered exposition with a task-local recorder.

## Task 3: Instrument Metasrv

1. Wrap append permits and reserved-byte accounting through commit/forwarding.
2. Count bounded campaign outcomes and publish exact write readiness.
3. Count bounded maintenance pages/items after each stage.
4. Test rendered exposition without any user- or data-derived labels.

## Task 4: Document and ship

1. Document enablement, sidecar collection, metric names, and label policy.
2. Run guarded lifecycle, strict clippy, gate, ship, independent review, and
   independent verification.
3. Merge one reviewed PR and verify main.
