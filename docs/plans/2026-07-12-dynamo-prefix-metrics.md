# Dynamo prefix metrics implementation plan

**Goal:** Make #95 authority, amplification, and migration state observable
with finite, identity-free Prometheus series.

1. Add RED recorder tests for physical request work, hostile identity strings,
   authority gauge transitions, and migration outcomes.
2. Add a private `lake-meta` telemetry module and the workspace `metrics`
   dependency; keep every label value compile-time bounded.
3. Instrument every v1 Scan and v2 Query request at the AWS response boundary,
   including errors and Dynamo evaluated/returned counts.
4. Instrument authority refresh and durable barrier observation without
   changing protocol control flow; keep one-shot migration outcomes in JSON.
5. Document rollout PromQL, run lane-1 lifecycle, LocalStack integration,
   strict clippy, full gate, docs, and independent review.
