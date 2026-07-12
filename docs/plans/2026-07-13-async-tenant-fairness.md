# Async tenant fairness implementation plan

## Failure mode

The durable async scan loop waits for an entire state page to finish and starts
jobs only under one replica-wide concurrency number. A tenant can therefore
keep every worker busy with lease-renewed scans while another tenant's job is
visible but never selected. Worker execution also has no absolute deadline.

## Design

1. Decode validated `AsyncQueryRecord` candidates directly from each bounded
   metadata scan page. This removes the current candidate point-read fan-out.
2. Keep a bounded scheduler state containing only active query IDs and active
   counts per tenant. Select eligible page candidates in tenant round-robin
   order; a tenant at its ceiling is skipped and never owns a worker task.
3. Own worker tasks in a `JoinSet`. Scan ticks continue while jobs run, and task
   completion releases both replica and tenant capacity. Shutdown aborts and
   joins all owned tasks.
4. Move lease renewal into the worker future itself and race execution against
   one monotonic deadline. Timeout stops execution and renewal before writing
   the stable `execution_timeout` terminal code with the worker lease.
5. Validate the async per-tenant ceiling and execution timeout before listener
   startup. Export only finite scheduler outcome labels and an aggregate active
   gauge.

## Non-goals

This does not create a cluster-global quota, durable tenant queue index, or
byte/memory accounting. Result encoding and download buffering remain a
separate P0 because they require a streaming representation change.
