# Catalog stale-while-revalidate implementation plan

Issue: #47

## Outcome

After startup warm, SQL planning always uses the last fully published catalog
generation and never waits for registry I/O. One process-local revalidation
runs at a time; failure is observable but does not discard readable state.

## Steps

1. Separate catalog refresh serialization from last-success and health state.
2. Keep first warm synchronous; make runtime stale checks atomically spawn one
   revalidation and return immediately.
3. Publish a replacement snapshot and success timestamp only after a complete
   scan; retain last-good and bounded failure state on error.
4. Make the Query background loop perform explicit scheduled refresh while
   request planning only triggers non-blocking stale checks.
5. Add deterministic paused/failing metastore tests plus a Query planning
   regression, then run strict clippy, spec lifecycle, gate, review, and
   verification.

## Safety properties

- No partial or empty replacement is published after runtime failure.
- At most one detached request-triggered refresh exists per replica.
- Startup readiness still depends on a successful authority scan.
- Health stores counters/timestamps, not unbounded error history.
- Fallible server configuration runs before task creation, and a server
  lifetime guard cancels scheduled and request-triggered refresh on future
  drop as well as graceful shutdown.
