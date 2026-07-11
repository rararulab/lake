# Bounded Metasrv Shutdown Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:test-driven-development task-by-task.

**Goal:** Bound Flight drain, maintenance termination, and leadership-campaign
termination with one Metasrv shutdown deadline.

**Architecture:** The serve function records a Tokio `Instant` when shutdown is
requested and passes that deadline to a private owned-task join helper. The
helper joins maintenance and campaign concurrently, aborts and reaps both on
timeout, and returns a typed error. Maintenance receives its cancellation token
inside each sweep and checks it at table boundaries while allowing one active
table mutation to finish normally before the deadline.

**Tech Stack:** Rust, Tokio tasks/time/select, tokio-util CancellationToken,
tonic Flight, Snafu, agent-spec.

---

### Task 1: Specify and prove the owned-task timeout

**Files:**
- Modify: `crates/lake-metasrv/src/lib.rs`

1. Add `background_shutdown_aborts_owned_tasks_at_total_deadline`. Spawn two
   pending tasks that each hold an `Arc`, then call the not-yet-existing join
   helper with a short deadline.
2. Run `cargo test -p lake-metasrv background_shutdown_aborts_owned_tasks_at_total_deadline`
   and confirm RED because the helper and typed timeout do not exist.
3. Add `MetasrvError::BackgroundDrainTimeout`, plus a private helper that uses
   `timeout_at` around concurrent joins; on timeout abort and await both handles.
4. Re-run the focused test and confirm PASS with both `Arc` strong counts back
   to one.
5. Commit the helper and test.

### Task 2: Stop maintenance at table boundaries

**Files:**
- Modify: `crates/lake-metasrv/src/maintenance.rs`

1. Add `maintenance_shutdown_stops_before_next_table` with two registrations
   and a test engine that pauses its first `maintain` call and counts calls.
2. Run the focused test and confirm RED because the current sweep has no
   cancellation input and starts the second table.
3. Add `sweep_until(metasrv, shutdown)`; check cancellation before registry
   table enumeration, before waiting for each table lock, and before starting
   each later table. Keep `sweep` as the non-cancelled test wrapper.
4. Route `run_maintenance_loop_until` through `sweep_until` and re-run the test,
   expecting exactly one engine-maintenance call.
5. Commit the cancellation-boundary change.

### Task 3: Apply one total deadline to server shutdown

**Files:**
- Modify: `crates/lake-metasrv/src/lib.rs`
- Modify: `crates/lake-metasrv/AGENT.md`
- Modify: `docs/architecture.md`
- Modify: `docs/guides/cli.md`

1. Record `Instant::now() + shutdown_grace` at explicit shutdown, use
   `timeout_at` for Flight drain, then reuse the same deadline for background
   cleanup. For spontaneous server exit, create a fresh cleanup deadline.
2. Cancel maintenance before draining Flight, drop the server before cancelling
   campaign, and call the concurrent join helper after campaign cancellation.
3. Preserve the existing crash branch and error precedence: Flight drain errors
   remain primary after owned tasks have been reaped.
4. Document that `LAKE_SHUTDOWN_GRACE_MS` is one total Metasrv shutdown budget.
5. Run existing normal-shutdown and leadership-resignation tests.
6. Commit server integration and documentation.

### Task 4: Verify and publish

**Files:**
- Create: `verification/issue-57-bounded-shutdown.md`

1. Run `mise run spec-lifecycle specs/issue-57-bounded-shutdown.spec.md`.
2. Run `cargo test -p lake-metasrv`, two-node tests, nightly rustfmt, and strict
   clippy with all targets/features and warnings denied.
3. Run `mise run gate`.
4. Obtain independent correctness review and release verification, record the
   evidence, then push, open the PR, and merge only after APPROVE/PASS.
