# Metasrv Append Admission Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Bound each Metasrv process's concurrent FILE appends and worst-case
buffered Flight control metadata without changing object upload or commit
semantics.

**Architecture:** Add validated `AppendLimits` and a process-local
`AppendAdmission` containing concurrency and byte semaphores. One combined RAII
permit reserves a concurrency slot plus the configured per-stream worst case
before payload polling, and remains owned by `DoPut` through follower
forwarding or local commit/response. Existing buffering and pre-commit digest
verification remain intact, but the per-stream limit becomes configuration.

**Tech Stack:** Rust 2024, Tokio owned/weighted semaphores and timeout, tonic
Arrow Flight, jj, agent-spec.

---

### Task 1: Validated append limits and deployment parsing

**Files:**
- Modify: `crates/lake-metasrv/src/lib.rs`
- Modify: `crates/lake-cli/src/commands/limits.rs`
- Modify: `crates/lake-cli/src/commands/serve.rs`
- Test: `crates/lake-cli/src/commands/limits.rs`

**Step 1: Write the failing test**

Add `append_limit_values_are_validated_before_serving`. It must reject zero,
non-integer, `max_buffered_bytes < max_stream_bytes`, and byte values that do
not fit a `u32` weighted permit, then assert valid values through accessors.

**Step 2: Run test to verify it fails**

Run: `cargo test -p lake-cli append_limit_values_are_validated_before_serving -- --nocapture`
Expected: compile failure because `AppendLimits` and parser do not exist.

**Step 3: Write minimal implementation**

Add public `AppendLimits` with fields/accessors:

```rust
pub struct AppendLimits {
    max_concurrent: usize,
    queue_wait: Duration,
    max_stream_bytes: usize,
    max_buffered_bytes: usize,
}
```

`try_new` validates non-zero values, buffer >= stream, and both byte values
convert to `u32`. Defaults: `8`, `100ms`, `64 * 1024 * 1024`,
`256 * 1024 * 1024`. Add it to `MetasrvServerConfig` and parse the four spec
environment variables before server bind.

**Step 4: Run test to verify it passes**

Run the focused CLI test. Expected: PASS.

**Step 5: Commit**

Run: `jj commit -m "feat(metasrv): configure append admission limits (#53)" -m "Closes #53"`.

### Task 2: Combined concurrency and worst-case byte admission

**Files:**
- Modify: `crates/lake-metasrv/src/control.rs`
- Test: `crates/lake-metasrv/src/control.rs`

**Step 1: Write failing tests**

Add `append_admission_rejects_concurrency_saturation_and_releases` and
`append_admission_reserves_worst_case_buffer_budget`. The first uses one
concurrency permit with ample bytes. The second uses two concurrency permits
but only one stream's byte reservation. Both retain the first permit past the
queue timeout, assert `ResourceExhausted`, drop it, then assert acquisition.

**Step 2: Run tests to verify they fail**

Run: `cargo test -p lake-metasrv append_admission_ -- --nocapture`
Expected: compile failure because `AppendAdmission` does not exist.

**Step 3: Write minimal implementation**

Add cloneable `AppendAdmission` with `Arc<Semaphore>` fields. Its `acquire`
wraps sequential acquisition of one owned concurrency permit and
`max_stream_bytes` owned byte permits in one `tokio::time::timeout`. Return a
private `AppendPermit` owning both guards. Timeout maps to
`Status::resource_exhausted("append admission limit reached")`; semaphore
closure maps to `Unavailable`.

**Step 4: Run tests to verify they pass**

Run both focused tests. Expected: PASS.

**Step 5: Commit**

Run: `jj commit -m "feat(metasrv): reserve append concurrency and memory (#53)" -m "Closes #53"`.

### Task 3: Hold admission across Flight forwarding and commit

**Files:**
- Modify: `crates/lake-metasrv/src/control.rs`
- Modify: `crates/lake-metasrv/src/lib.rs`
- Modify: `crates/lake-metasrv/tests/two_node_forwarding.rs`
- Test: `crates/lake-metasrv/src/control.rs`
- Test: `crates/lake-metasrv/tests/two_node_forwarding.rs`

**Step 1: Write failing stream-limit test**

Rename/extend the existing oversized payload case as
`configured_append_stream_limit_rejects_before_commit`. Configure a tiny
`AppendLimits::max_stream_bytes`, assert `ResourceExhausted`, and verify the
registry version is unchanged.

**Step 2: Write failing two-node lifecycle test**

Add `forwarded_append_holds_admission_until_commit_finishes`. Wrap the shared
engine's table handle so its first `append_reserved` notifies and pauses. Start
two nodes with one-slot append limits, send the first append through the
follower, wait for the leader's paused commit, and assert a second append to
that follower returns `ResourceExhausted`. Release the first and verify a later
append is admitted.

**Step 3: Run tests to verify they fail**

Run each selector. Expected: stream limit remains constant and concurrent
forwarded append is admitted.

**Step 4: Wire the permit**

Construct one shared `AppendAdmission` when serving and store it on
`MetasrvFlightService`. In `DoPut`, authenticate, acquire the combined permit,
then poll the first message. Keep the guard in scope through `forward_put` or
`append_file_stream`; pass configured `max_stream_bytes` to the local buffer
validator. Do not release before the post-commit result gate/response is built.

**Step 5: Run tests to verify they pass**

Run both focused selectors and all Metasrv tests. Expected: PASS.

**Step 6: Commit**

Run: `jj commit -m "perf(metasrv): admit FILE append lifetimes (#53)" -m "Closes #53"`.

### Task 4: Documentation, gates, and release

**Files:**
- Modify: `crates/lake-metasrv/AGENT.md`
- Modify: `crates/lake-cli/AGENT.md`
- Modify: `docs/architecture.md`
- Modify: `docs/guides/cli.md`
- Create: `verification/issue-53-append-admission.md`

**Step 1: Document operational contract**

Record defaults, environment variables, worst-case reservation semantics,
`ResourceExhausted`, and the permit lifetime across follower/leader paths.

**Step 2: Run task verification**

Run nightly fmt check, `cargo test -p lake-metasrv`, `cargo test -p lake-cli`,
strict clippy for both crates, and
`mise run spec-lifecycle specs/issue-53-append-admission.spec.md`.
Expected: lifecycle 5/5 and all commands pass.

**Step 3: Run repository gate**

Run: `mise run gate`
Expected: hooks, workspace tests, e2e, and site checks pass.

**Step 4: Independent review and verification**

Reviewer attacks acquisition order/deadlock, cancellation, double admission
on forwarding, exact buffer arithmetic, leadership changes, and commit/result
lifetimes. Verifier independently checks boundaries, selectors, strict clippy,
and full gate.

**Step 5: Record evidence and merge**

After APPROVE/PASS, write verification, commit, push
`issue-53-append-admission`, open a PR closing #53, merge, and confirm main plus
issue state.
