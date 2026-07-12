# Idempotent Stage Cleanup Race Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make concurrent same-operation append and recovery tolerate terminal reference-stage cleanup without weakening fail-closed lineage.

**Architecture:** Keep the durable operation-derived stage and immediate cleanup. Add test-only barriers to prove both observation→read races, then recover only through existing durable authorities: transaction history for pre-commit staging loss and complete final sidecars for finalization staging loss. No lock or timing retry participates in production correctness.

**Tech Stack:** Rust 2024, Tokio `Notify`, Lance 8 transaction history, object_store create/delete semantics, agent-spec.

---

### Task 1: Deterministically reproduce the persistence race

**Files:**
- Modify: `crates/lake-engine-lance/src/lib.rs`

**Step 1:** Add cfg(test) `StageCleanupRaceHooks` to `WriteConfig`, with barriers for first-writer staging, contender AlreadyExists observation, and contender read release.

**Step 2:** Add `same_operation_append_survives_terminal_stage_cleanup`: pause the first append after staging, pause the contender after AlreadyExists, let the winner commit/finalize/delete, then release the contender.

**Step 3:** Run `cargo test -p lake-engine-lance same_operation_append_survives_terminal_stage_cleanup`; expect RED with staging chunk NotFound from the contender.

**Step 4:** Change `append_reserved` so a staging-persistence error first reconciles the exact operation; return the durable committed version only when reconciliation proves it, otherwise return the original error.

**Step 5:** Re-run the selector; expect both calls to return version 2 and staging to be absent.

### Task 2: Deterministically reproduce the finalization race

**Files:**
- Modify: `crates/lake-engine-lance/src/lib.rs`

**Step 1:** Extend the cfg(test) hook with a first-finalizer barrier after the initial final-sidecar completeness check and before staged chunk 0 is read.

**Step 2:** Add `concurrent_recovery_survives_terminal_stage_cleanup`: construct the existing crash window, pause reconciler A, let reconciler B finalize and delete, then resume A.

**Step 3:** Run the selector; expect RED with staging chunk NotFound from reconciler A.

**Step 4:** On staged-chunk NotFound inside finalization, re-run `final_reference_chunks_complete`; return success only if the complete committed sidecar set is now durable, otherwise propagate the original error.

**Step 5:** Re-run the selector and the incomplete-lineage tests; expect PASS without retries.

### Task 3: Lock payload-conflict semantics

**Files:**
- Modify: `crates/lake-engine-lance/src/lib.rs`

**Step 1:** Add `different_payload_replay_remains_idempotency_conflict` using a committed append and a replay with the same tenant/operation ID but a distinct valid digest.

**Step 2:** Run the selector against the fixed code; expect `EngineError::IdempotencyConflict` and no new table version.

**Step 3:** Run all `lake-engine-lance` tests repeatedly enough to exercise ordinary scheduling as well as the deterministic barriers.

### Task 4: Document and verify

**Files:**
- Modify: `crates/lake-engine-lance/AGENT.md`
- Modify: `docs/architecture.md`
- Create: `verification/issue-86-idempotent-stage-cleanup-race.md`

**Step 1:** Document that staging disappearance is resolved only from transaction history or complete final reference lineage.

**Step 2:** Run rustfmt, clippy with warnings denied, spec lifecycle, package tests, full gate, and strict rustdoc.

**Step 3:** Commit a fixed candidate and obtain independent correctness review and verifier PASS.

**Step 4:** Push, open a PR closing #86, and merge after all evidence is green.
