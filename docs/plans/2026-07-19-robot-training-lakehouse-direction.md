# Robot Training Lakehouse Direction Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Record the format-neutral robot-training data model and make Lake's authority relationship with Rerun explicit before behavioral implementation begins.

**Architecture:** Keep the existing stateless Query, bounded Metasrv, and direct object-storage data path unchanged. Add a product-level domain model in which Lake owns dataset revisions and training provenance while Rerun, MCAP, and LeRobot integrations sit behind format adapters; logical episodes remain independent of physical object boundaries.

**Tech Stack:** Markdown, Mermaid, existing Lake architecture invariants, Rerun RRD/segment terminology, MCAP, LeRobotDataset v3.

---

### Task 1: Write the robot-training design direction

**Files:**
- Create: `docs/design/robot-training-lakehouse.md`

**Step 1:** Define the falsifiable architecture problem: two format adapters can currently produce incompatible dataset identity and authority semantics while both satisfy the existing FILE rules.

**Step 2:** Define the canonical terms `Dataset`, `Episode`, `Artifact`, `Recording`, `Layer`, `DatasetRevision`, `TrainingView`, and `Materialization`.

**Step 3:** Record the authority rules: Lake owns membership, revisions, access, retention, and provenance; Rerun is an adapter and never an independent source of truth.

**Step 4:** Record the logical/physical split, the direct-object data path, the two-level query model, and the phased delivery sequence.

**Step 5:** Search the new document for all required terms:

Run: `rg -n 'Dataset|Episode|Artifact|Recording|Layer|DatasetRevision|TrainingView|Materialization|Rerun|MCAP|LeRobot' docs/design/robot-training-lakehouse.md`

Expected: every canonical term and all three initial format families have at least one defining occurrence.

### Task 2: Connect the north star and architecture

**Files:**
- Modify: `goal.md`
- Modify: `docs/architecture.md`

**Step 1:** Add a compact product outcome to `goal.md`: ingest, inspect, select, freeze, train, and write immutable derived layers without turning Lake into a training orchestrator.

**Step 2:** Add an architecture summary and Mermaid flow to `docs/architecture.md` that points to the design document.

**Step 3:** State explicitly that logical Episode identity does not equal an object key or RRD file, and that Query/Metasrv do not proxy recording bytes.

**Step 4:** Confirm the existing crate map and storage-engine interfaces remain unchanged.

Run: `jj diff --git -- goal.md docs/architecture.md`

Expected: documentation-only changes that preserve every existing architecture invariant.

### Task 3: Update progressive-disclosure routing

**Files:**
- Modify: `docs/AGENT.md`

**Step 1:** Add `docs/design/robot-training-lakehouse.md` to the documentation catalog with a one-line description.

**Step 2:** Confirm no unrelated catalog entries changed.

Run: `jj diff --git -- docs/AGENT.md`

Expected: exactly one new routing entry.

### Task 4: Verify and publish issue #244

**Files:**
- Verify: `goal.md`
- Verify: `docs/AGENT.md`
- Verify: `docs/architecture.md`
- Verify: `docs/design/robot-training-lakehouse.md`
- Verify: `docs/plans/2026-07-19-robot-training-lakehouse-direction.md`

**Step 1:** Run `mise run hooks`; expect exit 0.

**Step 2:** Run `mise run site-check`; expect TypeScript, Vitest, and production build exit 0.

**Step 3:** Run `jj diff --summary`; expect only paths allowed by issue #244.

**Step 4:** Commit locally with `docs(architecture): define robot-training lakehouse direction (#244)` and body `Closes #244`.

**Step 5:** Run the independent verifier and reviewer workflows. On PASS and APPROVE, create bookmark `issue-244-robot-training-direction`, run `mise run ship`, open and squash-merge the PR, then forget and delete the workspace and bookmark.
