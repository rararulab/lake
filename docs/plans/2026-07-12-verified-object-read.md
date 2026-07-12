# Streaming Integrity-Verified Object Read Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make `LakeClient::open` validate streamed bytes against the stored `DataLocation` size and SHA-256 before a full read can end successfully.

**Architecture:** `lake-objects` owns an unoverrideable `open_verified(&dyn ManagedObjectStore, &DataLocation)` helper. It validates the expected hash before storage I/O, wraps the backend stream in Tokio's size-capped `Take`, hashes only bytes delivered to the caller, probes one private byte after the declared length, and returns typed terminal integrity errors through `std::io::Error`. The SDK routes its existing full-open API through that helper; range reads remain unchanged.

**Tech Stack:** Rust 2024, Tokio `AsyncRead`/`Take`, SHA-256, Snafu, Arrow Flight SDK tests.

---

### Task 1: Specify full-read integrity behavior

**Files:**
- Modify: `crates/lake-objects/src/lib.rs`

**Step 1: Write the exact-identity test**

Add `verified_reader_accepts_exact_identity_while_streaming` with a small
chunked backend reader and a matching DataLocation. Drain through a small
caller buffer and assert exact ordered bytes plus successful EOF.

**Step 2: Write the fail-closed matrix test**

Add `verified_reader_rejects_invalid_short_long_and_hash_mismatch`. Use a store
that counts `open_reader` calls. Assert malformed expected SHA-256 returns a
typed object error with zero opens. Drain short, long, and same-size corrupt
streams and downcast terminal `InvalidData` sources to the corresponding public
integrity variants. Assert the overlong byte is never delivered.

**Step 3: Verify RED**

Run both selectors separately. Expected: compilation fails because
`open_verified` and `ObjectIntegrityError` do not exist.

### Task 2: Implement the bounded verification reader

**Files:**
- Create: `crates/lake-objects/src/integrity.rs`
- Modify: `crates/lake-objects/src/lib.rs`

**Step 1: Define typed errors and expectation parsing**

Add public cloneable variants for invalid SHA-256, premature EOF, bytes beyond
the declared size, and same-size hash mismatch. Validate exactly 64 ASCII hex
characters and normalize to lowercase before opening the store.

**Step 2: Implement the reader state machine**

Wrap `ObjectReader` in `tokio::io::Take<ObjectReader>`. While bytes remain,
delegate directly into the caller's ReadBuf and hash only newly delivered
bytes. If backend EOF arrives early, return typed InvalidData. At the declared
size, poll the uncapped inner reader into a one-byte private buffer: one byte is
an overlong error; EOF finalizes and compares SHA-256. Successful and failed
terminal states remain stable on later polls. A zero-capacity caller buffer
must not be mistaken for EOF.

**Step 3: Expose the unoverrideable helper**

`open_verified` parses the expectation, then calls `store.open_reader`, then
returns the boxed verification stream. Map malformed identity through a typed
`ObjectError::Integrity` source.

**Step 4: Verify GREEN**

Run both object selectors and the full `lake-objects` unit suite. Expected:
PASS with no LocalStack requirement.

### Task 3: Make the SDK safe by default

**Files:**
- Modify: `crates/lake-sdk/src/lib.rs`

**Step 1: Write the SDK RED test**

Add `sdk_open_verifies_datalocation_identity_without_query` using a lazy
unreachable Query channel and an injected static managed store. Matching bytes
must drain successfully; same-size corrupt bytes must end in the public typed
integrity error.

**Step 2: Verify RED**

Run the selector. Expected: the corrupt stream currently succeeds because
`LakeClient::open` delegates to raw `open_reader`.

**Step 3: Route existing open through verification**

Replace the raw store delegation with `lake_objects::open_verified`. Do not add
a second safer API or change `open_range`.

**Step 4: Verify GREEN**

Run the SDK selector and full `lake-sdk` unit suite. Expected: PASS.

### Task 4: Document and ship

**Files:**
- Modify: `README.md`
- Modify: `docs/architecture.md`
- Modify: `docs/design/managed-objects.md`
- Modify: `crates/lake-objects/AGENT.md`
- Modify: `crates/lake-sdk/AGENT.md`

**Step 1: Document EOF semantics**

State that `open` verifies size/hash only when drained to EOF, uses constant
memory, rejects malformed identity before I/O, and that range/presigned reads
do not prove the full-object hash.

**Step 2: Run gates**

Run nightly fmt, strict clippy for both crates, spec lifecycle, package tests,
and `mise run gate`.

**Step 3: Fix a clean candidate and review it**

Commit with `Closes #83`, then obtain independent reviewer APPROVE and verifier
PASS against the same revision. Address findings and rerun affected evidence.

**Step 4: Publish and merge**

Push the jj bookmark, open a PR closing #83, merge it, and confirm the issue is
closed.
