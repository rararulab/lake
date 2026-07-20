# RRD and MCAP Episode Metadata Adapters Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a bounded, format-neutral inspection seam with real RRD and MCAP implementations that produce canonical `lake_common::EpisodeManifestV1` values.

**Architecture:** Introduce a leaf `lake-adapters` crate. Callers provide an async random-access byte source, Lake-owned Episode/Recording/Artifact identities, and an explicit read budget. Each format implementation drives the upstream Rerun or MCAP decoder, prefers its footer/summary index, falls back only to a bounded linear scan, and maps the result into the same format-neutral manifest contract. Rerun and MCAP types never cross the crate's public output boundary, and this slice performs no upload or catalog commit.

**Tech Stack:** Rust 2024, `async-trait`, `bytes`, `snafu`, `sha2`, `re_log_encoding` 0.34.1, `mcap` 0.25.0, `lake-common`, Tokio tests, Task-Contract Lane 1.

---

### Task 1: Lock the Lane 1 contract and crate boundary

**Files:**

- Create: `specs/issue-323-rrd-mcap-adapters.spec.md`
- Create: `crates/lake-adapters/AGENT.md`
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Create: `crates/lake-adapters/Cargo.toml`
- Create: `crates/lake-adapters/src/lib.rs`

**Step 1:** Finish and lint the issue spec. Its real selectors must name the indexed RRD, fallback RRD, indexed MCAP, fallback MCAP, and shared manifest tests.

Run: `mise run spec-lint specs/issue-323-rrd-mcap-adapters.spec.md`

Expected: PASS.

**Step 2:** Add `lake-adapters` to workspace members and dependencies. Pin the upstream format crates in `[workspace.dependencies]`; enable only the features needed to decode production files and construct test fixtures.

**Step 3:** Add the leaf crate and its 10–20 line `AGENT.md`. The crate may depend on `lake-common`; no existing Lake tier may depend on it in this slice.

**Step 4:** Add a minimal `lib.rs` module skeleton and run the focused build.

Run: `cargo check -p lake-adapters`

Expected: PASS with no parser implementation yet.

**Step 5:** Commit the contract/scaffold unit.

Run: `jj commit -m "feat(adapters): scaffold bounded format seam (#323)"`

### Task 2: Specify the shared source, budget, and output contract

**Files:**

- Create: `crates/lake-adapters/src/source.rs`
- Create: `crates/lake-adapters/src/model.rs`
- Modify: `crates/lake-adapters/src/lib.rs`
- Create: `crates/lake-adapters/tests/budget.rs`

**Step 1:** Write failing tests for an in-memory counting source. Cover exact byte/range budgets, a one-byte-short budget, a one-range-short budget, short reads, and invalid ranges. Assert the underlying source is not called for a request that would exceed budget.

Run: `cargo test -p lake-adapters --test budget`

Expected: FAIL because the source and budget types do not exist.

**Step 2:** Define the public format-neutral types:

- `RandomAccessSource` with async `len` and exact `read_range` operations;
- `ReadBudget` plus private accounting used to charge bytes and requests before I/O;
- `EpisodeInspectionContext` carrying Lake-owned Episode, Recording, Layer, and Artifact identities plus an optional opaque selector;
- `RecordingAdapter`, whose public async result is exactly `EpisodeManifestV1`;
- a structured `AdapterError`, including `BudgetExceeded`, `FallbackScanTooLarge`, `ShortRead`, invalid source, ambiguity, and manifest mapping failures;
- two concrete implementations behind that same seam.

**Step 3:** Implement a private budget-enforcing source wrapper. Check range count and byte count before delegating, use checked arithmetic, and require the returned buffer length to match the requested range exactly.

**Step 4:** Re-run the focused test.

Run: `cargo test -p lake-adapters --test budget`

Expected: PASS.

**Step 5:** Commit the shared seam.

Run: `jj commit -m "feat(adapters): bound random-access inspection (#323)"`

### Task 3: Implement the indexed RRD path with upstream framing

**Files:**

- Create: `crates/lake-adapters/src/rrd.rs`
- Create: `crates/lake-adapters/tests/rrd_adapter.rs`
- Modify: `crates/lake-adapters/src/lib.rs`

**Step 1:** Build a valid modern RRD fixture with the upstream encoder and footer enabled. Write `rrd_adapter_uses_footer_within_read_budget` first. Assert:

- the source is not fully scanned;
- the result has the supplied Lake identities, RRD recording format, producer version, native selector, canonical timelines/streams, one base layer, and one recording Artifact binding;
- replaying with `max_bytes = successful_usage.bytes - 1` fails with `BudgetExceeded` before the source crosses the limit;
- replaying with `max_ranges = successful_usage.ranges - 1` does the same.

Run: `cargo test -p lake-adapters --test rrd_adapter rrd_adapter_uses_footer_within_read_budget -- --exact`

Expected: FAIL because `RrdAdapter` is not implemented.

**Step 2:** Implement the indexed read sequence: exact RRD header, exact terminal `StreamFooter`, then the bounded footer payload range. Decode every frame and the footer payload with public `re_log_encoding`/Rerun protocol types, validate the footer checksum and file spans, and reject footer payloads above `max_record_bytes` before reading them.

**Step 3:** Select exactly one recording store. Ignore blueprint stores, accept the opaque selector when supplied, and return a structured ambiguity/not-found error instead of choosing nondeterministically.

**Step 4:** Convert the selected Rerun manifest into internal neutral recording metadata. Canonicalize timeline names and entity streams, map sequence vs timestamp semantics, and derive per-stream schema fingerprints from sorted component identifiers.

**Step 5:** Build `EpisodeManifestV1` through `EpisodeManifestDraftV1::try_from_draft`; do not serialize/reparse it or duplicate common validation.

**Step 6:** Re-run the focused selector.

Run: `cargo test -p lake-adapters --test rrd_adapter rrd_adapter_uses_footer_within_read_budget -- --exact`

Expected: PASS.

**Step 7:** Commit the indexed RRD path.

Run: `jj commit -m "feat(adapters): inspect indexed RRD metadata (#323)"`

### Task 4: Add the bounded RRD linear fallback

**Files:**

- Modify: `crates/lake-adapters/src/rrd.rs`
- Modify: `crates/lake-adapters/tests/rrd_adapter.rs`

**Step 1:** Generate the same valid RRD fixture with the upstream encoder footer disabled. Write `rrd_adapter_falls_back_to_bounded_linear_scan`. Assert the exact configured fallback size succeeds, one byte less returns `FallbackScanTooLarge`, the source never reads past either budget, and output remains a canonical `EpisodeManifestV1`.

Run: `cargo test -p lake-adapters --test rrd_adapter rrd_adapter_falls_back_to_bounded_linear_scan -- --exact`

Expected: FAIL because no-footer files are not handled.

**Step 2:** Reuse already-read prefix/tail bytes and fetch only missing spans so the fallback consumes no more than the complete file size. Refuse the scan before its first fallback range when `file_size > max_fallback_scan_bytes`.

**Step 3:** Drive `re_log_encoding::DecoderApp` over the bounded bytes. Accumulate only recording identifiers, temporal ranges, entity paths, timelines, and component identifiers; discard payload batches as soon as their neutral summary is updated.

**Step 4:** Use the same canonical mapping kernel as the footer path. Do not add a second RRD-to-manifest implementation.

**Step 5:** Re-run both RRD selectors and the full RRD integration test binary.

Run: `cargo test -p lake-adapters --test rrd_adapter`

Expected: PASS.

**Step 6:** Commit the RRD fallback.

Run: `jj commit -m "feat(adapters): bound legacy RRD fallback (#323)"`

### Task 5: Implement the indexed MCAP summary path

**Files:**

- Create: `crates/lake-adapters/src/mcap.rs`
- Create: `crates/lake-adapters/tests/mcap_adapter.rs`
- Modify: `crates/lake-adapters/src/lib.rs`

**Step 1:** Generate a valid MCAP with a summary, two channels with schemas, time ranges, and an attachment using `mcap::Writer`. Write `mcap_adapter_uses_summary_within_read_budget` with the same exact-success and one-less-byte/range assertions as the RRD indexed test.

Run: `cargo test -p lake-adapters --test mcap_adapter mcap_adapter_uses_summary_within_read_budget -- --exact`

Expected: FAIL because `McapAdapter` is not implemented.

**Step 2:** Validate the start magic/header within budget. Drive `mcap::sans_io::SummaryReader` against the random-access source, translating its seek/read events without loading message payloads. Configure `file_size` and `record_length_limit` from the caller's budget.

**Step 3:** Map summary statistics, channels, schemas, log-time range, message encodings, schema fingerprints, and attachment indexes into neutral metadata with deterministic ordering. Preserve the caller's Lake identities; never use topic names or object paths as Episode identity.

**Step 4:** Use the shared manifest mapping kernel and return the exact `EpisodeManifestV1`; the instrumented source remains the authority for exact read usage.

**Step 5:** Re-run the focused selector.

Run: `cargo test -p lake-adapters --test mcap_adapter mcap_adapter_uses_summary_within_read_budget -- --exact`

Expected: PASS.

**Step 6:** Commit the indexed MCAP path.

Run: `jj commit -m "feat(adapters): inspect indexed MCAP metadata (#323)"`

### Task 6: Add the bounded MCAP linear fallback

**Files:**

- Modify: `crates/lake-adapters/src/mcap.rs`
- Modify: `crates/lake-adapters/tests/mcap_adapter.rs`

**Step 1:** Generate a valid MCAP without a summary and write `mcap_adapter_falls_back_to_bounded_linear_scan`. Assert exact scan cap success, one byte less refusal before the scan, structured record-size failure, canonical streams/timelines, and bounded source usage.

Run: `cargo test -p lake-adapters --test mcap_adapter mcap_adapter_falls_back_to_bounded_linear_scan -- --exact`

Expected: FAIL because summary absence is not handled.

**Step 2:** Assemble the bounded source bytes without re-reading already fetched spans. Drive `mcap::sans_io::LinearReader` with `record_length_limit`, chunk CRC validation, and bounded decompressed chunk sizes. Use upstream `parse_record` for every emitted record.

**Step 3:** Accumulate channel/schema definitions, message counts/time range, header producer, and attachment summaries without retaining message payloads. Feed the same neutral MCAP-to-manifest kernel used by the summary path.

**Step 4:** Re-run both MCAP selectors and the full MCAP integration test binary.

Run: `cargo test -p lake-adapters --test mcap_adapter`

Expected: PASS.

**Step 5:** Commit the MCAP fallback.

Run: `jj commit -m "feat(adapters): bound summaryless MCAP fallback (#323)"`

### Task 7: Prove both implementations share the Lake contract

**Files:**

- Create: `crates/lake-adapters/tests/manifest_contract.rs`
- Modify: `crates/lake-adapters/src/model.rs`
- Modify: `crates/lake-adapters/src/lib.rs`

**Step 1:** Write `adapter_outputs_lake_common_episode_manifest_v1`. Invoke both implementations through `&dyn RecordingAdapter`, assert both outputs expose only neutral types, round-trip each manifest through canonical JSON, and verify identical caller-owned Episode/Recording/Layer/Artifact identities.

Run: `cargo test -p lake-adapters --test manifest_contract adapter_outputs_lake_common_episode_manifest_v1 -- --exact`

Expected: FAIL until both implementations satisfy the trait without leaking format-specific output.

**Step 2:** Consolidate duplicated manifest construction in one private mapping function. Keep format-specific extraction in `rrd.rs` and `mcap.rs` only.

**Step 3:** Add compile-time `Send + Sync` assertions for the source trait object, adapters, and returned future/output where useful.

**Step 4:** Re-run the exact selector, crate tests, and forbidden dependency search.

Run: `cargo test -p lake-adapters`

Expected: PASS.

Run: `rg -n 're_log|re_chunk|re_sorbet|mcap::' crates/lake-common crates/lake-query crates/lake-metasrv`

Expected: zero new production matches attributable to #323.

**Step 5:** Commit the common-contract proof.

Run: `jj commit -m "test(adapters): prove dual-format manifest seam (#323)"`

### Task 8: Verify, review, ship, and merge

**Files:**

- Create: `verification/report.md`
- Modify only if findings require fixes: files allowed by the issue spec

**Step 1:** Run formatting and the exact Lane 1 lifecycle.

Run: `mise run fmt`

Run: `mise run spec-lifecycle specs/issue-323-rrd-mcap-adapters.spec.md`

Expected: base selectors fail by zero-match; product selectors each execute real tests and pass.

**Step 2:** Run the full local gate from a clean checkout.

Run: `mise run gate`

Expected: PASS, including tests, e2e, site, and integration legs.

**Step 3:** Give the immutable product head to the independent verifier. Require fresh base/product hashes, selector transition evidence, hostile budget probes, `pass_to_fail = 0`, and `verification/report.md` bound to those hashes.

**Step 4:** Give the verified product/report commits to the independent reviewer. Resolve every P0/P1/P2 finding and rerun affected checks.

**Step 5:** Run the complete shipping gate and push the tracked bookmark.

Run: `mise run ship`

Expected: PASS and bookmark pushed.

**Step 6:** Create PR #323 with the spec-backed acceptance evidence, merge it after green checks, wait for `main` CI success, then forget the exact jj workspace/bookmark and remove only `.worktrees/issue-323-rrd-mcap-adapters`.
