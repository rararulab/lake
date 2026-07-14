# Credentialless managed read capabilities Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Let an authenticated SDK without AWS credentials request a bounded
server-issued HTTPS GET capability for one tenant-scoped managed S3 object.

**Architecture:** A Query-owned, optional issuer receives a principal-scoped
`DataLocation` and expiry, then delegates to the AWS SDK's existing GET
presigner. Query returns a bounded opaque wire response through one Flight
action. The SDK decodes that response without stage discovery or a direct
object-store client. The CLI installs an issuer only for its S3 stage.

**Tech Stack:** Rust 2024, Arrow Flight actions, AWS SDK S3 presigning,
`lake-objects` capability types, `snafu`, Tokio tests.

---

### Task 1: Define bounded capability action wire types

**Files:**
- Modify: `crates/lake-objects/src/lib.rs`
- Test: `crates/lake-objects/src/lib.rs`

1. Write a failing encode/decode test for a single `DataLocation`, 1s..=1h
   expiry, response headers, and a redacted debug representation.
2. Run the exact `lake-objects` test and observe failure because wire types do
   not exist.
3. Add request/response wire types that have explicit byte bounds and reuse
   existing `PresignedRead` validation without serializing credentials.
4. Re-run the test, then commit the wire-only change.

### Task 2: Enforce Query authorization and S3 issuer scope

**Files:**
- Modify: `crates/lake-query/src/lib.rs`
- Modify: `crates/lake-query/src/flight.rs`
- Test: `crates/lake-query/src/flight.rs`

1. Add a failing Flight action test using a recording issuer; it must observe
   the authenticated tenant's prefix and must not expose the URL via Debug.
2. Add a failing denial test covering absent issuer, malformed request, invalid
   expiry, and foreign/escaping locations; assert no issuer invocation.
3. Introduce the injected issuer trait/configuration and the one action handler.
   Authenticate before decoding/signing, scope to `tenants/<tenant-id>`, and
   map failures to safe Flight status messages.
4. Re-run both focused tests and commit the Query action change.

### Task 3: Construct the issuer only on the S3 Query server

**Files:**
- Modify: `crates/lake-cli/src/commands/mod.rs`
- Modify: `crates/lake-cli/src/commands/serve.rs`
- Test: `crates/lake-cli/src/commands/serve.rs`

1. Write a failing context/configuration test proving local mode has no issuer
   and S3 mode has an S3-backed issuer.
2. Build the S3 object-store signer from the existing Query process AWS
   configuration and stage descriptor; retain no bucket access outside its
   configured managed root.
3. Re-run the focused CLI test and commit the wiring change.

### Task 4: Provide the no-cloud-credentials SDK API

**Files:**
- Modify: `crates/lake-sdk/src/lib.rs`
- Test: `crates/lake-sdk/src/lib.rs`
- Modify: `README.md`
- Modify: `docs/architecture.md`

1. Add a failing SDK test backed by a recording Flight action service. Assert
   it sends one bounded action and does not run stage discovery or create a
   managed object store.
2. Add the explicit remote-capability method, decode the opaque response, and
   preserve current `presign_read` local-IAM behavior unchanged.
3. Document capability lifetime, required headers, redaction, tenant scope,
   and the fact that object bytes continue to bypass Lake servers.
4. Re-run the focused SDK test and commit the client/docs change.

### Task 5: Verify the complete contract

**Files:**
- Create: `verification/issue-140-credentialless-read-capabilities.md`

1. Run every acceptance selector and record its summary.
2. Run `mise run spec-lifecycle specs/issue-140-credentialless-read-capabilities.spec.md`.
3. Run `mise run gate` and record the outcome.
4. Commit verification evidence with the final implementation commit if it does
   not alter behavior.
