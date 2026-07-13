# Credentialless direct managed-reader plan

## Goal

Give an authenticated Rust SDK process with no cloud credentials the same
direct streaming `DataLocation` experience as a credentialed client, without
making every application handle bearer URLs and signed headers.

## Decisions

- `open_via_query` asks Query for one short-lived managed GET capability, then
  streams object storage directly with a shared Rustls HTTP client. It verifies
  declared size and SHA-256 at EOF using the existing constant-memory reader.
- `open_range_via_query` validates a non-empty half-open range before signing,
  sends its exact HTTP `Range`, and accepts only a matching `206 Content-Range`
  and `Content-Length`. It deliberately makes no whole-object integrity claim.
- The client never follows redirects, never retains a capability outside the
  request, and maps transport errors without URL or header values. Dropping the
  returned reader drops the HTTP response body instead of draining it.
- Query remains an authorization/signing control plane: it and Metasrv never
  proxy object bytes, and Query-only construction still performs no stage
  discovery, S3 client construction, or cloud credential lookup.

## Verification sequence

1. Add full-object, exact-range, malformed-response, redirect, and corruption
   integration tests using an in-process Query action and HTTP object endpoint.
2. Run the focused SDK tests and the issue spec lifecycle.
3. Run the repository gate, record its result, and publish the stacked PR only
   after the credentialless capability PR is accepted.
