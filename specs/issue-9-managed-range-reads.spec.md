spec: task
name: "managed-range-reads"
inherits: project
tags: [objects, sdk, s3, video]
---

## Intent

Let Rust SDK users consume multi-gigabyte SQL `FILE` values without reading
from byte zero. Video decoders, frame extractors, and model loaders must be
able to request one exact byte interval through the same local/S3 managed-stage
abstraction while query and metasrv continue carrying metadata only.

## Decisions

- Public ranges are half-open `start..end` byte offsets, matching Rust slice
  semantics. Empty, reversed, and out-of-bounds ranges fail before storage I/O.
- `ManagedObjectStore` gains an object-safe async range-open operation;
  `LakeClient` exposes it without changing sequential `open`.
- Local storage seeks to `start` and wraps the file in a bounded reader. S3
  sends exactly one inclusive HTTP `Range: bytes=start-(end-1)` request.
- Range ownership checks reuse the existing canonical local root and exact S3
  bucket/path-prefix validation. Public SQL never accepts a range URI.
- The reader remains streaming and bounded; the SDK does not allocate the
  requested interval or cache it.

## Boundaries

### Allowed Changes
- `crates/lake-objects/**`
- `crates/lake-sdk/**`
- `README.md`
- `docs/architecture.md`
- `docs/design/managed-objects.md`
- `docs/plans/**`
- `specs/**`
- `verification/**`
- `scripts/test-integration.ts`
- `**/.github/actionlint.yaml`
- `**/.github/workflows/ci.yml`
- `**/.github/workflows/pages.yml`
- `**/AGENT.md`
- `**/mise.toml`

The root CI/config paths are shared-checkout history owned by merged issue 6.
This workspace modifies only the crate `AGENT.md` cards listed above, but the
repository-wide worktree verifier also reports issue 6's pre-squash branch
commits as changes.

### Forbidden
- Sending object bytes through query or metasrv
- Buffering a complete range in the SDK
- Accepting arbitrary local paths or S3 locations
- Codec-aware frame indexing, transcoding, or multi-range requests
- Changing SQL, registry, or table commit protocols

## Completion Criteria

Scenario: local managed FILE returns exactly one byte interval
  Test:
    Package: lake-objects
    Filter: local_range_reader_returns_exact_interval
  Given a committed local managed object and a half-open byte range
  When the managed stage opens that range
  Then the reader yields only those bytes and then EOF

Scenario: invalid managed FILE ranges fail before storage I/O
  Test:
    Package: lake-objects
    Filter: range_reader_rejects_empty_reversed_and_out_of_bounds_ranges
  Given empty, reversed, or oversized byte intervals
  When either managed stage is asked to open the range
  Then it returns a typed invalid-range error without opening the object

Scenario: S3 managed FILE uses a bounded Range GET
  Test:
    Package: lake-objects
    Filter: s3_range_read_localstack_is_wired
  External verification: `mise run test-integration` runs
  `s3_range_read_returns_requested_bytes_localstack` against LocalStack.
  Given a multipart object in the managed S3 prefix
  When its middle byte interval is opened
  Then the direct reader yields exactly that interval

Scenario: SDK exposes range reads for queried DataLocations
  Test:
    Package: lake-sdk
    Filter: sdk_opens_range_from_queried_datalocation
  Given a FILE inserted and queried through LakeClient
  When the caller opens a half-open range on its DataLocation
  Then the configured managed stage streams exactly those bytes directly

## Out of Scope

- Browser presigned range URLs and tenant authorization
- Multi-range/multipart HTTP responses
- SDK block cache or read-ahead
- Resumable uploads
- Container or codec frame indexes
