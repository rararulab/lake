spec: task
name: "async-result-manifest-memory"
inherits: project
tags: [query, async, result, manifest, memory, bounds]
---

## Intent

Keep asynchronous-result manifest loading within a compact, structural memory
bound. A completed async result currently validates and preallocates its
manifest using the 64 MiB Arrow result-part ceiling. A manifest contains only
the bounded query id, one bounded IPC schema, summaries, and at most 4,096
bounded `DataLocation` values; it is not an Arrow result part.

The existing URI length check alone cannot prove a compact JSON manifest:
4,096 U+0000 bytes pass the current `uri.len() <= 4,096` check, but serde JSON
serializes each byte as the six-byte escape `\\u0000`. With 4,096 parts, those
URIs alone occupy 4,096 * 4,096 * 6 = 100,663,296 bytes (96 MiB), before the
schema or JSON structure. Therefore a contract that accepts every currently
valid URI and also promises a sub-64 MiB manifest is contradictory.

Reproducer: create a completed async-query state record whose immutable
manifest `DataLocation` declares the current 64 MiB part ceiling, then redeem
its result through `load_manifest`. Before this change the coordinator accepts
the declaration, allocates a `Vec` at that capacity, and begins the read. A
second reproducer constructs a 4,096-part manifest with 4,096 U+0000 bytes in
each part URI. It passes the current URI validation, but JSON serialization
exceeds the Arrow result-part ceiling. Under concurrent poll/DoGet requests,
the first reproducer causes avoidable allocations outside DataFusion's
execution accounting; the second makes the advertised structural bound false.

## Decisions

- Every `DataLocation` used as an async result part or completed manifest is
  valid only when its URI is non-empty JSON-safe ASCII: every byte is in
  `0x21..=0x7e` except double quote (`0x22`) and backslash (`0x5c`). Lake's
  managed local (`file://`, URL-encoded) and S3 (`s3://`) result stores must
  emit that language. This rejects controls, quote, and backslash before a
  URI can expand during JSON serialization, while preserving durable object
  identity, key layout, and the JSON protocol.
- Define `MAX_RESULT_MANIFEST_BYTES` as the fixed 32 MiB ceiling. The current
  immutable JSON layout has a conservative maximum of 21,684,406 bytes:
  4,096 part objects * 4,269 bytes plus separators/brackets = 17,489,921;
  a 1 MiB `Vec<u8>` schema as three decimal digits plus separators =
  4,194,305; and the fixed query-id, scalar, field-name, comma, bracket, and
  quote envelope is at most 180 bytes. The part-object term includes a
  4,096-byte JSON-safe URI, the fixed 35-byte Arrow content type, a 20-digit
  `u64`, and the 64-byte lowercase digest. Thus the derived bound is below
  32 MiB (33,554,432 bytes) and strictly below `MAX_RESULT_PART_BYTES`.
- Make this URI grammar and serialized-size budget part of
  `AsyncResultManifest` semantic validation before worker JSON allocation or
  publication. The worker must reject a structurally invalid manifest before
  serializing it, and must check the emitted JSON length against the manifest
  ceiling before storing it.
- Use the manifest ceiling when the coordinator validates, reserves, and
  reads the immutable manifest object. Reject its declared size before opening
  the object reader or allocating the JSON buffer.
- Reject a location whose declared size exceeds the manifest ceiling before
  opening or allocating for the object. Continue requiring exact byte count,
  verified object identity, JSON decoding, and semantic manifest validation.
- Keep manifests as the existing immutable JSON object and retain result-part
  format, limits, tickets, state schema, and object layout.

## Boundaries

### Allowed Changes
crates/lake-query/src/async_query.rs
crates/lake-objects/src/lib.rs
crates/lake-objects/src/local.rs
crates/lake-objects/src/s3.rs
specs/issue-130-async-result-manifest-memory.spec.md
verification/issue-130-async-result-manifest-memory.md

### Forbidden
changing async-query state record ticket or object-key formats
changing Arrow IPC result-part format or MAX_RESULT_PART_BYTES semantics
changing ManagedObjectStore interfaces or object-key layouts
streaming or paginating manifest format
weakening verified-object exact-size digest or semantic validation
introducing unbounded manifest reads allocations or buffers
changing tenant admission quotas or DataFusion execution memory accounting

## Completion Criteria

Scenario: A part-sized manifest declaration is rejected before object I/O
  Test:
    Package: lake-query
    Filter: async_result_manifest_rejects_part_sized_location_before_read
  Given a completed async result whose manifest `DataLocation` declares the
  Arrow result-part byte ceiling
  When the coordinator loads that manifest
  Then it returns the existing invalid-manifest failure before opening the
  object reader or reserving a part-sized buffer

Scenario: Escaped URI bytes are rejected before manifest serialization
  Test:
    Package: lake-query
    Filter: async_result_manifest_rejects_json_escaped_uri_before_serialization
  Given a result manifest whose part URI consists of 4,096 U+0000 bytes and
  would expand sixfold during JSON serialization
  When the worker validates the manifest before serialization
  Then it rejects the URI and neither serializes nor publishes that manifest

Scenario: The maximum JSON-safe manifest structure fits the fixed ceiling
  Test:
    Package: lake-query
    Filter: async_result_manifest_maximum_json_safe_structure_fits_ceiling
  Given a manifest at the maximum query-id, schema, part count, JSON-safe URI,
  digest, and summary field bounds
  When its serialized JSON size is checked before publication
  Then it is semantically valid, the serialized result is at most
  21,684,406 bytes, and that result is below the 32 MiB manifest ceiling and
  the Arrow result-part ceiling

Scenario: The existing async-query lifecycle remains valid
  Test:
    Package: lake-query
    Filter: async_result_manifest_publishes_only_after_bounded_parts
  Given a successful bounded async SQL result
  When its worker publishes the immutable manifest
  Then the existing result lifecycle publishes the manifest only after every
  bounded part and it remains readable

## Out of Scope

- Streaming or changing the manifest JSON layout.
- Per-tenant byte accounting, cluster-global memory limits, or DataFusion
  memory-pool changes.
- Result endpoint URLs, retention/GC, storage backend, or multipart changes.
