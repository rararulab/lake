spec: task
name: "stream-managed-file-example-reads"
inherits: project
tags: [sdk, objects, file, examples, streaming]
---

## Intent

Make Lake's runnable managed `FILE` example safe to copy into the intended
multi-gigabyte video/model workload. The example currently gets a direct,
integrity-verifying `LakeClient::open` reader but drains it with `read_to_end`
into `Vec<u8>`. A caller following that pattern retains the entire object in
the SDK process, defeating Lake's large-object streaming experience.

## Decisions

- Keep `LakeClient::open`, `DataLocation`, SQL `FILE`, managed-stage discovery,
  and all object-store semantics unchanged.
- The direct-read portion of `managed_file` streams into a local file sink via
  `tokio::io::copy`, then compares the copied count to
  `DataLocation.size_bytes`. Draining to EOF preserves the existing full-read
  integrity guarantee without allocating object-sized memory.
- The root README explicitly describes the runnable example as streaming its
  direct read to a sink, while retaining its existing `tokio::io::copy`
  quick-start snippet.
- Add a named SDK source-contract test that rejects `read_to_end` in this
  runnable example and requires the file-sink/copy pattern. The example command
  remains the end-to-end runtime proof.

## Boundaries

### Allowed Changes
crates/lake-sdk/examples/managed_file.rs
crates/lake-sdk/src/lib.rs
crates/lake-sdk/AGENT.md
README.md
specs/issue-135-stream-managed-file-example-reads.spec.md

### Forbidden
crates/lake-objects/**
crates/lake-query/**
crates/lake-meta/**
crates/lake-metasrv/**
crates/lake-engine/**
crates/lake-engine-lance/**
Cargo.toml
Cargo.lock
docs/architecture.md
buffering a complete direct-read object in the example
changing object integrity, range-read, upload, stage-discovery, or SQL semantics
routing object bytes through Query or Metasrv

## Completion Criteria

Scenario: Runnable managed FILE example streams direct reads to a sink
  Test:
    Package: lake-sdk
    Filter: managed_file_example_streams_direct_reads_to_sink
  Given the source of the public managed FILE example
  When its direct DataLocation read is inspected
  Then it opens through LakeClient, streams with tokio::io::copy into a file
  sink, checks the copied count against the immutable DataLocation size, and
  contains no read_to_end direct-read path

## Out of Scope

- Changing the SDK public API or object integrity implementation.
- Adding media decoding, chunk/Merkle integrity, or range verification.
- Altering S3/local storage behavior, FILE SQL grammar, or metadata/query
  services.
