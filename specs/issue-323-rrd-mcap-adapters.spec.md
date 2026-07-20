spec: task
name: "issue-323-rrd-mcap-adapters"
inherits: project
tags: [robotics, adapters, rrd, mcap, episode, bounded-io]
---

## Intent

Add the first format boundary for Lake's robot-training data: one independent
`lake-adapters` crate with RRD and MCAP implementations that extract only
Episode-level metadata and return Lake's existing `EpisodeManifestV1` contract.
The adapters must use format-owned indexes when available and retain a finite,
observable fallback for valid recordings whose optional index is absent.

Current reproducer:

1. Put one valid indexed RRD, one valid footerless RRD, one valid summarized
   MCAP, and one valid summary-less MCAP behind a reader that records every byte
   range and rejects reads beyond caller-provided byte and request budgets.
2. Ask Lake to derive a format-neutral Episode manifest from each Artifact.
3. Today there is no adapter seam or implementation. A caller must either
   duplicate format parsing, download an arbitrarily large recording, reject
   valid files whose optional footer/summary is absent, or leak Rerun/MCAP types
   into Lake's core model.

Required behavior: both adapters emit a validated `lake_common::EpisodeManifestV1`.
An RRD footer or MCAP summary drives bounded random reads; an absent optional
index triggers a bounded linear scan; corrupt present index data fails closed.
Every byte and read request is charged before I/O, so an exact budget passes and
the same fixture with a budget one unit smaller fails without over-reading.

This advances the `goal.md` ingest -> inspect -> select loop and its explicit
rule that RRD and MCAP are Adapters rather than the core data model. It preserves
Lake as the Dataset authority and does not create a Rerun Hub clone, a Viewer,
an ingestion/commit path, or per-sample traffic through Query or Metasrv.

## Decisions

- Add a domain crate named `lake-adapters`. Its public seam is format-neutral,
  async, `Send + Sync`, and accepts Lake-owned Episode/Artifact identity plus a
  caller-owned random-access source and explicit finite extraction limits.
- The extraction limits bound total bytes returned by the source and total read
  requests. The implementation accounts a request before issuing it, uses
  checked offset/length arithmetic, never silently substitutes an unbounded
  default, and returns a typed budget error without performing the forbidden
  read.
- The output type is exactly `lake_common::EpisodeManifestV1`, constructed
  through its validated public API. Public adapter inputs, outputs, and errors
  expose no `re_log_encoding`, Rerun, MCAP, ROS, object-store, Query, Metasrv,
  storage-engine, credential, or signed-URL type.
- Caller context supplies Lake-owned logical identities and values that the
  recording cannot authoritatively infer. Adapters extract only available
  Episode-level recording, timeline, stream/topic, schema/codec, producer, time
  range, and sample-count summaries. They do not invent task/success semantics,
  retain decoded messages, or put per-message/per-chunk metadata in the
  manifest.
- Reuse `re_log_encoding` 0.34.x for RRD decoding. Use its public
  `StreamHeader`, `StreamFooter`, and `Decodable` contracts together with
  Rerun's protobuf/application conversion to drive random ranges from the
  caller-owned source; use `DecoderApp` for the bounded linear fallback. Do not
  hand-write the RRD byte layout or independently parse frames or footer bytes.
- Reuse `mcap` 0.25.x for MCAP decoding. Use its summary/indexed-reader APIs for
  indexed reads and its upstream linear reader for the bounded fallback; do not
  parse MCAP records, summaries, or message indexes independently.
- A genuinely absent RRD footer or MCAP summary selects the linear path. A
  present but malformed, incompatible, out-of-range, or internally inconsistent
  footer/summary returns a typed format error and must not retry as a linear
  scan.
- Indexed extraction reads only footer/summary/index data and the source ranges
  required for Episode-level summaries. Tests use instrumented sources to prove
  that unrelated payload sentinels are not fetched.
- Linear extraction may inspect upstream-decoded records only until completion
  or budget exhaustion. It keeps bounded aggregate state proportional to the
  distinct Episode-level identities retained in the manifest and never returns
  partial success after a format or budget error.
- The crate follows the inherited `snafu`, `bon::Builder`, async API,
  documentation, and Apache-2.0 source-header constraints. It includes its own
  short `AGENT.md` catalog card.
- Update architecture/design status only enough to mark the adapter seam and
  these two bounded metadata extractors implemented. RRD/MCAP remain external
  format authorities for local decoding, never catalog authorities.

## Boundaries

### Allowed Changes
Cargo.toml
Cargo.lock
crates/lake-adapters/**
docs/architecture.md
docs/design/robot-training-lakehouse.md
docs/plans/2026-07-20-rrd-mcap-adapters.md
specs/issue-323-rrd-mcap-adapters.spec.md
verification/report.md

### Forbidden
crates/lake-common/**
crates/lake-engine/**
crates/lake-engine-lance/**
crates/lake-meta/**
crates/lake-catalog/**
crates/lake-query/**
crates/lake-metasrv/**
crates/lake-sdk/**
crates/lake-cli/**
crates/lake-flight/**
crates/lake-objects/**
crates/lake-iceberg/**
goal.md
site/**
.github/**
changing EpisodeManifestV1, Episode/ArtifactRef, DataLocation, Arrow schemas,
  table versions, commit protocol, GC, authorization, or object upload
exposing Rerun, re_log_encoding, MCAP, ROS, storage-engine, Query, or Metasrv
  types through the lake-adapters public seam
hand-writing RRD frames/footer parsing or MCAP records/summary/index parsing
using an unbounded read, scan, allocation, retry, or collection-growth path
implementing Episode upload/append/commit, Viewer/Hub integration,
  DatasetRevision, TrainingView, materialization, or Python APIs

## Completion Criteria

Rule: adapter-seam-v1 — format implementations emit Lake-owned metadata

Scenario: both formats produce validated EpisodeManifestV1 values
  Test:
    Package: lake-adapters
    Filter: format_adapters_emit_valid_episode_manifest_v1
  Level: integration
  Targets: crates/lake-adapters/src/lib.rs, crates/lake-adapters/tests/format_adapters.rs
  Given equivalent RRD and MCAP fixtures plus caller-supplied Lake Episode,
  Recording, Layer, and Artifact identities
  When each format adapter extracts Episode-level metadata
  Then each result is a validated lake_common::EpisodeManifestV1 with stable
  Recording, Timeline, Stream, and Artifact bindings, while the public seam and
  serialized manifest contain no upstream format or infrastructure types

Rule: rrd-bounded-reads — footer-first random access with finite fallback

Scenario: an RRD footer drives bounded random metadata reads
  Test:
    Package: lake-adapters
    Filter: rrd_footer_metadata_stays_within_read_budget
  Level: integration
  Targets: crates/lake-adapters/src/rrd.rs, crates/lake-adapters/tests/rrd.rs
  Given a valid RRD with a footer/manifest, indexed Episode metadata, and an
  unrelated payload sentinel behind an instrumented random-access source
  When the RRD adapter extracts metadata with exactly the observed byte and
  request budgets
  Then extraction succeeds through re_log_encoding, does not fetch the
  unrelated payload, and reducing either exact budget by one returns the typed
  budget error before the source observes an over-budget read

Scenario: a footerless RRD uses only a bounded linear fallback
  Test:
    Package: lake-adapters
    Filter: rrd_missing_footer_uses_bounded_linear_fallback
  Level: integration
  Targets: crates/lake-adapters/src/rrd.rs, crates/lake-adapters/tests/rrd.rs
  Given a valid RRD whose optional footer is absent and an instrumented source
  When extraction runs once with an exact sufficient budget and once with the
  byte or request budget reduced by one
  Then the upstream linear decoder produces the same Episode-level manifest in
  the first run, while the second returns the typed budget error without
  exceeding the configured limit or returning a partial manifest

Rule: mcap-bounded-reads — summary-first random access with finite fallback

Scenario: an MCAP summary drives bounded random metadata reads
  Test:
    Package: lake-adapters
    Filter: mcap_summary_metadata_stays_within_read_budget
  Level: integration
  Targets: crates/lake-adapters/src/mcap.rs, crates/lake-adapters/tests/mcap.rs
  Given a valid MCAP with Summary, channel/schema/chunk indexes, and an unrelated
  message payload sentinel behind an instrumented random-access source
  When the MCAP adapter extracts metadata with exactly the observed byte and
  request budgets
  Then extraction succeeds through mcap's summary/indexed APIs, does not fetch
  the unrelated payload, and reducing either exact budget by one returns the
  typed budget error before the source observes an over-budget read

Scenario: a summary-less MCAP uses only a bounded linear fallback
  Test:
    Package: lake-adapters
    Filter: mcap_missing_summary_uses_bounded_linear_fallback
  Level: integration
  Targets: crates/lake-adapters/src/mcap.rs, crates/lake-adapters/tests/mcap.rs
  Given a valid MCAP whose optional Summary is absent and an instrumented source
  When extraction runs once with an exact sufficient budget and once with the
  byte or request budget reduced by one
  Then mcap's upstream linear reader produces the same Episode-level manifest
  in the first run, while the second returns the typed budget error without
  exceeding the configured limit or returning a partial manifest

Rule: indexed-corruption — malformed indexes fail closed

Scenario: corrupt present indexes never downgrade to linear scans
  Test:
    Package: lake-adapters
    Filter: corrupt_format_index_fails_closed_without_fallback
  Level: integration
  Targets: crates/lake-adapters/src/rrd.rs, crates/lake-adapters/src/mcap.rs
  Given an RRD with a present malformed or out-of-range footer and an MCAP with
  a present malformed or out-of-range Summary/index
  When each adapter attempts Episode metadata extraction
  Then each returns a typed format error after bounded reads, performs no
  linear fallback, and returns no EpisodeManifestV1

## Out of Scope

- Uploading, registering, appending, committing, or querying Episodes and
  manifest Artifacts.
- Changing the EpisodeManifest v1 wire or the Episode/ArtifactRef Arrow table
  contract.
- Per-message decoding output, image/video/point-cloud decoding, temporal
  sampling, or full topic/entity query APIs.
- Rerun Viewer or Hub behavior, MCAP/RRD writers, derived RRD Materializations,
  format conversion, or LeRobot adapters.
- DatasetRevision retention, TrainingView selection/splits, Layers execution,
  Python/PyTorch readers, authorization, signed URLs, or direct-read
  capabilities.
