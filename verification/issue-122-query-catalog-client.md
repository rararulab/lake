# Issue #122 verification

Candidate base: `e7c5a452116530cd79606eed15bb0757113c2ea4`

## Delivered contract

- `LakeCatalog` depends on a read-only `CatalogSource` with only point resolve
  and conditional coherent directory operations. The local adapter contains
  all direct registry reads; served Query builds an authenticated remote source
  before listener bind and has no catalog fallback.
- Metasrv's version-1 `catalog_snapshot` returns `not_modified` for the caller's
  known generation or a canonical full snapshot fenced by matching generation
  reads. Races retry at most three times.
- Authority scans pages of 64 entries and accounts each registration before
  retaining it. Generation tokens, entries, individual schema IPC, and
  serialized response bytes are independently bounded; a process admits one
  full snapshot until its response is dropped. Remote snapshots fail closed
  without the monotonic authority marker. Full snapshots require QueryService,
  MetadataPeer, or Admin; user principals are denied.
- Existing startup fail-closed, runtime last-good, refresh coalescing,
  registration TTL/fencing, provider cache, immutable statement snapshots, and
  append read-your-write behavior are preserved.
- This removes catalog registry access from Query's catalog path. Lance
  physical manifest KV remains a separate storage-engine concern and no
  credential-separation claim is made.

## Red/green evidence

- The first local-source selector failed to compile because
  `CatalogSource`, its conditional DTOs, and `LocalCatalogSource` did not exist.
- The first Metasrv selector failed to compile because bounded snapshot limits
  and generation-fenced snapshot construction did not exist.
- A controlled metastore returns old entries then mutates the generation; the
  action retries and publishes both tables, then returns `not_modified` for the
  resulting token. Entry, schema, and response-byte limits reject during
  bounded construction; a legacy non-authoritative store fails closed, and a
  held response rejects a concurrent full snapshot.
- A real bearer-secured Metasrv listener produces listings, Arrow schemas, and
  immutable registrations identical to the local source.
- RPC instrumentation observes one directory warm plus one point resolution;
  sixteen further warm resolves/refresh checks add zero requests.
- After the listener stops, sixteen stale callers return the same last-good Arc
  and coalesce to one failed RPC. After a simulated append acknowledgement,
  local invalidation performs one point resolve and observes version 8 without
  a directory refresh.
- Served Query construction with a directly available local registry but an
  unreachable metadata endpoint fails with a catalog-authority error, proving
  there is no fallback before bind.

## Verification

- `mise run doctor` — PASS in the #122 jj workspace.
- `mise run spec-lint specs/issue-122-query-catalog-client.spec.md` — PASS,
  quality 100%.
- `cargo test -p lake-catalog` — PASS, 23/23.
- `cargo test -p lake-metasrv remote_catalog_snapshot_is_generation_coherent_and_bounded`
  — PASS, 1/1 (69 filtered).
- `cargo test -p lake-query remote_catalog_` — PASS, 4/4 (75 filtered).
- `cargo test -p lake-cli query_catalog_wiring_requires_remote_metadata_source`
  — PASS, 1/1 (31 filtered).
- Affected `cargo clippy --all-targets -- -D warnings` — PASS.
- `mise run spec-lifecycle specs/issue-122-query-catalog-client.spec.md` —
  PASS, all six selectors executed at least one test.
- Independent correctness/security re-review — APPROVE; both prior P1 findings
  (incremental bounded construction/admission and non-authoritative fail-closed)
  are closed with no new P0/P1.
- `mise run gate` — PASS in 132.03s: workspace tests, selftest E2E, three
  ignored upstream ADBC interoperability tests, hooks, and site checks all pass.
