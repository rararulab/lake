# Issue #138 verification: bounded control-plane enumeration

Candidate base: `990146066bbcca06640489cd995569887ec7636c`

## Delivered contract

- Metasrv exposes bounded `list_tables_page` and User-grant-only
  `list_namespaces_page` actions. Page request inputs, serialization output,
  and in-flight enumeration responses have independent finite limits.
- Global namespace enumeration has no exact bounded implementation without a
  durable namespace index, so Admin, QueryService, and MetadataPeer requests
  fail closed instead of returning a partial or duplicate catalogue.
- Legacy `list_tables` emits a complete result only when one bounded backend
  page proves completion. Any backend continuation is `resource_exhausted`.
  This is intentional for Dynamo v1: `Scan` pagination can continue after
  unrelated rows were removed by `FilterExpression`; a continuation is not
  evidence that matching table names exceeded the response limit.
- `lake client list` follows every opaque continuation and writes a decoded
  page before it asks for the next one. It does not buffer a whole catalogue.

## Observable outcome evidence

- `client_list_follows_control_enumeration_pages` supplies two actual Flight
  action results: `["alpha"]` with `"cursor-a"`, then `["beta"]` with no
  continuation. Its recording control client requires `alpha` to have been
  written before the second request, and asserts the exact action bodies:
  `list_tables_page` with a null continuation followed by the same action with
  `"cursor-a"`.
- `legacy_table_enumeration_fails_closed_on_dynamo_v1_filtered_scan` models a
  Dynamo v1 scan that returns one matching table and a continuation owned by
  an unrelated physical key. The legacy Flight action returns
  `ResourceExhausted`, names `list_tables_page`, and emits no JSON page.
- `legacy_table_enumeration_fails_closed_at_page_boundary` confirms a
  namespace with 257 table registrations similarly returns
  `ResourceExhausted`, so neither backend condition can become a partial
  legacy result.

## Verification

- `mise run fmt-check` — PASS.
- `git diff --check` — PASS.
- `cargo test -p lake-metasrv legacy_table_enumeration_fails_closed -- --nocapture`
  — PASS, 2/2: page-boundary and Dynamo-v1-filtered continuation cases.
- `cargo test -p lake-cli client_list_follows_control_enumeration_pages -- --nocapture`
  — PASS, 1/1.
- `cargo clippy -p lake-metasrv -p lake-cli --all-targets -- -D warnings` —
  PASS.
- `mise run spec-lifecycle specs/issue-138-control-enumeration.spec.md` —
  PASS, all 5 selectors executed; the legacy selector executes both
  fail-closed tests.
- `mise run gate` — PASS: hooks, full workspace/all-target tests, ignored ADBC
  interoperability tests, selftest, and site checks.
