# Issue #59 verification: paged table maintenance

## Candidate

- Base: `bb5083da`
- Implementation head: `a4215211`
- Scope: typed registry paging, bounded Metasrv table maintenance, and startup configuration

## Contract evidence

- `mise run spec-lifecycle specs/issue-59-paged-maintenance.spec.md`: 4/4 scenarios passed.
- All four selectors were absent on the base, present exactly once on the
  candidate, and passed as focused tests.
- Five registrations with page size two produce pages of 2/2/1 with no
  duplicate or missing table.
- Three maintenance candidates over two ticks produce scanned/attempted counts
  of 2 then 1, engine calls of 2 then 3, exactly two registry page scans, zero
  namespace listings, and two point reads per candidate.
- A held table lock allows an old scanned registration to be deleted and
  replaced before maintenance continues; the engine receives only the
  replacement location.
- CLI parsing rejects zero/malformed interval, zero/over-10000 page size, and
  preserves valid 15-second/512-table settings.

## Quality gates

- `cargo test -p lake-meta`: 14 passed; one environment-dependent DynamoDB
  LocalStack test ignored.
- `cargo test -p lake-metasrv`: 58 passed, 1 LocalStack test ignored.
- Two-node integration: 5/5 passed.
- `cargo test -p lake-cli`: 17 passed.
- Strict clippy for all affected crates with all targets/features and
  `-D warnings`: passed.
- Local and independent `mise run gate`: passed.
- Workspace, diff, commit-chain, and allowed-boundary checks: clean.

## Review

- Independent correctness/security review: APPROVE, no blocker or high finding.
- Independent release verifier: PASS.
- Registry page continuation is backend-opaque. DynamoDB empty filtered pages
  retain their last-evaluated key and advance on the next tick; final pages
  publish `None` and wrap on the following tick.
- Cursor state is process-local and published only after a successful scan.
  Every candidate is re-resolved under its table lock before tombstone checks,
  engine maintenance, or registry CAS.
- The production server passes validated `MaintenanceLimits` to the actual
  loop, which uses both configured interval and table page size.
- No durable metadata, DynamoDB schema, or Flight wire format changed.
