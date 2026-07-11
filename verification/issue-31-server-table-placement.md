# Verification report — issue #31

- base_sha: `5c70514b1a2ebe0128380f220f6929c92d3af9e7`
- head_sha: `50a87f3ebf7d84fe0bef1336292308ecb815c091`
- score_authority: verifier
- implementer_evidence: self_check_only

## Commands

### Candidate identity and clean state

```text
$ mise run doctor
[ ok ] mise tools installed
[ ok ] nightly rustfmt
[ ok ] cargo check
[ ok ] jj repo: /Users/ryan/code/rararulab/lake/.worktrees/issue-31-server-table-placement
[ ok ] gh authenticated
[ ok ] git remote: origin

$ jj st
The working copy has no changes.
Working copy  (@) : rtpnqkvy 9463ba70 (empty) (no description set)
Parent commit (@-): vqrntxll 50a87f3e issue-31-server-table-placement | refactor(storage): make table placement server-authoritative (#31)

$ jj log -r 'main|issue-31-server-table-placement'
50a87f3ebf7d84fe0bef1336292308ecb815c091 refactor(storage): make table placement server-authoritative (#31)
5c70514b1a2ebe0128380f220f6929c92d3af9e7 Merge pull request #30 from rararulab/issue-29-idempotent-append
```

The colocated repository's default Git HEAD was not used to identify or diff
the candidate.

### Full clean gate

```text
$ rm -rf data && test ! -e data && mise run gate
[hooks] Finished in 98.8ms
[site-check]  Test Files  2 passed (2)
[site-check]       Tests  5 passed (5)
[e2e] created table robots.episodes
[e2e] committed robots.episodes at v2
[e2e] self-check ok
[test] test result: ok. 31 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.44s
[test] test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 5.16s
[test] test result: ok. 17 passed; 0 failed; 2 ignored; 0 measured; 0 filtered out; finished in 10.25s
[test] Finished in 20.60s
Finished in 20.61s
```

The recurring macOS linker warning remained non-fatal:

```text
warning: linker stderr: ld: __eh_frame section too large (max 16MB) to encode dwarf unwind offsets in compact unwind table, performance of exception handling might be affected
```

### Spec lifecycle and candidate boundary

The standard wrapper's fixed `--change-scope worktree` read the default
colocated Git workspace rather than this jj workspace, producing the known
non-candidate boundary failure while all selectors passed:

```text
$ mise run spec-lifecycle specs/issue-31-server-table-placement.spec.md
Spec: server-table-placement  stage: complete  passed: false
  [FAIL] [boundaries] explicit change set respects declared paths
  [PASS] remote create derives the registered location from server policy
  [PASS] remote create rejects a legacy caller-selected location
  [PASS] placement rejects identifiers that can escape the managed root
  [PASS] local and S3 placement produce deterministic managed locations
  [PASS] remote CLI no longer exposes caller-selected placement
spec-lifecycle-guard: FAIL — agent-spec lifecycle exited 1
```

The candidate-authoritative boundary run passed all 18 paths obtained from
`jj diff --summary -r main..issue-31-server-table-placement`, each supplied as
a repeated `--change`:

```text
$ agent-spec lifecycle specs/issue-31-server-table-placement.spec.md --code . --format text <18 repeated --change arguments from jj diff>
EXPLICIT_CHANGE_COUNT=18
Quality: 100% (determinism: 100%, testability: 100%, coverage: 100%)
Results: 6 total, 6 passed, 0 failed, 0 skipped, 0 uncertain, 0 pending_review
  [PASS] [boundaries] explicit change set respects declared paths
  [PASS] remote create derives the registered location from server policy
  [PASS] remote create rejects a legacy caller-selected location
  [PASS] placement rejects identifiers that can escape the managed root
  [PASS] local and S3 placement produce deterministic managed locations
  [PASS] remote CLI no longer exposes caller-selected placement
Pass rate: 100.0%
```

### Acceptance selectors and repair regression

```text
$ cargo test -p lake-metasrv remote_create_uses_server_table_placement
test control::table_placement_tests::remote_create_uses_server_table_placement ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 30 filtered out; finished in 0.01s

$ cargo test -p lake-metasrv remote_create_rejects_caller_location
test control::table_placement_tests::remote_create_rejects_caller_location ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 30 filtered out; finished in 0.01s

$ cargo test -p lake-metasrv table_placement_rejects_unsafe_identifiers
test placement::tests::table_placement_rejects_unsafe_identifiers ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 30 filtered out; finished in 0.00s

$ cargo test -p lake-metasrv table_placement_derives_managed_locations
test placement::tests::table_placement_derives_managed_locations ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 30 filtered out; finished in 0.00s

$ cargo test -p lake-cli remote_create_table_has_no_location_argument
test tests::remote_create_table_has_no_location_argument ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 12 filtered out; finished in 0.00s

$ cargo test -p lake-metasrv remote_create_rejects_overlong_dataset_segment_before_mutation
test control::table_placement_tests::remote_create_rejects_overlong_dataset_segment_before_mutation ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 30 filtered out; finished in 0.01s
```

Every selector and the repair regression executed one real test; none was a
zero-match pass.

### Independent cold boot

```text
$ rm -rf /Users/ryan/code/rararulab/lake/.worktrees/issue-31-server-table-placement/data && mise run e2e
created table robots.episodes
committed robots.episodes at v2
+----------+----------+------------+
| robot_id | episodes | avg_reward |
+----------+----------+------------+
| alpha    | 2        | 0.8        |
| beta     | 1        | 0.4        |
+----------+----------+------------+
self-check ok
```

I removed `data/` once more, booted the candidate `target/debug/lake meta` on
an ephemeral loopback port, and drove the real remote CLI create/resolve path.

```text
REMOTE_SERVER_READY port=56368
created 机器人.剧集
  "current_version": 1,
  "location": "/Users/ryan/code/rararulab/lake/.worktrees/issue-31-server-table-placement/data/tables/机器人/剧集.lance",
```

Legacy caller placement remained absent from the CLI:

```text
LEGACY_LOCATION_EXIT=2
error: unexpected argument '--location' found
Usage: lake client create-table --column <name:type> <TABLE>
```

## Transition matrix

- fail_to_pass:
  - Read-only `jj file show main:<path>` inspection found all five acceptance
    selector names absent from `main`; the candidate ran one passing test for
    each.
  - Previous independent candidate `d880f2621bc0` accepted 250/255-byte table
    names at placement and failed in the engine. Candidate `50a87f3ebf7d`
    rejects both before mutation, while the 249-byte control still creates.
  - The repair is pinned by
    `remote_create_rejects_overlong_dataset_segment_before_mutation`, which
    asserts `InvalidArgument`, no registry entry, and no filesystem directory.
- pass_to_fail: 0; the complete clean gate passed.

## Probes

### Probe 1 — CJK namespace and table name

- input: remote create/resolve for namespace `机器人`, table `剧集`, column
  `编号:utf8` through a cold-booted real Metasrv.
- expected: create succeeds and resolves beneath the server-owned root.
- observed: exit 0, version 1, exact path ending in `机器人/剧集.lance`.
- result: PASS.

### Probe 2 — 249-byte table-name boundary

- input: remote create with a 249-byte ASCII table name; appending `.lance`
  yields a 255-byte storage segment.
- expected: create succeeds.
- observed:

```text
BOUNDARY_249_EXIT=0
created limit249.<249-byte-name>
```

- result: PASS.

### Probe 3 — 250/255-byte pre-mutation rejection

- input: remote creates with 250-byte and 255-byte table names in fresh,
  distinct namespaces.
- expected: `InvalidArgument`; no engine filesystem path and no registry entry.
- observed:

```text
BOUNDARY_250_EXIT=1
Error: metasrv error: invalid table name: must not exceed 249 UTF-8 bytes before the .lance suffix
BOUNDARY_255_EXIT=1
Error: metasrv error: invalid table name: must not exceed 249 UTF-8 bytes before the .lance suffix
BOUNDARY_250_RESOLVE=limit250.<250-byte-name>: not found
BOUNDARY_255_RESOLVE=limit255.<255-byte-name>: not found
BOUNDARY_250_FS_MUTATION=no
BOUNDARY_255_FS_MUTATION=no
```

The focused regression independently confirmed the underlying tonic status is
`InvalidArgument`.
- result: PASS.

## Verdict

PASS — the frozen candidate passes the clean gate, explicit jj candidate
boundary, all acceptance selectors, independent cold boot, real remote
create/resolve wiring, CJK input, legacy CLI rejection, and the repaired
249/250/255-byte mutation-boundary probes with `pass_to_fail = 0`.
