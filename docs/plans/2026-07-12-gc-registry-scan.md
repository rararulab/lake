# GC registry scan implementation plan

**Goal:** Remove namespace/table N+1 metadata reads from managed-object GC
root validation without weakening its fail-closed fingerprint contract.

**Architecture:** Keep the full deterministic `BTreeMap` snapshot and all
existing validation points. Replace only the snapshot source with the typed
registry scan API already shared by catalog refresh.

## Task 1: Lock the metadata-I/O contract with a failing test

1. Add an instrumented `MetaStore` in the GC command tests that delegates to
   RocksDB and counts scan, list, and point-get calls.
2. Register tables in more than one namespace and assert snapshot equality,
   one scan, and zero list/get calls.
3. Run the focused test and confirm RED against the namespace/list/get
   implementation.

## Task 2: Read the registry root through one scan

1. Narrow `registry_snapshot` to the `MetaStore` boundary.
2. Build its deterministic `BTreeMap` from `registry::scan_tables`.
3. Update planning and apply call sites without changing their validation
   order or fingerprint representation.
4. Add destructive-path regressions proving registry changes before apply and
   between bounded pages stop deletion, plus legacy traversal fingerprint
   parity.
5. Run all bound tests and confirm GREEN.

## Task 3: Document and verify

1. Record the single-scan GC root behavior in architecture and managed-object
   operations documentation.
2. Run `mise run spec-lifecycle specs/issue-61-gc-registry-scan.spec.md`.
3. Run `mise run gate`, independent verification, and independent review.
4. Push, open one PR for #61, and merge after approval.
