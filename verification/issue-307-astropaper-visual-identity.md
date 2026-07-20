# Verification report — issue #307

- base_sha: `3729455699c7d9ed28b7b57263ab8abf5a283a50`
- head_sha: `ea640341c745711cd39bb1a5e118aaaa9a8f809d`
- score_authority: verifier
- implementer_evidence: self_check_only (not read)
- root_gate_evidence: `mise run gate` exit 0 under normal host permissions
- root_visual_evidence: production artifact inspected in the in-app browser; not executed by this verifier

The fixed candidate was SHA-pinned and clean before verification. The workspace's
current `@` was an empty commit and candidate `ea640341` was `@-`.

## Commands

### Candidate identity and boundaries

```text
$ jj st
The working copy has no changes.
Working copy  (@) : rvnxowln 60899bfa (empty) (no description set)
Parent commit (@-): rmksyovr ea640341 fix(site): use AstroPaper visual theme (#307)

$ git merge-base ea640341 origin/main
3729455699c7d9ed28b7b57263ab8abf5a283a50

$ git show -s --format='%H %P %s' ea640341
ea640341c745711cd39bb1a5e118aaaa9a8f809d 3729455699c7d9ed28b7b57263ab8abf5a283a50 fix(site): use AstroPaper visual theme (#307)
```

`git diff --check` produced no output. The 20 changed paths were all inside the
spec's allow-list. No forbidden `docs/**`, `.github/**`, crate source/Cargo,
root Cargo, or `goal.md` path changed.

### Lane-1 bindings

```text
$ env RUSTC_WRAPPER= cargo test -p lake-cli astropaper_homepage_replaces_legacy_marketing_visual_contract -- --exact
running 1 test
test astropaper_homepage_replaces_legacy_marketing_visual_contract ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out

$ env RUSTC_WRAPPER= cargo test -p lake-cli astropaper_theme_wraps_all_public_site_routes -- --exact
running 1 test
test astropaper_theme_wraps_all_public_site_routes ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out
```

The guarded lifecycle used a workspace-local Cargo target because mise's normal
cache path is outside this verifier sandbox:

```text
$ mise x -- env CARGO_TARGET_DIR=/Users/ryan/code/rararulab/lake/.worktrees/issue-307-astropaper-visual-identity/target RUSTC_WRAPPER= bun scripts/spec-lifecycle-guard.ts specs/issue-307-astropaper-visual-identity.spec.md
=== Lifecycle Report (guarded) ===
Spec: astropaper-visual-identity  stage: complete  passed: true
  [PASS] The homepage replaces the legacy marketing visual contract
  [PASS] AstroPaper wraps every public content route
spec-lifecycle-guard: OK — every Test selector executed >=1 test
```

### Site checks

Independent `mise run site-check` reached and passed the non-network checks:

```text
Result (25 files):
- 0 errors
- 0 warnings
- 0 hints
$ eslint .
$ prettier --check .
All matched files use Prettier code style!
```

The verifier sandbox then denied Astro's internal font HTTP listener before
static generation:

```text
Error: listen EPERM: operation not permitted 0.0.0.0
code: 'EPERM'
syscall: 'listen'
address: '0.0.0.0'
error: script "build" exited with code 1
```

This is an environment restriction rather than candidate behavior. The parent
provided fresh root evidence that the complete `mise run gate`, including
site production build, Pagefind, output smoke, workspace tests, and e2e, exited
0 under normal host permissions. This report does not relabel that as verifier-
executed evidence.

The verifier independently checked the source/output contracts after the
sandbox build failure:

```text
PASS source shared shell/legacy site/src/pages/index.astro
PASS source shared shell/legacy site/src/pages/docs/index.astro
PASS source shared shell/legacy site/src/pages/search.astro
PASS source shared shell/legacy site/src/pages/404.astro
PASS production config/docs/Pagefind/diagram/license source probes

$ env GITHUB_ACTIONS=true GITHUB_REPOSITORY=example-owner/lake-fork bun -e '<config probe>'
PASS fork-safe config /lake-fork
```

Those probes verified:

- every public route imports the shared Layout, Header, and Footer and uses
  `app-layout` plus `main-content`;
- the docs layout retains `app-prose` and `DocsNav`;
- legacy `site-shell`, `content-shell`, `hero-grid`, `layer-row`, numbered
  architecture, counter, and marketing markers are absent from all shell routes;
- production `rararulab/lake` resolves to `/lake` and
  `https://rararulab.github.io/lake/`;
- fork config derives `/lake-fork` rather than hard-coding `/lake`;
- Pagefind uses `withBase("pagefind/")`, output smoke requires the Pagefind
  bundle and both diagram artifacts, and the AstroPaper MIT license remains.

### Browser evidence boundary

The verifier did not execute a browser. The parent supplied root visual evidence
from the production artifact:

```text
desktop: AstroPaper narrow editorial column
390x844: content width 358px; no horizontal overflow
mobile menu: opens and closes
home/docs/search/404: shared app-layout shell
```

This evidence is recorded as `root_visual_evidence`, not verifier-executed
evidence.

## Transition matrix

- fail_to_pass: At `base_sha`, the homepage contained `hero-grid`, `site-shell`,
  `Read path`, `Architecture / 01`, `layer-row`, `Design targets`, `10⁴`, and
  `10¹¹`. At `head_sha`, those markers are absent; the AstroPaper application
  shell markers are present; both bound tests execute 1/1 and pass; guarded
  lifecycle reports 2/2 PASS; source, production-config, docs, diagram,
  Pagefind, attribution, and fork-base probes pass.
- pass_to_fail: `0` in the root full gate and the verifier's scoped checks.

## Probes

1. **Clean fixed candidate** — expected a clean working copy and candidate based
   directly on current main; observed clean `@`, fixed `@- = ea640341`, and
   merge-base/parent `37294556`. **PASS**.
2. **Legacy-shell removal across route types** — expected home, docs index,
   search, and 404 to share the narrow shell and contain no legacy markers;
   observed all required shared imports/classes and zero legacy markers.
   **PASS**.
3. **Docs/search/diagram preservation** — expected canonical docs layout,
   Pagefind base-aware bundle, and both diagram artifacts to remain required by
   output smoke; observed all source and smoke-test contracts. **PASS**.
4. **Fork repository name** — input `example-owner/lake-fork`; expected base
   `/lake-fork` and site URL `https://example-owner.github.io/lake-fork/`;
   observed exact values. **PASS**.
5. **Responsive/interactive production UI** — root browser evidence observed
   358px content width at 390px viewport, no horizontal overflow, working menu,
   and common shell on all public route types. **PASS (root visual evidence)**.

This change has no Lake ingest/commit/read runtime surface; the production site
artifact and root full gate are the applicable end-to-end path.

## Verdict

**PASS** — the fixed candidate is clean and SHA-pinned; independent bound tests,
guarded lifecycle, type/lint/format checks, route/theme/base/docs/search/diagram
probes all pass; and the normal-permission root gate plus explicitly attributed
root browser evidence cover production generation and visual interaction that
the verifier sandbox could not execute.
