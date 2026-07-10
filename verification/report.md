# Verification report — issue #0

- lane: 2
- base_ref: `main`
- base_sha: `2e04f20c5f4d6479d3d05ebd9c80c874abf17901`
- contrast_pre_fix_head: `86047af01ffe82bb0be7979099453ee48ccc732c`
- verified_site_head: `2a5f45924b14e71c7c5d7a076d497b779990d8f1`
- head_ref: `9719dcbde07f`
- head_sha: `9719dcbde07fe73c1a4e4a9212a5be995e2dfa18`
- site_tree_sha: `bec87c6c80d57021adfb9a9030ae0bd1509a3a6c`
- font_repair_round: 1 of 1, retained and reverified
- reviewer_p1_followup: yes
- score_authority: verifier
- implementer_evidence: self_check_only

Reviewer and implementer conclusions were ignored. The final head was checked
independently. Its diff from the fully audited site head is documentation and a
mise comment only, and both revisions resolve `site/` to the exact same tree
object. Lighthouse, axe, and cold-install evidence is therefore preserved
against byte-identical site inputs; the full gate was rerun at the final head.
The only working-copy product-external change was this verifier-owned report in
the empty child of the candidate commit.

The `web-perf` skill's chrome-devtools MCP was not configured, and the in-app
browser discovery list was empty. The requested audit was therefore executed
with the official Lighthouse CLI and axe-core CLI against the production Vite
artifact using the locally installed Google Chrome.

## Commands

### Environment and candidate identity

```console
$ mise run doctor
[ ok ] mise tools installed
[ ok ] nightly rustfmt
[ ok ] cargo check
[ ok ] jj repo: /Users/ryan/code/rararulab/lake/.worktrees/issue-0-marketing-site
[ ok ] gh authenticated
[warn] git remote origin missing — issue/PR flow unavailable

$ git rev-parse main
2e04f20c5f4d6479d3d05ebd9c80c874abf17901
$ git rev-parse 9719dcbde07f
9719dcbde07fe73c1a4e4a9212a5be995e2dfa18
$ git merge-base main 9719dcbde07f
2e04f20c5f4d6479d3d05ebd9c80c874abf17901
```

### Final docs-only delta and site identity

```console
$ git diff --stat 2a5f4592..9719dcbd
 docs/guides/workflow.md | 2 ++
 mise.toml               | 2 +-
 2 files changed, 3 insertions(+), 1 deletion(-)

$ git diff --name-status 2a5f4592..9719dcbd
M docs/guides/workflow.md
M mise.toml

$ git diff 2a5f4592..9719dcbd
+   - `mise run site-check` — frozen Bun install, TypeScript typecheck,
+     Vitest, and the GitHub Pages production build
-# ponytail: gate = the FAST local loop (hooks, cargo test, e2e).
+# ponytail: gate = the FAST local loop (hooks, cargo test, e2e, marketing site).

$ git rev-parse 2a5f4592:site
bec87c6c80d57021adfb9a9030ae0bd1509a3a6c
$ git rev-parse 9719dcbd:site
bec87c6c80d57021adfb9a9030ae0bd1509a3a6c
```

Result: **PASS**. No site source, dependency, build configuration, workflow
semantics, or executable task changed after the Lighthouse/axe/cold audit.

### Lighthouse color contrast

At `verified_site_head`, the production artifact was built with
`GITHUB_ACTIONS=true GITHUB_REPOSITORY=rararulab/lake` and served at
`http://127.0.0.1:4173/lake/`. The final head has the identical `site_tree_sha`.

```console
$ npx -y lighthouse http://127.0.0.1:4173/lake/ --only-categories=accessibility --output=json --output-path=/tmp/lake-lighthouse-2a5f4592.json --chrome-path='/Applications/Google Chrome.app/Contents/MacOS/Google Chrome' --chrome-flags='--headless=new --no-sandbox' --quiet
{
  "lighthouseVersion": "13.4.0",
  "accessibilityScore": 1,
  "colorContrastScore": 1,
  "colorContrastViolations": 0
}
```

Result: **PASS**, zero Lighthouse `color-contrast` violations.

### axe-core color contrast

```console
$ npx -y @axe-core/cli http://127.0.0.1:4173/lake/ --rules color-contrast --exit --save lake-axe-2a5f4592.json --dir /tmp --no-reporter --chrome-path='/Applications/Google Chrome.app/Contents/MacOS/Google Chrome' --chrome-options='--headless=new' --load-delay 500
Running axe-core 4.12.1 in chrome-headless
Testing complete of 1 pages
{
  "reports": 1,
  "violationRules": 0,
  "colorContrastViolationRules": 0,
  "colorContrastViolationNodes": 0
}
```

Result: **PASS**, zero axe `color-contrast` violations and zero violating
nodes.

### Independently observed contrast transition

A temporary jj workspace was created at previous head `86047af01ffe`, cold
installed, built with the same production environment, audited with the same
CLI versions, then forgotten and removed.

```console
$ npx -y lighthouse http://127.0.0.1:4174/lake/ --only-categories=accessibility ...
{
  "accessibilityScore": 0.95,
  "colorContrastScore": 0,
  "colorContrastViolations": 12
}

$ npx -y @axe-core/cli http://127.0.0.1:4174/lake/ --rules color-contrast ...
{
  "colorContrastViolationRules": 1,
  "colorContrastViolationNodes": 11
}
```

The accessibility change is therefore an observed fail-to-pass transition,
not a conclusion inherited from review.

### Pages workflow permissions and configure placement

```console
$ bun -e '<split build/deploy jobs and assert permissions/actions/path>'
{
  "topLevelContentsRead": true,
  "buildHasNoConfigurePages": true,
  "deployHasConfigurePages": true,
  "configurePagesCount": true,
  "deployPagesWrite": true,
  "deployNeedsBuild": true,
  "configureBeforeDeploy": true,
  "uploadArtifactV4": true,
  "artifactPath": true
}
```

Result: **PASS**. `actions/configure-pages@v5` occurs exactly once, in the
`deploy` job that owns `pages: write` and `id-token: write`, before
`actions/deploy-pages@v4`. The `build` job has no configure step and uploads
`site/dist` with `actions/upload-pages-artifact@v4`. Workflow-level permission
remains `contents: read`.

The owning task description is also synchronized:

```console
$ mise tasks | rg 'site-check|gate '
gate              fast local quality gate: hooks + Rust tests + e2e + marketing site
site-check        install, typecheck, test, and build the GitHub Pages marketing site
```

`AGENT.md`, `CLAUDE.md`, `docs/guides/workflow.md`, and
`docs/guides/mise-ci.md` all describe the site as part of `gate`; the mise CI
guide also records frozen install before typecheck/test/build.

### Preserved frozen cold install and package check

```console
$ bun_bin=$(mise which bun); tmp=$(mktemp -d); mkdir -p "$tmp/site"; rsync -a --exclude node_modules --exclude dist site/ "$tmp/site/"; test ! -e "$tmp/site/node_modules"; test ! -e "$tmp/site/dist"; "$bun_bin" install --cwd "$tmp/site" --frozen-lockfile; "$bun_bin" run --cwd "$tmp/site" check
1.3.14
bun install v1.3.14 (0d9b296a)
+ @fontsource-variable/geist@5.2.9
+ @fontsource-variable/geist-mono@5.2.8
140 packages installed [820.00ms]
$ tsc -b
$ vitest run
Test Files  2 passed (2)
Tests  5 passed (5)
$ vite build
transforming...✓ 6179 modules transformed.
✓ built in 408ms
```

Result: **PASS**. The original verifier font-dependency failure remains fixed
from a dependency-free copy under the project-pinned Bun. The final head uses
the identical site tree and lockfile.

### Full gate at final head

```console
$ rm -rf data; mise run gate
[hooks] $ prek run --all-files
[test] $ cargo test --workspace --all-targets
[e2e] $ cargo run -p lake-cli -- selftest
[site-install] $ bun install --cwd site --frozen-lockfile
[site-install] Checked 140 installs across 203 packages (no changes) [5.00ms]
[site-check] Test Files  2 passed (2)
[site-check] Tests  5 passed (5)
[test] test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
[e2e] created table robots.episodes
[e2e] committed robots.episodes at v2
[e2e] | alpha    | 2        | 0.8        |
[e2e] | beta     | 1        | 0.4        |
[e2e] self-check ok
[site-check] transforming...✓ 6179 modules transformed.
[site-check] ✓ built in 1.84s
Finished in 4.37s
```

Result: **PASS**. The two documented LocalStack-only tests remained ignored
per their test contracts; all runnable tests passed. E2e used a fresh `data/`.

### Preserved standalone site check and Pages base

```console
$ mise run site-check
Checked 140 installs across 203 packages (no changes) [6.00ms]
Test Files  2 passed (2)
Tests  5 passed (5)
transforming...✓ 6179 modules transformed.
✓ built in 1.79s
Finished in 3.04s

$ GITHUB_ACTIONS=true GITHUB_REPOSITORY=rararulab/lake bun run --cwd site build
✓ built in 1.88s
src="/lake/assets/index-B5O1l5fP.js"
href="/lake/assets/index-CLfACtca.css"
PASS: production asset references use /lake/
```

## Transition matrix

- fail_to_pass: Lighthouse `color-contrast` changed from score 0 / 12 violating
  items at `86047af01ffe` to score 1 / 0 items at `2a5f45924b14`; axe changed
  from 1 violating rule / 11 nodes to 0 / 0. The configure step changed from
  the unprivileged build job to the explicitly privileged deploy job. Final
  head `9719dcbde07f` preserves the exact audited site tree.
- pass_to_fail: 0. The pinned cold install, font dependency contract, full gate,
  standalone site check, e2e path, Pages base, artifact path, and least-
  privilege boundaries all remained green.

## Probes

1. Lighthouse and axe against both predecessor and candidate production
   artifacts: expected contrast fail→pass; observed 12→0 Lighthouse items and
   11→0 axe nodes, **PASS**.
2. Pinned Bun 1.3.14 frozen install without `node_modules` or `dist`: expected
   both Geist packages and a complete package check; observed 140 packages,
   5 tests, typecheck, and production build, **PASS**.
3. Workflow job-boundary assertion: expected configure only inside the deploy
   permission domain and artifact upload inside build; all nine structural
   assertions were true, **PASS**.

## Verdict

**PASS** — final head `9719dcbde07f` changes only gate documentation/comments,
preserves the byte-identical Lighthouse/axe/cold-audited site tree, keeps
configure-pages in the correct privileged deploy job, and passes the freshly
rerun full gate with `pass_to_fail = 0`.
