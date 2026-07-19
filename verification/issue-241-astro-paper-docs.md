# #241 AstroPaper Pages and repository docs

## Scope

- Migrated the GitHub Pages frontend from the Vite/React single-page app to an
  AstroPaper-based static Astro site.
- Rendered canonical Markdown directly from `docs/` under `/docs/`, without a
  duplicated content tree.
- Added Pagefind search, sitemap/robots metadata, responsive documentation
  navigation, theme switching, and fork-safe GitHub Pages base paths.
- Updated the Pages workflow so changes under either `site/` or `docs/` deploy
  the combined artifact.

## Revisions

- Base: `a00e2705be39db24bd221a756847623ea99cb36c`
- Candidate: `1c3af7c10e5ebe838f65129c2f54d38d5a6e6383`
- Authority: author self-check; no independent verifier verdict is claimed.

## Verification

- `mise run gate` passed after the Rust stable toolchain finished updating:
  hooks, all workspace targets, ADBC interoperability, end-to-end self-check,
  Astro diagnostics, ESLint, Prettier, static build, Pagefind, and output smoke
  tests all completed successfully.
- `GITHUB_ACTIONS=true GITHUB_REPOSITORY=rararulab/lake mise exec -- bun run
  --cwd site build` passed: 88 static pages built and 86 pages indexed by
  Pagefind under the production `/lake/` base path.
- `mise exec -- bun run --cwd site test` passed and checked the landing page,
  documentation routes, search bundle, and canonical architecture rendering.
- Production HTML scans found no asset or route references that escape the
  `/lake/` base and no generated documentation links ending in `.md`/`.mdx`.
- `git diff --check` passed.

## Visual and hostile-path checks

- Inspected the landing, documentation index, document pages, search page, and
  404 page at 1440×900 and 390×844 in the browser.
- Confirmed a single primary heading, keyboard-visible navigation, collapsed
  mobile docs navigation, no horizontal page overflow, and no overflowing code
  blocks at 390 px.
- Browser console and page errors remained empty while navigating the tested
  routes and toggling the color theme.
- Verified internal Markdown links become Pages routes while repository-relative
  links outside `docs/` become GitHub source links.

## Result

Author self-check PASS. No P0/P1 findings remain.
