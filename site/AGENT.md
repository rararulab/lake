# site — AstroPaper site guidelines

## Purpose

This directory owns lake's AstroPaper-based public site, generated documentation,
and GitHub Pages build.

## Architecture

- `src/pages/index.astro` owns the landing-page composition and public copy.
- `src/content.config.ts` reads canonical Markdown directly from `../docs/`.
- `src/pages/docs/` emits the documentation index and static document routes.
- `src/styles/` carries the adapted AstroPaper visual system.
- `astro-paper.config.ts` derives the production URL and base path from
  `GITHUB_REPOSITORY`.

## Critical Invariants

- Product claims must stay consistent with `../goal.md`; design targets are never presented as achieved production metrics.
- The output must remain a static artifact with no runtime backend dependency.
- Every interactive control must be keyboard accessible and reduced-motion safe.
- Documentation must be rendered from `../docs/`; do not create a second canonical copy.
- `bun run check` must cover typechecking, linting, formatting, the production
  build, Pagefind indexing, and output smoke tests.

## What NOT To Do

- Do NOT copy Markdown from `../docs/` into the site tree.
- Do NOT add client-side routing; Astro owns static routes.
- Do NOT hard-code a GitHub Pages repository path; forks must build correctly.
- Do NOT use stock photography or fake product screenshots; the system diagram must explain the real architecture.

## Dependencies

Astro renders static routes, AstroPaper provides the accessible theme foundation,
Tailwind CSS provides tokens and utilities, and Pagefind indexes the generated
artifact before `.github/workflows/pages.yml` uploads `site/dist/`.
