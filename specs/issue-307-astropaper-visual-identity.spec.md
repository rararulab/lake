spec: task
name: "astropaper-visual-identity"
inherits: project
tags: [site, astropaper, github-pages, documentation]
---

## Intent

Lake's GitHub Pages implementation uses Astro and carries AstroPaper-derived
configuration and attribution, but its rendered visual identity still reproduces
the custom marketing site introduced before the migration.

Without this change, a visitor comparing the deployed site with AstroPaper
v6.1.0 sees the old wide hero, grid background, architecture panel, numbered
marketing sections, large design-target counters, Geist typography, and orange
signal palette. The repository can truthfully claim an Astro implementation,
but not that its public site uses the requested AstroPaper theme.

Replace that legacy visual composition with a lake-specific adaptation of
AstroPaper v6.1.0's editorial visual system. Continue publishing canonical
repository documentation, search, diagrams, and fork-safe static routes from
the same GitHub Pages artifact.

This advances the `goal.md` new-user signal by making the architecture and
operating documentation discoverable through a coherent public site. It does
not alter Lake runtime behavior or present planned capabilities as implemented.

## Decisions

- AstroPaper v6.1.0 is the visual reference version.
- Use AstroPaper's narrow `app-layout`, typography, theme tokens,
  header/navigation treatment, focus states, link language, cards, and vertical
  content rhythm.
- Replace the legacy marketing homepage with a concise lake introduction,
  understated links to documentation and GitHub, and AstroPaper-style lists for
  documentation entry points.
- Remove the legacy grid hero, read-path dashboard, architecture rows,
  design-target counter band, and marketing CTA treatment.
- Preserve lake-specific content grounded in `goal.md`; do not import
  AstroPaper demo posts, biography, tags, archives, or RSS solely for visual
  similarity.
- Use the same visual shell for homepage, docs index, docs pages, search, and
  404. Docs-specific navigation is allowed where needed.
- Lock the visual structure with fast Rust integration tests that read the
  checked-in Astro sources. Production rendering remains covered by
  `mise run site-check` and `site/scripts/check-output.ts`.
- Preserve canonical docs ingestion, Pagefind, sitemap, documentation diagrams,
  accessibility, reduced motion, theme switching, and GitHub Pages base-path
  handling.

## Boundaries

### Allowed Changes

site/src/**
site/public/**
site/scripts/check-output.ts
site/astro-paper.config.ts
site/package.json
site/bun.lock
site/README.md
site/AGENT.md
crates/lake-cli/tests/site_contract.rs
specs/issue-307-astropaper-visual-identity.spec.md
verification/issue-307-astropaper-visual-identity.md

### Forbidden

docs/**
.github/**
crates/**/src/**
crates/**/Cargo.toml
Cargo.toml
Cargo.lock
goal.md

## Constraints

- `docs/` remains the only canonical documentation source.
- The site remains fully static and adds no runtime backend or client-side
  router.
- Repository-relative routes work under both `/` locally and `/<repository>/`
  on GitHub Pages.
- Planned scale targets or robot-training APIs are not presented as shipped
  production behavior.
- AstroPaper attribution and MIT license text remain present.
- No upstream demo content is published as lake content.

## Completion Criteria

Rule: astropaper-visible-theme — AstroPaper is the site's visible design system
rather than only its implementation ancestry

Scenario: The homepage replaces the legacy marketing visual contract
  Test:
    Package: lake-cli
    Filter: astropaper_homepage_replaces_legacy_marketing_visual_contract
  Given the checked-in homepage, header, layout, and theme sources
  When the site visual contract is inspected
  Then the homepage uses AstroPaper's narrow editorial application shell,
  theme typography, header language, and content-list primitives
  And it does not contain the legacy `site-shell`, `hero-grid`, `layer-row`,
  architecture dashboard, large design-target counters, or marketing CTA bands
  And lake-specific copy remains aligned with `goal.md`

Scenario: AstroPaper wraps every public content route
  Test:
    Package: lake-cli
    Filter: astropaper_theme_wraps_all_public_site_routes
  Given the homepage, docs index, docs layout, search page, 404 page, and shared
  components
  When their layout and style dependencies are inspected
  Then each route uses the shared AstroPaper layout, header, footer, application
  width, theme tokens, and focus treatment
  And docs pages retain AstroPaper prose styling plus the navigation required to
  browse canonical repository documentation
  And none of those routes falls back to the previous marketing-site shell

## Out of Scope

- Changing canonical Markdown under `docs/`.
- Adding a blog, demo posts, tags, archives, author biography, comments, or RSS.
- Changing Rust runtime behavior, product architecture, deployment workflow, or
  GitHub Pages hosting model.
- Rewriting architecture diagrams or removing the diagram publication fix from
  issue #270.
- Pixel-for-pixel copying of AstroPaper demo content; the contract concerns its
  visual system and structure.
