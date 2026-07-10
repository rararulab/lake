# site — Agent Guidelines

## Purpose

This directory owns lake's static public marketing site and its GitHub Pages build.

## Architecture

- `src/app.tsx` owns page composition and public copy.
- `src/components/ui/` contains shadcn/ui open-code primitives owned by this repository.
- Focused visual components live directly under `src/components/`.
- `src/index.css` owns the single dark visual theme and responsive motion.
- `vite.config.ts` derives the production base path from `GITHUB_REPOSITORY`.

## Critical Invariants

- Product claims must stay consistent with `../goal.md`; design targets are never presented as achieved production metrics.
- The output must remain a static artifact with no runtime backend dependency.
- Every interactive control must be keyboard accessible and reduced-motion safe.
- `bun run check` must cover typechecking, tests, and the production build.

## What NOT To Do

- Do NOT introduce a second component system; extend the local shadcn primitives instead.
- Do NOT add client-side routing for this single-page site.
- Do NOT hard-code a GitHub Pages repository path; forks must build correctly.
- Do NOT use stock photography or fake product screenshots; the system diagram must explain the real architecture.

## Dependencies

React renders the site, Tailwind CSS provides tokens and utilities, shadcn primitives provide component structure, and Vite emits `dist/` for `.github/workflows/pages.yml`.
