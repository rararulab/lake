# Lake Marketing Site Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a polished, responsive GitHub Pages site that explains lake's embodied-AI lakehouse architecture and gives visitors a direct path to the repository.

**Architecture:** Add an isolated Vite + React application under `site/`, using locally owned shadcn/ui components and Tailwind CSS. Keep the site static and dependency-light, derive the production base path from `GITHUB_REPOSITORY`, and deploy the built artifact through a least-privilege GitHub Pages workflow.

**Tech Stack:** React 19, TypeScript, Vite, Tailwind CSS v4, shadcn/ui open-code components, Tabler Icons, Vitest, Testing Library, Bun, GitHub Pages.

---

### Task 1: Establish the site contract and toolchain

**Files:**
- Create: `site/AGENT.md`
- Create: `site/package.json`
- Create: `site/components.json`
- Create: `site/tsconfig.json`
- Create: `site/tsconfig.app.json`
- Create: `site/tsconfig.node.json`
- Create: `site/vite.config.ts`
- Create: `site/index.html`
- Create: `site/src/test/setup.ts`
- Modify: `.gitignore`
- Modify: `mise.toml`

**Step 1: Add the isolated site package and TypeScript/Vite test configuration**

Pin runtime and development dependencies, configure the `@` alias, and expose `dev`, `test`, `typecheck`, `build`, and `check` scripts.

**Step 2: Install dependencies**

Run: `bun install --cwd site`

Expected: `site/bun.lock` is created and Bun exits successfully.

**Step 3: Wire the repository quality gate**

Add a `site-check` mise task and include it in `gate`, so the landing page cannot silently regress outside the Rust checks.

**Step 4: Verify the empty scaffold**

Run: `bun run --cwd site typecheck`

Expected: PASS before application source files are introduced.

### Task 2: Drive the public content contract with tests

**Files:**
- Create: `site/src/app.test.tsx`

**Step 1: Write failing tests**

Test that the page exposes one primary heading, an accessible navigation landmark, the GitHub repository CTA, honest design-target labels, and the three system tiers from `goal.md`.

**Step 2: Run the test to verify RED**

Run: `bun run --cwd site test`

Expected: FAIL because `App` does not exist yet.

### Task 3: Implement the shadcn-based interface

**Files:**
- Create: `site/src/main.tsx`
- Create: `site/src/app.tsx`
- Create: `site/src/index.css`
- Create: `site/src/lib/utils.ts`
- Create: `site/src/components/ui/button.tsx`
- Create: `site/src/components/ui/badge.tsx`
- Create: `site/src/components/ui/separator.tsx`
- Create: `site/src/components/lake-wordmark.tsx`
- Create: `site/src/components/architecture-map.tsx`
- Create: `site/src/components/command-block.tsx`

**Step 1: Implement the minimal semantic page**

Build the navigation, non-centered split hero, architecture map, workflow narrative, design targets, code path, and final GitHub CTA using shadcn primitives and semantic HTML.

**Step 2: Run the test to verify GREEN**

Run: `bun run --cwd site test`

Expected: PASS.

**Step 3: Apply the visual system**

Implement the graphite-and-signal-orange theme, responsive typography, industrial grid texture, mixed-density sections, focus states, and reduced-motion-safe transitions.

**Step 4: Refactor while green**

Extract repeated data and presentation into focused components without changing the tested content contract.

**Step 5: Run package checks**

Run: `bun run --cwd site check`

Expected: typecheck, tests, and production build all PASS.

### Task 4: Add GitHub Pages deployment

**Files:**
- Create: `.github/workflows/pages.yml`

**Step 1: Add the deployment workflow**

On pushes to `main` that touch the site or workflow, run the hermetic site check, upload `site/dist`, and deploy through the `github-pages` environment.

**Step 2: Validate workflow syntax and local production output**

Run: `mise run hooks && bun run --cwd site build`

Expected: hooks pass and `site/dist/index.html` exists with repository-relative assets.

### Task 5: Visual QA, documentation, and repository verification

**Files:**
- Create: `site/README.md`
- Create: `verification/report.md`

**Step 1: Run the site locally and inspect desktop and mobile widths**

Run: `bun run --cwd site dev -- --host 127.0.0.1`

Expected: no horizontal overflow, clipped content, console errors, inaccessible controls, or motion that ignores reduced-motion.

**Step 2: Document local development and Pages setup**

Record the Bun/mise commands and the one-time repository setting: Settings, Pages, Source, GitHub Actions.

**Step 3: Run final verification**

Run: `mise run site-check && mise run gate`

Expected: the frontend package and the full lake repository gate PASS.

**Step 4: Commit locally**

Run: `jj commit -m "feat(site): add lake marketing page (#0)"`

Expected: a self-contained local commit in the marketing-site workspace; no push or merge.
