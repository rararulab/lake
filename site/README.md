# lake marketing site

The static public site for lake. Product copy is grounded in [`../goal.md`](../goal.md), and design-target numbers must remain explicitly labeled as targets.

## Local development

The repository-managed Bun version is available through mise:

```bash
mise install
bun install --cwd site --frozen-lockfile
bun run --cwd site dev
```

Open <http://localhost:5173>. Run the complete frontend check with:

```bash
mise run site-check
```

That command installs the frozen dependency graph, typechecks the TypeScript project, runs Vitest, and builds the production artifact into `site/dist/`. It is also part of `mise run gate`.

## GitHub Pages

`.github/workflows/pages.yml` builds and deploys the site after relevant pushes to `main`. Vite derives the deployment base from `GITHUB_REPOSITORY`, so a fork named `my-lake` emits paths under `/my-lake/` without a code change.

Repository administrators must perform one initial setup:

1. Open **Settings → Pages**.
2. Set **Build and deployment → Source** to **GitHub Actions**.
3. Run the **Deploy marketing site** workflow or push a site change to `main`.

The expected project URL for this repository is <https://rararulab.github.io/lake/>.

## Component ownership

The site follows shadcn/ui's open-code model. Primitives under `src/components/ui/` are repository-owned source, not wrappers around a remote component package. Extend those primitives instead of introducing a second component system.
