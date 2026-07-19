# lake site and documentation

The static public site for lake, built on
[AstroPaper](https://github.com/satnaing/astro-paper). Product copy is grounded
in [`../goal.md`](../goal.md), and the documentation routes render the canonical
Markdown in [`../docs/`](../docs/) directly.

## Local development

The repository-managed Bun version is available through mise:

```bash
mise install
bun install --cwd site --frozen-lockfile
bun run --cwd site dev
```

Open <http://localhost:4321>. Run the complete frontend check with:

```bash
mise run site-check
```

That command installs the frozen dependency graph, typechecks and lints the Astro
project, builds the production artifact into `site/dist/`, indexes it with
Pagefind, and checks the expected landing-page and documentation output. It is
also part of `mise run gate`.

## GitHub Pages

`.github/workflows/pages.yml` builds and deploys the site after relevant pushes
to `main`. Astro derives the deployment origin and base from
`GITHUB_REPOSITORY`, so a fork named `my-lake` emits paths under `/my-lake/`
without a code change. Changes under either `site/` or `docs/` trigger a deploy.

Repository administrators must perform one initial setup:

1. Open **Settings → Pages**.
2. Set **Build and deployment → Source** to **GitHub Actions**.
3. Run the **Deploy site and documentation** workflow or push a site or docs
   change to `main`.

The expected project URL for this repository is <https://rararulab.github.io/lake/>.

## Theme attribution

The theme foundation is adapted from AstroPaper v6.1.0 by Sat Naing under the
MIT License. See [`ASTROPAPER_LICENSE`](ASTROPAPER_LICENSE). Lake-specific page
composition, documentation routing, and product copy are maintained here.
