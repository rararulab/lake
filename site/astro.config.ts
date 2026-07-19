import mdx from "@astrojs/mdx";
import { unified } from "@astrojs/markdown-remark";
import sitemap from "@astrojs/sitemap";
import tailwindcss from "@tailwindcss/vite";
import { cp } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { defineConfig } from "astro/config";
import rehypeCallouts from "rehype-callouts";
import remarkToc from "remark-toc";
import config from "./astro-paper.config";
import { rehypeRewriteDocLinks } from "./src/utils/rewrite-doc-links";

const repositoryRoot = fileURLToPath(new URL("..", import.meta.url));
const docsRoot = fileURLToPath(new URL("../docs", import.meta.url));
const docsAssetsRoot = fileURLToPath(new URL("../docs/assets", import.meta.url));

export default defineConfig({
  site: config.site.url,
  base: config.site.base || "/",
  output: "static",
  integrations: [
    mdx(),
    sitemap(),
    {
      name: "publish-doc-assets",
      hooks: {
        "astro:build:done": async ({ dir }) => {
          await cp(docsAssetsRoot, new URL("assets/", dir), { recursive: true });
        },
      },
    },
  ],
  markdown: {
    processor: unified({
      remarkPlugins: [remarkToc],
      rehypePlugins: [
        rehypeCallouts,
        [
          rehypeRewriteDocLinks,
          {
            base: config.site.base,
            docsRoot,
            docsAssetsRoot,
            repositoryRoot,
            repositoryUrl: config.site.repositoryUrl,
          },
        ],
      ],
    }),
    shikiConfig: {
      themes: { light: "github-light", dark: "github-dark" },
      defaultColor: false,
      wrap: false,
    },
  },
  // Bun materializes Astro's Vite peer context separately even when both
  // copies resolve to 7.3.6. Their runtime contracts match, while TypeScript
  // treats Vite's private plugin-container types as nominally distinct.
  vite: { plugins: [tailwindcss() as never] },
});
