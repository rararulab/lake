import { dirname, relative, resolve, sep } from "node:path";

interface ElementNode {
  type?: string;
  tagName?: string;
  properties?: Record<string, unknown>;
  children?: ElementNode[];
}

interface VFileLike {
  path?: string;
}

interface Options {
  base: string;
  docsRoot: string;
  docsAssetsRoot: string;
  repositoryRoot: string;
  repositoryUrl: string;
}

function normalizePath(path: string): string {
  return path.split(sep).join("/");
}

function isInside(root: string, target: string): boolean {
  const path = relative(root, target);
  return path === "" || (!path.startsWith("..") && !path.includes(`..${sep}`));
}

export function rehypeRewriteDocLinks(options: Options) {
  const base = options.base ? `${options.base.replace(/\/$/, "")}/` : "/";

  return (tree: ElementNode, file: VFileLike): void => {
    if (!file.path || !isInside(options.docsRoot, file.path)) return;

    const visit = (node: ElementNode): void => {
      if (node.tagName === "a" && typeof node.properties?.href === "string") {
        const href = node.properties.href;
        if (!/^(?:[a-z]+:|#|\/\/)/i.test(href)) {
          const [pathWithQuery, hash = ""] = href.split("#", 2);
          const [pathname, query = ""] = pathWithQuery.split("?", 2);
          const target = resolve(dirname(file.path!), decodeURIComponent(pathname));
          const suffix = `${query ? `?${query}` : ""}${hash ? `#${hash}` : ""}`;

          if (/\.mdx?$/i.test(pathname) && isInside(options.docsRoot, target)) {
            const slug = normalizePath(relative(options.docsRoot, target))
              .replace(/\.(md|mdx)$/i, "")
              .replace(/(^|\/)index$/i, "$1")
              .replace(/\/$/, "");
            node.properties.href = `${base}docs/${slug ? `${slug}/` : ""}${suffix}`;
          } else if (isInside(options.docsAssetsRoot, target)) {
            const asset = normalizePath(relative(options.docsAssetsRoot, target));
            node.properties.href = `${base}assets/${asset}${suffix}`;
          } else if (isInside(options.repositoryRoot, target)) {
            const repositoryPath = normalizePath(
              relative(options.repositoryRoot, target)
            );
            node.properties.href = `${options.repositoryUrl}/blob/main/${repositoryPath}${suffix}`;
          }
        }
      }

      node.children?.forEach(visit);
    };

    visit(tree);
  };
}
