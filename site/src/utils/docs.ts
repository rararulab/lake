import type { CollectionEntry } from "astro:content";
import { relative, resolve, sep } from "node:path";
import { withBase } from "./withBase";

export type DocEntry = CollectionEntry<"docs">;

const docsRoot = resolve(process.cwd(), "../docs");

function normalizePath(path: string): string {
  return path.split(sep).join("/");
}

export function getDocSlug(entry: DocEntry): string {
  const source = entry.filePath
    ? normalizePath(relative(docsRoot, entry.filePath))
    : entry.id;
  return source
    .replace(/\.(md|mdx)$/i, "")
    .replace(/(^|\/)index$/i, "$1")
    .replace(/\/$/, "");
}

export function getDocUrl(entry: DocEntry): string {
  const slug = getDocSlug(entry);
  return withBase(slug ? `docs/${slug}/` : "docs/");
}

export function getDocTitle(entry: DocEntry): string {
  if (entry.data.title) return entry.data.title;

  const heading = entry.body?.match(/^#\s+(.+)$/m)?.[1];
  if (heading) return heading.replace(/[`*_]/g, "").trim();

  const segment = getDocSlug(entry).split("/").at(-1) ?? "Documentation";
  return segment
    .split("-")
    .map(word => word.charAt(0).toUpperCase() + word.slice(1))
    .join(" ");
}

export function getDocSection(entry: DocEntry): string {
  const [section] = getDocSlug(entry).split("/");
  return section && getDocSlug(entry).includes("/") ? section : "overview";
}

export function isPublicDoc(entry: DocEntry): boolean {
  return getDocSlug(entry).split("/").at(-1)?.toLowerCase() !== "agent";
}

export function sortDocs(entries: DocEntry[]): DocEntry[] {
  return [...entries].sort((left, right) => {
    const bySection = getDocSection(left).localeCompare(getDocSection(right));
    return bySection || getDocTitle(left).localeCompare(getDocTitle(right));
  });
}
