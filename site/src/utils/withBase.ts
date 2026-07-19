const base = import.meta.env.BASE_URL.replace(/\/+$/, "");
const baseRoot = base === "" ? "/" : `${base}/`;

export function withBase(path: string): string {
  const normalizedPath = path.replace(/^\/+/, "");
  return normalizedPath ? `${baseRoot}${normalizedPath}` : baseRoot;
}

export function stripBase(pathname: string): string {
  if (!base || pathname === base) return pathname === base ? "/" : pathname;
  return pathname.startsWith(baseRoot) ? pathname.slice(base.length) : pathname;
}
