const THEME_KEY = "theme";
const LIGHT = "light";
const DARK = "dark";

function getPreferredTheme(): string {
  const stored = localStorage.getItem(THEME_KEY);
  if (stored) return stored;
  return window.matchMedia("(prefers-color-scheme: dark)").matches ? DARK : LIGHT;
}

let themeValue: string =
  (window as unknown as { __theme?: { value: string } }).__theme?.value ??
  getPreferredTheme();

function reflect(): void {
  const root = document.firstElementChild;
  root?.setAttribute("data-theme", themeValue);
  root?.classList.toggle("dark", themeValue === DARK);
  document.querySelector("#theme-btn")?.setAttribute("aria-label", themeValue);

  const background = window.getComputedStyle(document.body).backgroundColor;
  document
    .querySelector('meta[name="theme-color"]')
    ?.setAttribute("content", background);
}

function setup(): void {
  reflect();
  const button = document.querySelector<HTMLButtonElement>("#theme-btn");
  if (!button || button.dataset.ready === "true") return;

  button.dataset.ready = "true";
  button.addEventListener("click", () => {
    themeValue = themeValue === LIGHT ? DARK : LIGHT;
    localStorage.setItem(THEME_KEY, themeValue);
    reflect();
  });
}

setup();
document.addEventListener("astro:page-load", setup);

window
  .matchMedia("(prefers-color-scheme: dark)")
  .addEventListener("change", ({ matches }) => {
    themeValue = matches ? DARK : LIGHT;
    localStorage.setItem(THEME_KEY, themeValue);
    reflect();
  });
