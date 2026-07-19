declare global {
  interface Window {
    __lakeTheme?: "light" | "dark";
  }
}

export {};

function installThemeToggle(): void {
  const button = document.querySelector<HTMLButtonElement>("#theme-toggle");
  if (!button || button.dataset.ready === "true") return;

  button.dataset.ready = "true";
  button.addEventListener("click", () => {
    const root = document.documentElement;
    const next = root.dataset.theme === "dark" ? "light" : "dark";
    root.dataset.theme = next;
    root.classList.toggle("dark", next === "dark");
    localStorage.setItem("theme", next);
    button.setAttribute(
      "aria-label",
      `Use ${next === "dark" ? "light" : "dark"} theme`
    );
  });
}

installThemeToggle();
document.addEventListener("astro:page-load", installThemeToggle);
