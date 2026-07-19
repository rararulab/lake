import { defineAstroPaperConfig } from "./src/types/config";

const fallbackRepository = "rararulab/lake";
const repository = process.env.GITHUB_REPOSITORY ?? fallbackRepository;
const [owner = "rararulab", name = "lake"] = repository.split("/");
const isGitHubActions = process.env.GITHUB_ACTIONS === "true";
const base = isGitHubActions ? `/${name}` : "";
const origin = isGitHubActions ? `https://${owner}.github.io` : "http://localhost:4321";

export default defineAstroPaperConfig({
  site: {
    url: `${origin}${base}/`,
    base,
    title: "lake",
    description:
      "An open-source lakehouse for embodied-AI data with stateless SQL compute and immutable object storage.",
    author: "Rararulab",
    lang: "en",
    repositoryUrl: `https://github.com/${repository}`,
  },
  features: {
    lightAndDarkMode: true,
    search: "pagefind",
  },
});
