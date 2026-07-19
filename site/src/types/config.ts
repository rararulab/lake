export interface AstroPaperConfig {
  site: {
    url: string;
    base: string;
    title: string;
    description: string;
    author: string;
    lang: string;
    repositoryUrl: string;
  };
  features: {
    lightAndDarkMode: boolean;
    search: "pagefind" | false;
  };
}

export function defineAstroPaperConfig(config: AstroPaperConfig): AstroPaperConfig {
  return config;
}
