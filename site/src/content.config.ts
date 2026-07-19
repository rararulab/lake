import { defineCollection } from "astro:content";
import { glob } from "astro/loaders";
import { z } from "astro/zod";

export const DOCS_PATH = "../docs";

const docs = defineCollection({
  loader: glob({ pattern: "**/*.{md,mdx}", base: DOCS_PATH }),
  schema: z.object({
    title: z.string().optional(),
    description: z.string().optional(),
  }),
});

export const collections = { docs };
