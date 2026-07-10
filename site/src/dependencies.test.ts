// Copyright 2026 Rararulab
// SPDX-License-Identifier: Apache-2.0

import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

interface PackageManifest {
  dependencies?: Record<string, string>;
}

describe("site dependency contract", () => {
  it("declares every self-hosted font imported by the stylesheet", () => {
    const manifest = JSON.parse(
      readFileSync(resolve(process.cwd(), "package.json"), "utf8"),
    ) as PackageManifest;

    expect(manifest.dependencies).toHaveProperty("@fontsource-variable/geist");
    expect(manifest.dependencies).toHaveProperty("@fontsource-variable/geist-mono");
  });
});
