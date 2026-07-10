// Copyright 2026 Rararulab
// SPDX-License-Identifier: Apache-2.0

import { render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { App } from "@/app";

describe("lake marketing site", () => {
  it("introduces lake with one clear primary heading", () => {
    render(<App />);

    expect(
      screen.getByRole("heading", {
        level: 1,
        name: /the lakehouse for embodied ai/i,
      }),
    ).toBeInTheDocument();
    expect(screen.getAllByRole("heading", { level: 1 })).toHaveLength(1);
  });

  it("provides accessible navigation and a repository call to action", () => {
    render(<App />);

    const navigation = screen.getByRole("navigation", {
      name: /primary navigation/i,
    });
    expect(within(navigation).getByRole("link", { name: /architecture/i })).toHaveAttribute(
      "href",
      "#architecture",
    );

    const repositoryLinks = screen.getAllByRole("link", { name: /view on github/i });
    expect(repositoryLinks.length).toBeGreaterThan(0);
    for (const link of repositoryLinks) {
      expect(link).toHaveAttribute("href", "https://github.com/rararulab/lake");
    }
  });

  it("describes the real three-tier architecture", () => {
    render(<App />);

    const architecture = screen.getByRole("region", { name: /architecture/i });
    expect(within(architecture).getByText("Query layer")).toBeInTheDocument();
    expect(within(architecture).getByText("Metadata layer")).toBeInTheDocument();
    expect(within(architecture).getByText("Object storage")).toBeInTheDocument();
  });

  it("labels scale numbers as design targets", () => {
    render(<App />);

    const targets = screen.getByRole("region", { name: /design targets/i });
    expect(within(targets).getByText("10⁴")).toBeInTheDocument();
    expect(within(targets).getByText("10¹¹")).toBeInTheDocument();
    expect(within(targets).getAllByText(/design target/i).length).toBeGreaterThan(0);
  });
});
