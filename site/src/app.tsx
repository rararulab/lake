// Copyright 2026 Rararulab
// SPDX-License-Identifier: Apache-2.0

import {
  IconArrowRight,
  IconArrowUpRight,
  IconBolt,
  IconBrandGithub,
  IconBraces,
  IconCheck,
  IconDatabaseExport,
  IconLayersLinked,
} from "@tabler/icons-react";

import { ArchitectureMap } from "@/components/architecture-map";
import { CommandBlock } from "@/components/command-block";
import { LakeWordmark } from "@/components/lake-wordmark";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Separator } from "@/components/ui/separator";

const repositoryUrl = "https://github.com/rararulab/lake";

const workflow = [
  {
    index: "01",
    title: "Write once",
    body: "Robot fleets commit episode data as immutable files, then atomically advance a version pointer.",
    icon: IconDatabaseExport,
  },
  {
    index: "02",
    title: "Resolve precisely",
    body: "Readers resolve one table version through a cached catalog and never observe a half-written snapshot.",
    icon: IconLayersLinked,
  },
  {
    index: "03",
    title: "Fan out freely",
    body: "Stateless query nodes read object storage directly, so metadata load tracks cache misses, not reader count.",
    icon: IconBolt,
  },
] as const;

const principles = [
  ["Immutable by default", "Readers keep streaming the old snapshot while a writer commits the next one."],
  ["SQL at the edge", "DataFusion compute stays stateless and speaks Arrow Flight SQL."],
  ["Storage is a choice", "Lance is the default engine, isolated behind a trait for future engines."],
  ["Metadata stays bounded", "Leader-elected authority handles coordination while query nodes absorb fan-out."],
] as const;

export function App() {
  return (
    <div className="min-h-[100dvh] overflow-x-clip bg-[var(--ink)] text-[var(--paper)]">
      <a className="skip-link" href="#main-content">
        Skip to content
      </a>

      <header className="sticky top-0 z-50 border-b border-white/[0.08] bg-[color-mix(in_srgb,var(--ink)_86%,transparent)] backdrop-blur-xl">
        <div className="site-container flex h-16 items-center justify-between">
          <a href="#top" className="rounded-md outline-none focus-visible:ring-2 focus-visible:ring-[var(--signal)]">
            <LakeWordmark />
          </a>

          <nav aria-label="Primary navigation" className="flex items-center gap-1 sm:gap-2">
            <a className="nav-link hidden sm:inline-flex" href="#architecture">
              Architecture
            </a>
            <a className="nav-link hidden md:inline-flex" href="#workflow">
              Data path
            </a>
            <a className="nav-link hidden md:inline-flex" href="#targets">
              Targets
            </a>
            <Button asChild variant="outline" size="sm" className="ml-1">
              <a aria-label="View on GitHub" href={repositoryUrl} target="_blank" rel="noreferrer">
                <IconBrandGithub />
                <span className="hidden xs:inline">View on GitHub</span>
                <span className="xs:hidden">GitHub</span>
              </a>
            </Button>
          </nav>
        </div>
      </header>

      <main id="main-content">
        <section id="top" className="hero-grid relative scroll-mt-20 border-b border-white/[0.08]">
          <div className="site-container relative grid min-h-[calc(100dvh-4rem)] items-center gap-14 py-20 lg:grid-cols-[1.05fr_0.95fr] lg:gap-20 lg:py-24">
            <div className="relative z-10 max-w-[44rem]">
              <Badge className="mb-7">
                <span className="size-1.5 rounded-full bg-current" />
                Open source · built in Rust
              </Badge>
              <h1 className="max-w-[10ch] text-[clamp(3.2rem,8vw,7.4rem)] font-semibold leading-[0.86] tracking-[-0.068em] text-balance">
                The lakehouse for embodied AI.
              </h1>
              <p className="mt-7 max-w-[34rem] text-[1.04rem] leading-7 text-[var(--muted)] sm:text-lg sm:leading-8">
                Immutable robot episodes in object storage. Stateless SQL compute for training and evaluation at fleet scale.
              </p>
              <div className="mt-9 flex flex-col gap-3 sm:flex-row">
                <Button asChild size="lg">
                  <a href={repositoryUrl} target="_blank" rel="noreferrer">
                    <IconBrandGithub />
                    View on GitHub
                    <IconArrowUpRight />
                  </a>
                </Button>
                <Button asChild variant="outline" size="lg">
                  <a href="#architecture">
                    Explore architecture
                    <IconArrowRight />
                  </a>
                </Button>
              </div>
              <div className="mt-10 flex flex-wrap gap-x-6 gap-y-2 font-mono text-[0.66rem] uppercase tracking-[0.1em] text-white/60">
                <span>DataFusion SQL</span>
                <span>Arrow Flight</span>
                <span>S3 compatible</span>
                <span>Lance engine</span>
              </div>
            </div>

            <div className="relative mx-auto w-full max-w-[38rem] lg:max-w-none">
              <div className="hero-orbit" aria-hidden="true" />
              <ArchitectureMap />
              <div className="absolute -bottom-5 -left-3 hidden items-center gap-2 rounded-md border border-white/12 bg-[var(--ink-soft)] px-3 py-2 font-mono text-[0.62rem] text-white/60 shadow-xl sm:flex">
                <span className="size-1.5 animate-pulse rounded-full bg-[var(--signal)]" />
                metadata shielded by cache
              </div>
            </div>
          </div>
          <div className="hero-index" aria-hidden="true">
            01 / SYSTEM
          </div>
        </section>

        <section
          id="architecture"
          className="scroll-mt-20 border-b border-white/[0.08]"
          aria-label="Architecture"
        >
          <div className="site-container py-24 sm:py-32">
            <div className="section-kicker">Architecture / 01</div>
            <div className="mt-6 grid gap-10 lg:grid-cols-[0.82fr_1.18fr] lg:gap-20">
              <div>
                <h2 id="architecture-title" className="section-title max-w-[11ch]">
                  Separate what scales from what must agree.
                </h2>
                <p className="section-copy mt-6 max-w-[31rem]">
                  Query compute fans out with demand. Metadata remains a bounded authority. Object storage carries the bytes.
                </p>
              </div>
              <div className="lg:pt-3">
                <ArchitectureMap />
              </div>
            </div>
          </div>
        </section>

        <section id="workflow" className="scroll-mt-20 border-b border-white/[0.08]">
          <div className="site-container py-24 sm:py-32">
            <div className="grid gap-12 lg:grid-cols-[0.76fr_1.24fr] lg:gap-24">
              <div className="lg:sticky lg:top-28 lg:self-start">
                <div className="section-kicker">Data path / 02</div>
                <h2 className="section-title mt-6 max-w-[9ch]">One commit. Exact reads.</h2>
                <p className="section-copy mt-6 max-w-[28rem]">
                  Per-table versioning keeps the protocol small enough to reason about and strong enough for concurrent training reads.
                </p>
              </div>
              <div className="border-t border-white/12">
                {workflow.map((item) => {
                  const Icon = item.icon;
                  return (
                    <article key={item.index} className="workflow-row">
                      <span className="font-mono text-xs text-white/60">{item.index}</span>
                      <Icon className="mt-0.5 text-[var(--signal)]" size={21} stroke={1.5} />
                      <div>
                        <h3 className="text-xl font-medium tracking-[-0.025em]">{item.title}</h3>
                        <p className="mt-2 max-w-[34rem] text-sm leading-6 text-[var(--muted)] sm:text-base sm:leading-7">
                          {item.body}
                        </p>
                      </div>
                    </article>
                  );
                })}
              </div>
            </div>
          </div>
        </section>

        <section
          id="targets"
          className="target-field scroll-mt-20 border-b border-white/[0.08]"
          aria-labelledby="targets-title"
        >
          <div className="site-container py-24 sm:py-32">
            <div className="flex flex-col justify-between gap-7 sm:flex-row sm:items-end">
              <div>
                <div className="section-kicker">Scale / 03</div>
                <h2 id="targets-title" className="section-title mt-6">
                  Design targets
                </h2>
              </div>
              <p className="max-w-[27rem] text-sm leading-6 text-[var(--muted)]">
                These are architecture targets, not production claims. They define the workload lake is being built to absorb.
              </p>
            </div>

            <div className="mt-14 grid border-y border-white/12 md:grid-cols-2">
              <div className="target-cell md:border-r md:border-white/12">
                <Badge variant="outline">Design target</Badge>
                <div className="target-number">10⁴</div>
                <p className="mt-3 text-sm text-[var(--muted)]">tables across robot fleets and datasets</p>
              </div>
              <div className="target-cell border-t border-white/12 md:border-t-0">
                <Badge variant="outline">Design target</Badge>
                <div className="target-number">10¹¹</div>
                <p className="mt-3 text-sm text-[var(--muted)]">episodes held across immutable table versions</p>
              </div>
            </div>
            <div className="mt-5 flex items-center gap-3 font-mono text-[0.65rem] uppercase tracking-[0.1em] text-white/60">
              <IconBraces size={15} stroke={1.5} />
              Read fan-out should grow query compute, not metadata traffic
            </div>
          </div>
        </section>

        <section className="border-b border-white/[0.08]">
          <div className="site-container grid gap-12 py-24 sm:py-32 lg:grid-cols-[0.9fr_1.1fr] lg:items-center lg:gap-24">
            <div>
              <div className="section-kicker">Interface / 04</div>
              <h2 className="section-title mt-6 max-w-[10ch]">Files in. SQL out.</h2>
              <p className="section-copy mt-6 max-w-[31rem]">
                Ingest an episode file, commit a snapshot, then query it through the same catalog path used by the fleet.
              </p>
              <div className="mt-7 space-y-3 text-sm text-white/58">
                {["Zero schema setup", "Snapshot-by-version reads", "Local RocksDB development path"].map((item) => (
                  <div key={item} className="flex items-center gap-2.5">
                    <span className="grid size-5 place-items-center rounded-full border border-[color-mix(in_srgb,var(--signal)_40%,transparent)] text-[var(--signal)]">
                      <IconCheck size={12} stroke={2} />
                    </span>
                    {item}
                  </div>
                ))}
              </div>
            </div>
            <CommandBlock />
          </div>
        </section>

        <section className="border-b border-white/[0.08]">
          <div className="site-container py-24 sm:py-32">
            <div className="section-kicker">Principles / 05</div>
            <h2 className="section-title mt-6 max-w-[15ch]">Purpose-built constraints, not warehouse sprawl.</h2>
            <div className="mt-14 grid border-t border-white/12 sm:grid-cols-2">
              {principles.map(([title, body], index) => (
                <article
                  key={title}
                  className={`principle-cell ${index % 2 === 0 ? "sm:border-r sm:border-white/12" : ""}`}
                >
                  <div className="flex items-center justify-between gap-5">
                    <h3 className="text-base font-medium tracking-[-0.015em]">{title}</h3>
                    <span className="font-mono text-[0.62rem] text-white/60">0{index + 1}</span>
                  </div>
                  <p className="mt-3 max-w-[28rem] text-sm leading-6 text-[var(--muted)]">{body}</p>
                </article>
              ))}
            </div>
          </div>
        </section>

        <section className="relative overflow-hidden">
          <div className="cta-radial" aria-hidden="true" />
          <div className="site-container relative z-10 py-28 sm:py-40">
            <Badge>Apache 2.0</Badge>
            <h2 className="mt-7 max-w-[14ch] text-[clamp(2.7rem,7vw,6.5rem)] font-semibold leading-[0.91] tracking-[-0.06em]">
              Build the data plane robots need.
            </h2>
            <div className="mt-10 flex flex-col gap-4 sm:flex-row sm:items-center">
              <Button asChild size="lg">
                <a href={repositoryUrl} target="_blank" rel="noreferrer">
                  <IconBrandGithub />
                  View on GitHub
                  <IconArrowUpRight />
                </a>
              </Button>
              <span className="font-mono text-[0.68rem] uppercase tracking-[0.1em] text-white/60">
                Rust · DataFusion · Arrow · Lance
              </span>
            </div>
          </div>
        </section>
      </main>

      <Separator />
      <footer className="site-container flex flex-col gap-4 py-8 text-xs text-white/60 sm:flex-row sm:items-center sm:justify-between">
        <LakeWordmark className="opacity-75" />
        <span>Open infrastructure for embodied-AI data.</span>
        <a className="footer-link" href={repositoryUrl} target="_blank" rel="noreferrer">
          Apache-2.0 · GitHub
        </a>
      </footer>
    </div>
  );
}
