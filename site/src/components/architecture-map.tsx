// Copyright 2026 Rararulab
// SPDX-License-Identifier: Apache-2.0

import {
  IconArrowDown,
  IconCloudDataConnection,
  IconDatabase,
  IconServerBolt,
} from "@tabler/icons-react";

const layers = [
  {
    label: "Query layer",
    detail: "Stateless DataFusion + Flight SQL",
    note: "fan out reads",
    icon: IconServerBolt,
    tone: "signal",
  },
  {
    label: "Metadata layer",
    detail: "Bounded, leader-elected catalog authority",
    note: "cache misses + writes",
    icon: IconDatabase,
    tone: "neutral",
  },
  {
    label: "Object storage",
    detail: "Immutable, versioned table snapshots",
    note: "S3-compatible",
    icon: IconCloudDataConnection,
    tone: "neutral",
  },
] as const;

export function ArchitectureMap() {
  return (
    <div className="architecture-shell" aria-label="lake data path diagram">
      <div className="flex items-center justify-between border-b border-white/10 px-4 py-3 sm:px-5">
        <div className="flex items-center gap-2 font-mono text-[0.66rem] uppercase tracking-[0.12em] text-[var(--muted)]">
          <span className="size-1.5 rounded-full bg-[var(--signal)] shadow-[0_0_14px_var(--signal)]" />
          System path
        </div>
        <span className="font-mono text-[0.62rem] text-white/60">read / write</span>
      </div>
      <div className="space-y-2.5 p-3 sm:p-4">
        {layers.map((layer, index) => {
          const Icon = layer.icon;
          return (
            <div key={layer.label}>
              <div
                className={
                  layer.tone === "signal"
                    ? "architecture-layer architecture-layer-signal"
                    : "architecture-layer"
                }
              >
                <span className="architecture-icon">
                  <Icon stroke={1.6} />
                </span>
                <span className="min-w-0 flex-1">
                  <span className="block text-sm font-medium text-[var(--paper)]">{layer.label}</span>
                  <span className="mt-0.5 block text-xs leading-5 text-[var(--muted)]">{layer.detail}</span>
                </span>
                <span className="hidden rounded-full border border-white/10 px-2 py-1 font-mono text-[0.58rem] uppercase tracking-[0.08em] text-white/60 sm:block">
                  {layer.note}
                </span>
              </div>
              {index < layers.length - 1 ? (
                <div className="architecture-connector" aria-hidden="true">
                  <IconArrowDown size={13} stroke={1.5} />
                </div>
              ) : null}
            </div>
          );
        })}
      </div>
      <div className="architecture-glow" aria-hidden="true" />
    </div>
  );
}
