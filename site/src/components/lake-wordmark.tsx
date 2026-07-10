// Copyright 2026 Rararulab
// SPDX-License-Identifier: Apache-2.0

import { cn } from "@/lib/utils";

export function LakeWordmark({ className }: { className?: string }) {
  return (
    <span className={cn("inline-flex items-center gap-2.5", className)} aria-label="lake home">
      <span className="relative grid size-7 place-items-center rounded-[6px] border border-white/14 bg-white/[0.055] font-mono text-[0.68rem] font-semibold text-[var(--paper)]">
        lk
        <span className="absolute -right-0.5 -top-0.5 size-1.5 rounded-full bg-[var(--signal)]" />
      </span>
      <span className="font-mono text-[0.9rem] font-semibold tracking-[-0.02em] text-[var(--paper)]">
        lake
      </span>
    </span>
  );
}
