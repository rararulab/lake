// Copyright 2026 Rararulab
// SPDX-License-Identifier: Apache-2.0

import { IconTerminal2 } from "@tabler/icons-react";

const commands = [
  ["01", "lake ingest", "robots.arm_left ./episode.parquet"],
  ["02", "lake sql", '"SELECT * FROM robots.arm_left LIMIT 8"'],
] as const;

export function CommandBlock() {
  return (
    <div className="overflow-hidden rounded-lg border border-white/12 bg-[#0a0a09] shadow-[0_26px_80px_rgba(0,0,0,0.34)]">
      <div className="flex items-center justify-between border-b border-white/10 px-4 py-3">
        <span className="flex items-center gap-2 font-mono text-[0.65rem] uppercase tracking-[0.1em] text-[var(--muted)]">
          <IconTerminal2 size={14} stroke={1.6} />
          from file to SQL
        </span>
        <span className="font-mono text-[0.6rem] text-white/30">local / S3</span>
      </div>
      <div className="divide-y divide-white/[0.07]">
        {commands.map(([number, command, arguments_]) => (
          <div key={number} className="grid grid-cols-[2rem_1fr] gap-2 px-4 py-4 font-mono text-[0.73rem] sm:grid-cols-[2.5rem_8rem_1fr] sm:px-5">
            <span className="select-none text-white/22">{number}</span>
            <span className="text-[var(--signal)]">{command}</span>
            <span className="col-start-2 break-all text-white/62 sm:col-start-auto">{arguments_}</span>
          </div>
        ))}
      </div>
      <div className="border-t border-white/10 bg-white/[0.025] px-4 py-3 font-mono text-[0.61rem] text-white/35 sm:px-5">
        immutable snapshot committed · query resolves exact version
      </div>
    </div>
  );
}
