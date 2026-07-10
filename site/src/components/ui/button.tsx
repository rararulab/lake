// Copyright 2026 Rararulab
// SPDX-License-Identifier: Apache-2.0

import { Slot } from "@radix-ui/react-slot";
import { cva, type VariantProps } from "class-variance-authority";
import * as React from "react";

import { cn } from "@/lib/utils";

const buttonVariants = cva(
  "inline-flex shrink-0 items-center justify-center gap-2 whitespace-nowrap rounded-md text-sm font-medium transition-[color,background-color,border-color,transform] outline-none focus-visible:ring-2 focus-visible:ring-[var(--signal)] focus-visible:ring-offset-2 focus-visible:ring-offset-[var(--ink)] disabled:pointer-events-none disabled:opacity-50 [&_svg]:pointer-events-none [&_svg]:size-4 [&_svg]:shrink-0",
  {
    variants: {
      variant: {
        default:
          "bg-[var(--signal)] text-[var(--signal-ink)] shadow-[0_0_0_1px_rgba(255,100,44,0.12)] hover:-translate-y-0.5 hover:bg-[var(--signal-bright)]",
        outline:
          "border border-white/16 bg-white/[0.035] text-[var(--paper)] hover:-translate-y-0.5 hover:border-white/28 hover:bg-white/[0.07]",
        ghost: "text-[var(--muted)] hover:bg-white/[0.06] hover:text-[var(--paper)]",
      },
      size: {
        default: "h-10 px-4 py-2",
        sm: "h-8 rounded-[5px] px-3 text-xs",
        lg: "h-12 rounded-md px-5 text-[0.9rem]",
        icon: "size-10",
      },
    },
    defaultVariants: {
      variant: "default",
      size: "default",
    },
  },
);

function Button({
  className,
  variant,
  size,
  asChild = false,
  ...props
}: React.ComponentProps<"button"> &
  VariantProps<typeof buttonVariants> & {
    asChild?: boolean;
  }) {
  const Comp = asChild ? Slot : "button";

  return (
    <Comp
      data-slot="button"
      className={cn(buttonVariants({ variant, size, className }))}
      {...props}
    />
  );
}

export { Button, buttonVariants };
