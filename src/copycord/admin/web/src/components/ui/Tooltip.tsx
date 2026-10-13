// Tooltip shown on hover and keyboard focus, hidden on Escape. The trigger
// gets aria-describedby pointing at the tip.

import { cloneElement, useId, useState } from "react";
import type { ReactElement } from "react";
import { cx } from "./cx";

export interface TooltipProps {
  content: string;
  side?: "top" | "bottom";
  children: ReactElement<{ "aria-describedby"?: string }>;
}

const SIDE = {
  top: "bottom-full mb-2",
  bottom: "top-full mt-2",
} as const;

export function Tooltip({ content, side = "top", children }: TooltipProps) {
  const id = useId();
  const [open, setOpen] = useState(false);
  return (
    <span
      className="relative inline-flex"
      onMouseEnter={() => setOpen(true)}
      onMouseLeave={() => setOpen(false)}
      onFocus={() => setOpen(true)}
      onBlur={() => setOpen(false)}
      onKeyDown={(e) => {
        if (e.key === "Escape") setOpen(false);
      }}
    >
      {cloneElement(children, { "aria-describedby": open ? id : undefined })}
      {open ? (
        <span
          id={id}
          role="tooltip"
          className={cx(
            "pointer-events-none absolute left-1/2 z-50 -translate-x-1/2 whitespace-nowrap rounded-control border border-border bg-bg-3 px-2.5 py-1.5 text-xs text-text-1 shadow-card",
            SIDE[side],
          )}
        >
          {content}
        </span>
      ) : null}
    </span>
  );
}
