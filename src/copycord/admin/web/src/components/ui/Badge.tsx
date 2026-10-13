// Badge matches the old .badge.good / .badge-ok pills; StatusPill is the
// dot plus text used for server and client state.

import type { ReactNode } from "react";
import { cx } from "./cx";

export type BadgeTone = "neutral" | "good" | "warn" | "bad" | "accent";

const TONE: Record<BadgeTone, string> = {
  neutral: "border-border bg-bg2 text-[#e6e1ff]",
  good: "border-good/35 bg-good/12 text-good",
  warn: "border-custom-ink/35 bg-custom-ink/12 text-custom-ink",
  bad: "border-danger/35 bg-danger/12 text-danger",
  accent: "border-accent/35 bg-brand-ghost text-accent",
};

export function Badge({ tone = "neutral", className, children }: { tone?: BadgeTone; className?: string; children: ReactNode }) {
  return (
    <span
      className={cx(
        "inline-flex items-center gap-[0.5ch] rounded-full border px-2 py-0.5 text-xs font-semibold leading-normal",
        TONE[tone],
        className,
      )}
    >
      {children}
    </span>
  );
}

export type Status = "online" | "offline" | "warn" | "unknown";

const STATUS: Record<Status, { tone: BadgeTone; dot: string; text: string }> = {
  online: { tone: "good", dot: "bg-good", text: "Online" },
  offline: { tone: "bad", dot: "bg-danger", text: "Offline" },
  warn: { tone: "warn", dot: "bg-custom-ink", text: "Degraded" },
  unknown: { tone: "neutral", dot: "bg-text-2", text: "Unknown" },
};

export function StatusPill({ status, label, className }: { status: Status; label?: ReactNode; className?: string }) {
  const s = STATUS[status];
  return (
    <span role="status" data-status={status} className="inline-flex">
      <Badge tone={s.tone} className={className}>
        <span aria-hidden="true" className={cx("size-2 rounded-full", s.dot, status === "online" && "animate-pulse")} />
        {label ?? s.text}
      </Badge>
    </span>
  );
}
