// Badge matches the old .badge.good / .badge-ok pills, fill and text only: the 1px border stays
// transparent so the pill keeps its box. StatusPill is the dot plus text used for server and client state.

import type { ReactNode } from "react";
import { cx } from "./cx";

export type BadgeTone = "neutral" | "good" | "warn" | "bad" | "accent" | "info";

const TONE: Record<BadgeTone, string> = {
  neutral: "bg-bg2 text-text-soft",
  good: "bg-good/12 text-good",
  warn: "bg-custom-ink/12 text-custom-ink",
  bad: "bg-danger/12 text-danger",
  accent: "bg-brand-ghost text-accent",
  info: "bg-info/12 text-info",
};

/** md: 24 px tall; sm: 20 px, to sit in a 20 px line of text without growing it. */
const SIZE = { md: "px-2 py-0.5 leading-normal", sm: "px-1.5 py-px leading-4" } as const;

export function Badge({
  tone = "neutral",
  size = "md",
  className,
  title,
  children,
}: {
  tone?: BadgeTone;
  size?: keyof typeof SIZE;
  className?: string;
  title?: string;
  children: ReactNode;
}) {
  return (
    <span
      title={title}
      className={cx(
        "inline-flex items-center gap-[0.5ch] rounded-full border border-transparent text-xs font-semibold",
        SIZE[size],
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
