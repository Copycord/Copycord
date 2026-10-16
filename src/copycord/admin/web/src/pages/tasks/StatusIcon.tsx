// Run and step status glyphs in one colour per state, the same on every surface: success green
// check-circle, failed red x-circle, cancelled amber slashed circle, running a blue spinning ring, queued
// a blue hollow dot, skipped a neutral arrow. Each is an img with a label and a title, and the status
// badge on the same row carries the word, so colour is never the only signal.
// The glyph's box centres itself on the row that holds it (STATUS_BOX).

import { Badge, type BadgeTone } from "../../components/ui/Badge";
import { cx } from "../../components/ui/cx";
import type { RunStatus, StepState } from "../../types/tasks";
import { RUN_PILL_BADGE, STATUS_BOX } from "./TasksSkeleton";

export const STATUS_LABEL: Record<RunStatus | StepState, string> = {
  queued: "Queued",
  running: "Running",
  success: "Success",
  failed: "Failed",
  cancelled: "Cancelled",
  skipped: "Skipped",
};

/** The text colour of each state: blue while active, green done, red failed, amber cancelled, neutral skipped. */
export const COLOUR: Record<RunStatus | StepState, string> = {
  queued: "text-info",
  running: "text-info",
  success: "text-good",
  failed: "text-danger",
  cancelled: "text-custom-ink",
  skipped: "text-text-2",
};

export function StatusIcon({ status, size = 16, className }: { status: RunStatus | StepState; size?: number; className?: string }) {
  const label = STATUS_LABEL[status] ?? status;
  return (
    <span role="img" aria-label={label} title={label} data-status={status} className={cx(STATUS_BOX, COLOUR[status], className)}>
      <svg
        viewBox="0 0 16 16"
        width={size}
        height={size}
        aria-hidden="true"
        focusable="false"
        className={status === "running" ? "animate-spin motion-reduce:animate-none" : undefined}
      >
        <Glyph status={status} />
      </svg>
    </span>
  );
}

/** The status badge's tint: the state's colour as a Badge tone; queued is the blue outline alone. */
const PILL_TONE: Record<RunStatus, BadgeTone> = {
  queued: "info",
  running: "info",
  success: "good",
  failed: "bad",
  cancelled: "warn",
};

/**
 * A run's status as the one coloured badge on its row or header: 24 px, 12 px text, the status word
 * alone, tinted by state; the glyph stands at the start of the row or before the title (StatusIcon).
 */
export function RunStatusPill({ status, testId, className }: { status: RunStatus; testId?: string; className?: string }) {
  const label = STATUS_LABEL[status] ?? status;
  return (
    <span data-testid={testId} title={`Status: ${label}`} className={cx("inline-flex", className)}>
      <Badge tone={PILL_TONE[status] ?? "neutral"} className={cx(RUN_PILL_BADGE, status === "queued" && "bg-transparent")}>
        {label}
      </Badge>
    </span>
  );
}

function Glyph({ status }: { status: RunStatus | StepState }) {
  switch (status) {
    case "success":
      return (
        <>
          <circle cx="8" cy="8" r="8" fill="currentColor" />
          <path d="M4.75 8.25 7 10.5l4.25-4.75" fill="none" stroke="var(--color-page)" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" />
        </>
      );
    case "failed":
      return (
        <>
          <circle cx="8" cy="8" r="8" fill="currentColor" />
          <path d="m5.5 5.5 5 5m0-5-5 5" fill="none" stroke="var(--color-page)" strokeWidth="1.6" strokeLinecap="round" />
        </>
      );
    case "cancelled":
      return (
        <>
          <circle cx="8" cy="8" r="6.75" fill="none" stroke="currentColor" strokeWidth="1.5" />
          <path d="m3.5 12.5 9-9" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
        </>
      );
    case "skipped":
      return (
        <>
          <circle cx="8" cy="8" r="6.75" fill="none" stroke="currentColor" strokeWidth="1.5" />
          <path d="M5 8h6M8.75 5.5 11.25 8l-2.5 2.5" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
        </>
      );
    case "running":
      return (
        <>
          <circle cx="8" cy="8" r="6.75" fill="none" stroke="currentColor" strokeOpacity="0.25" strokeWidth="2" />
          <path d="M8 1.25A6.75 6.75 0 0 1 14.75 8" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
        </>
      );
    default:
      return <circle cx="8" cy="8" r="4.5" fill="none" stroke="currentColor" strokeWidth="1.5" />;
  }
}
