// Run and step status glyphs in the theme colours: success check-circle, failed x-circle, cancelled
// slashed circle, running a spinning ring, queued a hollow dot. Each is an img with a label and a title.
// The glyph's box centres itself on the row that holds it (STATUS_BOX), so a run list row, a step row
// and the run header all show it mid-row, beside both lines where the row has a title and a meta line.

import { Badge, type BadgeTone } from "../../components/ui/Badge";
import { cx } from "../../components/ui/cx";
import type { RunStatus, StepState } from "../../types/tasks";
import { STATUS_BOX } from "./TasksSkeleton";

export const STATUS_LABEL: Record<RunStatus | StepState, string> = {
  queued: "Queued",
  running: "Running",
  success: "Success",
  failed: "Failed",
  cancelled: "Cancelled",
  skipped: "Skipped",
};

export const COLOUR: Record<RunStatus | StepState, string> = {
  queued: "text-muted",
  running: "text-primary",
  success: "text-good",
  failed: "text-danger",
  cancelled: "text-muted",
  skipped: "text-muted",
};

export function StatusIcon({ status, size = 16, className }: { status: RunStatus | StepState; size?: number; className?: string }) {
  const label = STATUS_LABEL[status] ?? status;
  return (
    <span role="img" aria-label={label} title={label} data-status={status} className={cx(STATUS_BOX, COLOUR[status], className)}>
      <svg viewBox="0 0 16 16" width={size} height={size} aria-hidden="true" focusable="false" className={status === "running" ? "animate-spin" : undefined}>
        <Glyph status={status} />
      </svg>
    </span>
  );
}

/** The status pill's tint: the status colour's Badge tone. */
const PILL_TONE: Record<RunStatus, BadgeTone> = {
  queued: "neutral",
  running: "accent",
  success: "good",
  failed: "bad",
  cancelled: "neutral",
};

/** A run's status as a pill: the app's Badge, a 12 px glyph and the status word. The run page header and the run list rows. */
export function RunStatusPill({ status, testId, className }: { status: RunStatus; testId?: string; className?: string }) {
  return (
    <span data-testid={testId} className={cx("inline-flex", className)}>
      <Badge tone={PILL_TONE[status] ?? "neutral"}>
        <span aria-hidden="true" className="inline-flex">
          <StatusIcon status={status} size={12} />
        </span>
        {STATUS_LABEL[status]}
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
    case "skipped":
      return (
        <>
          <circle cx="8" cy="8" r="6.75" fill="none" stroke="currentColor" strokeWidth="1.5" />
          <path d="m3.5 12.5 9-9" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
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
