// Run and step status glyphs in one colour per state, the same on every surface: success green
// check-circle, failed red x-circle, cancelled amber slashed circle, running the app's purple Spinner (it
// spins under reduced motion too, as it says the run is in progress), queued a blue hollow dot, skipped a neutral arrow. Each is an img with the status word as its label and title
// (on a run row, the run title and a step row it alone carries the status), so colour is never the only signal.
// The glyph's box centres itself on the row that holds it (STATUS_BOX).

import { Spinner } from "../../components/ui/Spinner";
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

/** The text colour of each state: blue queued, the accent purple running, green done, red failed, amber cancelled, neutral skipped. */
export const COLOUR: Record<RunStatus | StepState, string> = {
  queued: "text-info",
  running: "text-accent",
  success: "text-good",
  failed: "text-danger",
  cancelled: "text-custom-ink",
  skipped: "text-text-2",
};

export function StatusIcon({
  status,
  size = 16,
  className,
  testId,
}: {
  status: RunStatus | StepState;
  size?: number;
  className?: string;
  testId?: string;
}) {
  const label = STATUS_LABEL[status] ?? status;
  return (
    <span
      role="img"
      aria-label={label}
      title={label}
      data-status={status}
      data-testid={testId}
      className={cx(STATUS_BOX, COLOUR[status], className)}
    >
      {status === "running" ? (
        // The app's one loading spinner (purple arc on the neutral track) in the glyph's box; this span says the word.
        <Spinner size={size} label={null} />
      ) : (
        <svg viewBox="0 0 16 16" width={size} height={size} aria-hidden="true" focusable="false">
          <Glyph status={status} />
        </svg>
      )}
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
    default:
      return <circle cx="8" cy="8" r="4.5" fill="none" stroke="currentColor" strokeWidth="1.5" />;
  }
}
