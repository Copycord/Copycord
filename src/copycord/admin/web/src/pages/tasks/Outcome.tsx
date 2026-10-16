// What a run did, the same on the list row and the run page: its steps by outcome ("6 steps · 2 done ·
// 4 cancelled · 92 delivered") and the segmented bar (done green, cancelled amber, failed red, skipped
// neutral, the rest of the track still to go), so a cancelled run never reads as a full bar.

import { cx } from "../../components/ui/cx";
import type { TaskRun } from "../../types/tasks";
import { formatCount } from "./StepList";

export interface StepCounts {
  total: number;
  success: number;
  failed: number;
  skipped: number;
  cancelled: number;
}

const FINAL = ["success", "failed", "skipped", "cancelled"] as const;

/** Finished steps of any outcome. */
export function finishedOf(c: StepCounts): number {
  return c.success + c.failed + c.skipped + c.cancelled;
}

/**
 * The run's steps by outcome: from its steps when it holds them (the run page), else from its summary
 * counts and step_count (a list row). Null while the run has no steps to count.
 */
export function stepCounts(run: Pick<TaskRun, "summary" | "steps" | "step_count">): StepCounts | null {
  const c: StepCounts = { total: 0, success: 0, failed: 0, skipped: 0, cancelled: 0 };
  if (run.steps?.length) {
    for (const s of run.steps) if ((FINAL as readonly string[]).includes(s.state)) c[s.state as (typeof FINAL)[number]] += 1;
    c.total = run.steps.length;
  } else {
    const sum = run.summary ?? {};
    for (const k of FINAL) c[k] = typeof sum[k] === "number" ? Math.max(0, sum[k]) : 0;
    c.total = Math.max(run.step_count ?? 0, finishedOf(c));
  }
  return c.total > 0 ? c : null;
}

/** The messages the run delivered, when its summary counts them. */
export function deliveredOf(run: Pick<TaskRun, "summary">): number | null {
  const v = run.summary?.delivered;
  return typeof v === "number" ? v : null;
}

const WORDS: [keyof Omit<StepCounts, "total">, string][] = [
  ["success", "done"],
  ["failed", "failed"],
  ["cancelled", "cancelled"],
  ["skipped", "skipped"],
];

/** "6 steps · 2 done · 4 cancelled · 92 delivered"; an outcome no step had is left out. */
export function outcomeText(counts: StepCounts | null, delivered: number | null): string {
  const parts: string[] = [];
  if (counts) {
    parts.push(`${formatCount(counts.total)} ${counts.total === 1 ? "step" : "steps"}`);
    for (const [k, word] of WORDS) if (counts[k]) parts.push(`${formatCount(counts[k])} ${word}`);
  }
  if (delivered != null) parts.push(`${formatCount(delivered)} delivered`);
  return parts.join(" · ");
}

/** "6/6 steps finished". */
export function finishedText(counts: StepCounts): string {
  return `${formatCount(finishedOf(counts))}/${formatCount(counts.total)} ${counts.total === 1 ? "step" : "steps"} finished`;
}

/** The segments in the order they fill the track, each in its state's colour. */
const SEGMENTS: [keyof Omit<StepCounts, "total">, string][] = [
  ["success", "bg-good"],
  ["cancelled", "bg-custom-ink"],
  ["failed", "bg-danger"],
  ["skipped", "bg-text-2/60"],
];

/** The segmented bar: one segment per outcome sized by its share of the steps; the bare track is what is left. */
export function SegBar({ counts, className }: { counts: StepCounts; className?: string }) {
  const finished = finishedOf(counts);
  const label = `${formatCount(finished)} of ${formatCount(counts.total)} steps finished`;
  return (
    <div
      role="progressbar"
      aria-label={label}
      aria-valuemin={0}
      aria-valuemax={counts.total}
      aria-valuenow={finished}
      title={label}
      className={cx("flex gap-0.5 overflow-hidden rounded-full bg-line-1", className)}
    >
      {SEGMENTS.filter(([k]) => counts[k] > 0).map(([k, bg]) => (
        <span
          key={k}
          data-segment={k}
          className={cx("h-full transition-[width] duration-300 ease-out motion-reduce:transition-none", bg)}
          style={{ width: `${(counts[k] / counts.total) * 100}%` }}
        />
      ))}
    </div>
  );
}
