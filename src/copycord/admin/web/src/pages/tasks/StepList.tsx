// The run detail's Steps card, the same for every task type: one row per step with its status glyph,
// its title and its note on the right, and a thin progress bar under a running step whose done and
// total are known.

import { cx } from "../../components/ui/cx";
import type { TaskRun, TaskStep } from "../../types/tasks";
import { STATUS_LABEL, StatusIcon } from "./StatusIcon";
import { CARD_TITLE, FRAME, STEP_LINE, STEP_ROW } from "./TasksSkeleton";

const NUM = new Intl.NumberFormat("en-US");

export function formatCount(n: number): string {
  return NUM.format(n);
}

/** The note beside a step's title: the server label, else one derived from its state and counts. */
export function stepLabel(step: TaskStep): string {
  if (step.label) return step.label;
  if (step.state === "queued" && step.queue_position != null) return `Queued #${step.queue_position}`;
  if (step.state === "running" && step.done != null && step.total != null) {
    return `${formatCount(step.done)} / ${formatCount(step.total)}`;
  }
  if (step.note) return step.note;
  return STATUS_LABEL[step.state] ?? step.state;
}

function StepBar({ step }: { step: TaskStep }) {
  if (step.done == null || step.total == null || step.total <= 0) return null;
  const pct = Math.max(0, Math.min(100, (step.done / step.total) * 100));
  return (
    <div
      role="progressbar"
      aria-label={`${step.title} progress`}
      aria-valuemin={0}
      aria-valuemax={step.total}
      aria-valuenow={step.done}
      className="mt-2 ml-6 h-1 overflow-hidden rounded-full bg-line-1"
    >
      <div className="h-full rounded-full bg-progress" style={{ width: `${pct}%` }} />
    </div>
  );
}

export function StepList({ run }: { run: TaskRun }) {
  const steps = run.steps ?? [];
  return (
    <section aria-label="Steps" className={FRAME}>
      <h3 className={CARD_TITLE}>Steps</h3>
      {steps.length ? (
        <ul className="m-0 list-none p-0">
          {steps.map((s) => {
            const note = stepLabel(s);
            return (
              <li key={s.key} className={STEP_ROW}>
                <div className={STEP_LINE}>
                  <StatusIcon status={s.state} size={14} />
                  <span className="min-w-0 flex-1 truncate font-semibold text-text">{s.title}</span>
                  <span title={note} className={cx("max-w-[60%] truncate text-right text-xs", s.state === "failed" ? "text-danger" : "text-muted")}>
                    {note}
                  </span>
                </div>
                {s.state === "running" ? <StepBar step={s} /> : null}
              </li>
            );
          })}
        </ul>
      ) : (
        <p className="m-0 px-4 py-3 text-sm text-muted">No steps yet</p>
      )}
    </section>
  );
}
