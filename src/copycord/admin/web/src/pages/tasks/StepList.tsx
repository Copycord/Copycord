// The run detail's left column: the Summary card (run.summary counts) and the step list. Each step
// shows its icon, title, a label and a thin progress bar when done and total are known. Clicking a
// step filters the log to it; "All steps" clears the filter.

import { cx } from "../../components/ui/cx";
import type { TaskRun, TaskStep } from "../../types/tasks";
import { STATUS_LABEL, StatusIcon } from "./StatusIcon";
import { FRAME } from "./TasksSkeleton";

const NUM = new Intl.NumberFormat("en-US");

export function formatCount(n: number): string {
  return NUM.format(n);
}

/** The line under a step's title: the server label, else one derived from its state and counts. */
export function stepLabel(step: TaskStep): string {
  if (step.label) return step.label;
  if (step.state === "queued" && step.queue_position != null) return `Queued #${step.queue_position}`;
  if (step.state === "running" && step.done != null && step.total != null) {
    return `Cloning ${formatCount(step.done)} / ${formatCount(step.total)}`;
  }
  if (step.note) return step.note;
  return STATUS_LABEL[step.state] ?? step.state;
}

function summaryLabel(key: string): string {
  const words = key.replace(/_/g, " ");
  return words.charAt(0).toUpperCase() + words.slice(1);
}

export function SummaryCard({ summary }: { summary: TaskRun["summary"] }) {
  const rows = Object.entries(summary ?? {}).filter(([, v]) => typeof v === "number");
  return (
    <section aria-label="Summary" className={FRAME}>
      <h3 className="m-0 border-b border-border px-4 py-2.5 text-sm font-semibold text-text">Summary</h3>
      {rows.length ? (
        <dl className="m-0 grid grid-cols-[1fr_auto] gap-x-3 gap-y-1 px-4 py-3 text-sm">
          {rows.map(([k, v]) => (
            <div key={k} className="contents">
              <dt className="text-muted">{summaryLabel(k)}</dt>
              <dd className="m-0 text-right font-semibold tabular-nums text-text">{formatCount(v)}</dd>
            </div>
          ))}
        </dl>
      ) : (
        <p className="m-0 px-4 py-3 text-sm text-muted">No counts yet</p>
      )}
    </section>
  );
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
      className="mt-1.5 h-1 w-full overflow-hidden rounded-full bg-line-1"
    >
      <div className="h-full rounded-full bg-progress" style={{ width: `${pct}%` }} />
    </div>
  );
}

export interface StepListProps {
  run: TaskRun;
  /** The step key the log is filtered to, or null for all steps. */
  selected: string | null;
  onSelect: (key: string | null) => void;
}

export function StepList({ run, selected, onSelect }: StepListProps) {
  const steps = run.steps ?? [];
  const item = "flex w-full cursor-pointer items-start gap-2 border-0 border-l-2 px-3 py-2 text-left text-sm";
  return (
    <div className="flex flex-col gap-4">
      <SummaryCard summary={run.summary} />
      <section aria-label="Steps" className={FRAME}>
        <h3 className="m-0 border-b border-border px-4 py-2.5 text-sm font-semibold text-text">Steps</h3>
        <ul className="m-0 list-none p-0">
          <li>
            <button
              type="button"
              aria-pressed={selected === null}
              onClick={() => onSelect(null)}
              className={cx(item, selected === null ? "border-accent bg-rail-active text-text" : "border-transparent bg-transparent text-text-2 hover:bg-rail-hover")}
            >
              All steps
            </button>
          </li>
          {steps.map((s) => {
            const on = selected === s.key;
            return (
              <li key={s.key}>
                <button
                  type="button"
                  aria-pressed={on}
                  onClick={() => onSelect(on ? null : s.key)}
                  className={cx(item, on ? "border-accent bg-rail-active" : "border-transparent bg-transparent hover:bg-rail-hover")}
                >
                  <StatusIcon status={s.state} size={14} className="mt-0.5" />
                  <span className="min-w-0 flex-1">
                    <span className="block truncate font-semibold text-text">{s.title}</span>
                    <span className={cx("block truncate text-xs", s.state === "failed" ? "text-danger" : "text-muted")}>{stepLabel(s)}</span>
                    <StepBar step={s} />
                  </span>
                </button>
              </li>
            );
          })}
        </ul>
        {steps.length === 0 ? <p className="m-0 px-4 py-3 text-sm text-muted">No steps yet</p> : null}
      </section>
    </div>
  );
}
