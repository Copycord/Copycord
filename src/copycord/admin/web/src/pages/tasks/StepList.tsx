// The run detail's Steps card, the same for every task type. The head sums the steps up (counts by
// state, an overall bar) and filters them by state; under it one dense line per step with its status
// glyph, its title, a thin bar while it runs and its note on the right. Running and failed steps sort
// to the top, and the list scrolls past twelve rows, so a run of a hundred channels keeps a short card.

import { useMemo, useState } from "react";
import { useLastCount } from "../../components/ui/lastCount";
import { cx } from "../../components/ui/cx";
import type { StepState, TaskRun, TaskStep } from "../../types/tasks";
import { STATUS_LABEL, StatusIcon } from "./StatusIcon";
import {
  FADE_IN,
  FRAME,
  STEPS_BAR,
  STEPS_COUNT_KEY,
  STEP_BAR,
  STEP_EMPTY,
  STEP_LIST,
  STEP_ROW,
  StepsHead,
  type StepFilter,
} from "./TasksSkeleton";

const NUM = new Intl.NumberFormat("en-US");

export function formatCount(n: number): string {
  return NUM.format(n);
}

/** A task type's own note for a step (types/index.tsx stepNote), null to fall through. */
export type StepNote = (step: TaskStep) => string | null;

/** The note beside a step's title: the server label, the type's note, else one from its state and counts. */
export function stepLabel(step: TaskStep, noteOf?: StepNote): string {
  if (step.label) return step.label;
  const typed = noteOf?.(step);
  if (typed) return typed;
  if (step.state === "queued" && step.queue_position != null) return `Queued #${step.queue_position}`;
  if (step.state === "running" && step.done != null && step.total != null) {
    return `${formatCount(step.done)} / ${formatCount(step.total)}`;
  }
  if (step.note) return step.note;
  return STATUS_LABEL[step.state] ?? step.state;
}

/** The counts line words, in the order the line reads; a state with no steps is left out. */
const COUNT_WORDS: [StepState, string][] = [
  ["running", "running"],
  ["queued", "queued"],
  ["success", "done"],
  ["failed", "failed"],
  ["skipped", "skipped"],
  ["cancelled", "cancelled"],
];

/** "3 running, 2 queued, 94 done, 1 failed"; empty without steps. */
export function stepSummary(steps: TaskStep[]): string {
  const counts = new Map<StepState, number>();
  for (const s of steps) counts.set(s.state, (counts.get(s.state) ?? 0) + 1);
  return COUNT_WORDS.filter(([state]) => counts.get(state))
    .map(([state, word]) => `${formatCount(counts.get(state)!)} ${word}`)
    .join(", ");
}

const RANK: Record<StepState, number> = { running: 0, failed: 1, queued: 2, success: 3, skipped: 3, cancelled: 3 };

/** Running, then failed, then queued in queue order, then the finished ones; the run's order within each. */
export function orderSteps(steps: TaskStep[]): TaskStep[] {
  const pos = (s: TaskStep) => s.queue_position ?? Number.MAX_SAFE_INTEGER;
  return steps
    .map((s, i) => ({ s, i }))
    .sort((a, b) => RANK[a.s.state] - RANK[b.s.state] || (a.s.state === "queued" ? pos(a.s) - pos(b.s) : 0) || a.i - b.i)
    .map(({ s }) => s);
}

/** Share of the run done, 0 to 1: a finished step counts whole, a running one by its done of total, a queued one not at all. */
export function overallProgress(steps: TaskStep[]): number | null {
  if (!steps.length) return null;
  let sum = 0;
  for (const s of steps) {
    if (s.state === "queued") continue;
    if (s.state === "running") {
      if (s.done != null && s.total != null && s.total > 0) sum += Math.max(0, Math.min(1, s.done / s.total));
      continue;
    }
    sum += 1;
  }
  return sum / steps.length;
}

const NONE: Record<Exclude<StepFilter, "">, string> = {
  running: "No running steps",
  queued: "No queued steps",
  failed: "No failed steps",
  success: "No steps done yet",
};

function OverallBar({ steps }: { steps: TaskStep[] }) {
  const share = overallProgress(steps);
  // The track keeps its place before the first step lands.
  if (share == null) return <div aria-hidden="true" className={cx(STEPS_BAR, "invisible")} />;
  const pct = Math.round(share * 100);
  return (
    <div
      role="progressbar"
      aria-label="Overall progress"
      aria-valuemin={0}
      aria-valuemax={100}
      aria-valuenow={pct}
      title={`${pct}% done`}
      className={cx(STEPS_BAR, FADE_IN)}
    >
      <div className="h-full rounded-full bg-progress" style={{ width: `${share * 100}%` }} />
    </div>
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
      className={STEP_BAR}
    >
      <div className="h-full rounded-full bg-progress" style={{ width: `${pct}%` }} />
    </div>
  );
}

export function StepList({ run, noteOf }: { run: TaskRun; noteOf?: StepNote }) {
  const steps = useMemo(() => run.steps ?? [], [run.steps]);
  const [filter, setFilter] = useState<StepFilter>("");
  const ordered = useMemo(() => orderSteps(steps), [steps]);
  const shown = filter ? ordered.filter((s) => s.state === filter) : ordered;
  useLastCount(STEPS_COUNT_KEY, steps.length);
  return (
    <section aria-label="Steps" className={FRAME}>
      <StepsHead
        id="steps-filter"
        summary={<span className={FADE_IN}>{stepSummary(steps)}</span>}
        progress={<OverallBar steps={steps} />}
        filter={filter}
        onFilter={setFilter}
      />
      {!steps.length ? (
        <p className={cx(STEP_EMPTY, FADE_IN)}>No steps yet</p>
      ) : !shown.length ? (
        <p className={STEP_EMPTY}>{filter ? NONE[filter] : null}</p>
      ) : (
        <ul className={cx(STEP_LIST, FADE_IN)}>
          {shown.map((s) => {
            const note = stepLabel(s, noteOf);
            return (
              <li key={s.key} className={STEP_ROW}>
                <StatusIcon status={s.state} size={14} />
                <span className="min-w-0 flex-1 truncate font-semibold text-text">{s.title}</span>
                {s.state === "running" ? <StepBar step={s} /> : null}
                <span title={note} className={cx("max-w-[50%] truncate text-right text-xs", s.state === "failed" ? "text-danger" : "text-muted")}>
                  {note}
                </span>
              </li>
            );
          })}
        </ul>
      )}
    </section>
  );
}
