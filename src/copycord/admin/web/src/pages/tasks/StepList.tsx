// The run detail's Steps card, the same for every task type. The head counts the steps by state and
// filters them by state (the run's segmented bar is in the Summary card); under it one dense line per
// step with its status glyph, its title, a thin bar while it runs, its note and its state in words in the
// state's colour. Running and failed steps sort to the top, and the list scrolls past twelve rows, so a
// run of a hundred channels keeps a short card.

import { useMemo, useState } from "react";
import { useLastCount } from "../../components/ui/lastCount";
import { cx } from "../../components/ui/cx";
import type { StepState, TaskRun, TaskStep } from "../../types/tasks";
import { COLOUR, STATUS_LABEL, StatusIcon } from "./StatusIcon";
import {
  FADE_IN,
  RUN_CARD,
  STEPS_COUNT_KEY,
  STEP_BAR,
  STEP_EMPTY,
  STEP_LIST,
  STEP_NOTE,
  STEP_ROW,
  STEP_STATE,
  STEP_TITLE,
  StepsHead,
  stepRowsHeight,
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

/** A step's state in words at the row's edge: the filter's words, so a finished step reads "Done". */
export const STEP_STATE_LABEL: Record<StepState, string> = { ...STATUS_LABEL, success: "Done" };

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

const NONE: Record<Exclude<StepFilter, "">, string> = {
  running: "No running steps",
  queued: "No queued steps",
  failed: "No failed steps",
  success: "No steps done yet",
  cancelled: "No cancelled steps",
  skipped: "No skipped steps",
};

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
      <div className="h-full rounded-full bg-info" style={{ width: `${pct}%` }} />
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
    <section aria-label="Steps" className={RUN_CARD}>
      <StepsHead id="steps-filter" summary={<span className={FADE_IN}>{stepSummary(steps)}</span>} filter={filter} onFilter={setFilter} />
      {!steps.length ? (
        <p className={cx(STEP_EMPTY, FADE_IN)}>No steps yet</p>
      ) : !shown.length ? (
        <p className={STEP_EMPTY} style={{ minHeight: stepRowsHeight(steps.length) }}>
          {filter ? NONE[filter] : null}
        </p>
      ) : (
        <ul className={cx(STEP_LIST, FADE_IN)}>
          {shown.map((s) => {
            const word = STEP_STATE_LABEL[s.state] ?? s.state;
            const label = stepLabel(s, noteOf);
            // The state is in words at the edge; a note that only repeats it is left out.
            const note = label === word || label === STATUS_LABEL[s.state] ? "" : label;
            return (
              <li key={s.key} className={STEP_ROW}>
                <StatusIcon status={s.state} size={14} />
                <span className={STEP_TITLE}>{s.title}</span>
                {s.state === "running" ? <StepBar step={s} /> : null}
                <span title={note || undefined} className={cx(STEP_NOTE, s.state === "failed" ? "text-danger" : "text-muted")}>
                  {note}
                </span>
                <span className={cx(STEP_STATE, COLOUR[s.state])}>{word}</span>
              </li>
            );
          })}
        </ul>
      )}
    </section>
  );
}
