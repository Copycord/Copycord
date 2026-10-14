// The run's friendly log from run.log_lines: monospace, line numbers from seq, level colours, an
// optional step filter from StepList. It follows the tail while the user is at the bottom; scrolling up
// stops following and shows a "Jump to latest" chip. Nothing is fetched: the lines arrive with the run
// detail and in live frames.

import { useCallback, useLayoutEffect, useMemo, useRef, useState } from "react";
import { cx } from "../../components/ui/cx";
import type { TaskLogLine, TaskRun } from "../../types/tasks";
import { FRAME } from "./TasksSkeleton";

/** How close to the bottom, in px, still counts as at the bottom. */
const BOTTOM_SLACK = 8;

const LEVEL_CLASS: Record<string, string> = {
  info: "text-text-2",
  warn: "text-amber-400",
  error: "text-danger",
};

/** The well formed lines of run.log_lines, in seq order. */
export function logLines(run: TaskRun): TaskLogLine[] {
  if (!Array.isArray(run.log_lines)) return [];
  return (run.log_lines as TaskLogLine[])
    .filter((l) => l && typeof l === "object" && typeof l.msg === "string")
    .sort((a, b) => a.seq - b.seq);
}

export function LogView({ run, step, onClearStep }: { run: TaskRun; step: string | null; onClearStep?: () => void }) {
  const all = useMemo(() => logLines(run), [run]);
  const lines = useMemo(() => (step ? all.filter((l) => l.step === step) : all), [all, step]);
  const box = useRef<HTMLDivElement>(null);
  const [following, setFollowing] = useState(true);
  const stepTitle = step ? (run.steps?.find((s) => s.key === step)?.title ?? step) : null;

  const toBottom = useCallback(() => {
    const el = box.current;
    if (el) el.scrollTop = el.scrollHeight;
  }, []);

  useLayoutEffect(() => {
    if (following) toBottom();
  }, [lines, following, toBottom]);

  const onScroll = () => {
    const el = box.current;
    if (!el) return;
    setFollowing(el.scrollHeight - el.scrollTop - el.clientHeight <= BOTTOM_SLACK);
  };

  return (
    <section aria-label="Log" className={FRAME}>
      <div className="flex items-center gap-2 border-b border-border px-4 py-2.5">
        <h3 className="m-0 text-sm font-semibold text-text">Log</h3>
        {stepTitle ? (
          <span className="text-xs text-muted">
            {stepTitle}
            {onClearStep ? (
              <button type="button" onClick={onClearStep} className="ml-2 cursor-pointer border-0 bg-transparent p-0 text-xs text-accent hover:underline">
                Show all steps
              </button>
            ) : null}
          </span>
        ) : null}
      </div>
      <div className="relative">
        <div
          ref={box}
          role="log"
          aria-label="Run log"
          onScroll={onScroll}
          className="max-h-[480px] min-h-[120px] overflow-auto py-2 font-mono text-xs leading-5"
        >
          {lines.length === 0 ? (
            <p className="m-0 px-4 text-muted">{step ? "No log lines for this step" : "No log lines yet"}</p>
          ) : (
            lines.map((l) => (
              <div key={l.seq} data-level={l.level} className={cx("flex gap-3 px-4 hover:bg-rail-hover", LEVEL_CLASS[l.level] ?? LEVEL_CLASS.info)}>
                <span aria-hidden="true" className="w-10 shrink-0 select-none text-right text-muted/60">
                  {l.seq}
                </span>
                <span className="min-w-0 whitespace-pre-wrap break-words">{l.msg}</span>
              </div>
            ))
          )}
        </div>
        {following ? null : (
          <button
            type="button"
            onClick={() => {
              setFollowing(true);
              toBottom();
            }}
            className="absolute bottom-3 left-1/2 -translate-x-1/2 cursor-pointer rounded-full border border-border bg-card-solid px-3 py-1 text-xs font-semibold text-accent shadow-card hover:bg-rail-hover"
          >
            Jump to latest
          </button>
        )}
      </div>
    </section>
  );
}
