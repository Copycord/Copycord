// The list frame of the Tasks page: its header row ("N task runs", auto-refresh, filter slots) and the
// run rows in the GitHub Actions layout, or the empty state.

import { useEffect, useState, type ReactNode } from "react";
import { Link } from "react-router";
import { Badge } from "../../components/ui/Badge";
import { EmptyState } from "../../components/ui/EmptyState";
import type { TaskRun } from "../../types/tasks";
import { ToolbarSelect } from "../channels/ToolbarSelect";
import { StatusIcon } from "./StatusIcon";
import { FRAME, FRAME_HEAD, ROW, RunRowsLoading } from "./TasksSkeleton";

export const AUTO_REFRESH_KEY = "tasks:autorefresh";
export const AUTO_REFRESH_OPTIONS = [
  { value: "0", label: "Auto-refresh: Off" },
  { value: "10", label: "Auto-refresh: 10s" },
  { value: "30", label: "Auto-refresh: 30s" },
  { value: "60", label: "Auto-refresh: 60s" },
] as const;

const TRIGGER: Record<string, string> = { manual: "Manual run", rerun: "Re-run", api: "API run" };

export function triggerText(run: Pick<TaskRun, "trigger" | "actor">): string {
  return `${TRIGGER[run.trigger] ?? "Run"} by ${run.actor}`;
}

const WHEN = new Intl.DateTimeFormat(undefined, {
  month: "short",
  day: "numeric",
  hour: "numeric",
  minute: "2-digit",
  timeZoneName: "short",
});

export function formatWhen(iso: string | null | undefined): string {
  if (!iso) return "";
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? "" : WHEN.format(d);
}

/** "48s", "1m 29s", "2h 5m". */
export function formatDuration(ms: number): string {
  const s = Math.max(0, Math.floor(ms / 1000));
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${s % 60}s`;
  return `${Math.floor(m / 60)}h ${m % 60}m`;
}

/** Wall clock that ticks once a second while `live`. */
export function useNow(live: boolean): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    if (!live) return;
    setNow(Date.now());
    const t = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(t);
  }, [live]);
  return now;
}

export function runDuration(run: TaskRun, now: number): string | null {
  if (!run.started_at) return null;
  const start = Date.parse(run.started_at);
  const end = run.finished_at ? Date.parse(run.finished_at) : now;
  return Number.isNaN(start) || Number.isNaN(end) ? null : formatDuration(end - start);
}

/** The branch-like pill: the mapping the run works on. */
export function ContextBadge({ run }: { run: TaskRun }) {
  if (!run.context?.label) return null;
  return (
    <Badge tone="neutral" className="border-transparent bg-brand-ghost font-mono font-normal text-accent">
      {run.context.label}
    </Badge>
  );
}

export function RunTiming({ run }: { run: TaskRun }) {
  const now = useNow(run.status === "running");
  const duration = runDuration(run, now);
  return (
    <div className="flex w-[170px] shrink-0 flex-col gap-0.5 text-xs text-muted max-md:w-auto">
      <span title="Started">{formatWhen(run.started_at ?? run.created_at)}</span>
      {duration ? <span title="Duration">{duration}</span> : null}
    </div>
  );
}

export interface RunRowProps {
  run: TaskRun;
  typeTitle: string;
  /** Right edge slot for the row menu. */
  actions?: ReactNode;
}

export function RunRow({ run, typeTitle, actions }: RunRowProps) {
  return (
    <li className={ROW} data-run-id={run.id}>
      <StatusIcon status={run.status} className="mt-0.5" />
      <div className="min-w-0 flex-1">
        <Link to={`/tasks/runs/${encodeURIComponent(run.id)}`} className="font-semibold text-text no-underline hover:underline">
          {run.title}
        </Link>
        <div className="mt-0.5 text-xs text-muted">
          {typeTitle} #{run.number}: {triggerText(run)}
        </div>
      </div>
      <div className="w-[180px] shrink-0 max-md:hidden">
        <ContextBadge run={run} />
      </div>
      <RunTiming run={run} />
      {actions}
    </li>
  );
}

export interface RunListProps {
  runs: TaskRun[];
  total: number;
  loading: boolean;
  typeTitle: (key: string) => string;
  autoRefresh: string;
  onAutoRefresh: (value: string) => void;
  /** Right side of the header row: the filter dropdowns. */
  filters?: ReactNode;
  /** Shown in place of rows when the page has none. */
  empty?: ReactNode;
  /** Under the rows: paging. */
  footer?: ReactNode;
}

export function RunList({ runs, total, loading, typeTitle, autoRefresh, onAutoRefresh, filters, empty, footer }: RunListProps) {
  const first = loading && runs.length === 0;
  return (
    <div className={FRAME}>
      <div className={FRAME_HEAD}>
        <div className="flex items-center gap-4">
          <span className="font-semibold text-text">{total === 1 ? "1 task run" : `${total} task runs`}</span>
          <ToolbarSelect id="tasks-autorefresh" label="Auto-refresh" value={autoRefresh} options={AUTO_REFRESH_OPTIONS} onChange={onAutoRefresh} />
        </div>
        {filters ? <div className="flex flex-wrap items-center gap-3">{filters}</div> : null}
      </div>
      {first ? (
        <RunRowsLoading />
      ) : runs.length === 0 ? (
        (empty ?? <EmptyState title="No task runs yet" className="m-4" />)
      ) : (
        <ul aria-label="Task runs" className="m-0 list-none p-0">
          {runs.map((r) => (
            <RunRow key={r.id} run={r} typeTitle={typeTitle(r.type)} />
          ))}
        </ul>
      )}
      {footer}
    </div>
  );
}
