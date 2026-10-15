// The list frame of the Tasks page: its header row ("N task runs", auto-refresh, filter slots) and the
// run rows in the GitHub Actions layout, or the empty state.

import { useEffect, useState, type ReactNode } from "react";
import { Link } from "react-router";
import { Badge } from "../../components/ui/Badge";
import type { ConfirmOptions } from "../../components/ui/ConfirmDialog";
import { cx } from "../../components/ui/cx";
import { EmptyState } from "../../components/ui/EmptyState";
import { ACTIVE_STATUSES, type TaskRun } from "../../types/tasks";
import { RowMenu, type MenuItem } from "../channels/actions/RowMenu";
import { ToolbarSelect } from "../channels/ToolbarSelect";
import { RunStatusPill, StatusIcon } from "./StatusIcon";
import {
  AUTO_REFRESH_LABEL,
  AUTO_REFRESH_OPTIONS,
  BUSY,
  CONTEXT_CHIP,
  FADE_IN,
  FRAME_HEAD,
  FRAME_HEAD_FILTERS,
  FRAME_HEAD_LEFT,
  LIST_BODY,
  LIST_FRAME,
  MetaChip,
  ROW,
  ROW_CHIPS,
  ROW_CHIPS_EXTRA,
  ROW_ID,
  ROW_MAIN,
  ROW_MENU,
  ROW_NAME,
  ROW_TIMING,
  RUN_COUNT,
  RunCountLoading,
  RunRowsLoading,
  runCountText,
} from "./TasksSkeleton";

export { AUTO_REFRESH_KEY, AUTO_REFRESH_OPTIONS } from "./TasksSkeleton";

// rerun: runs stored before Re-run was removed still say how they started; nothing starts one now.
const TRIGGER: Record<string, string> = { manual: "Manual", rerun: "Re-run", api: "API" };

/** How the run started, for the trigger chip: "Manual". */
export function triggerLabel(run: Pick<TaskRun, "trigger">): string {
  return TRIGGER[run.trigger] ?? "Other";
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

/** "just now", "5m ago", "3h ago", "2d ago"; past a week the date itself. */
export function formatAgo(iso: string | null | undefined, now: number): string {
  if (!iso) return "";
  const t = Date.parse(iso);
  if (Number.isNaN(t)) return "";
  const m = Math.floor(Math.max(0, now - t) / 60000);
  if (m < 1) return "just now";
  if (m < 60) return `${m}m ago`;
  if (m < 1440) return `${Math.floor(m / 60)}h ago`;
  if (m < 10080) return `${Math.floor(m / 1440)}d ago`;
  return formatWhen(iso);
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
export function ContextBadge({ run, title }: { run: TaskRun; title?: string }) {
  if (!run.context?.label) return null;
  return (
    <Badge tone="neutral" className={CONTEXT_CHIP} title={title}>
      {run.context.label}
    </Badge>
  );
}

export function RunTiming({ run }: { run: TaskRun }) {
  const now = useNow(run.status === "running");
  const duration = runDuration(run, now);
  return (
    <div className={ROW_TIMING}>
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

/** One run: the task name and number over the run ID, then the run page's status pill and chips, the timing and the menu. */
export function RunRow({ run, typeTitle, actions }: RunRowProps) {
  return (
    <li className={ROW} data-run-id={run.id}>
      <StatusIcon status={run.status} />
      <div className={ROW_MAIN}>
        <div className={ROW_NAME}>
          <Link to={`/tasks/runs/${encodeURIComponent(run.id)}`} title={run.title} className="text-text no-underline hover:underline">
            {typeTitle} #{run.number}
          </Link>
        </div>
        <div className={ROW_ID} title={`Run ID ${run.id}`}>
          {run.id.slice(0, 8)}
        </div>
      </div>
      <div className={ROW_CHIPS}>
        <RunStatusPill status={run.status} />
        <span className={ROW_CHIPS_EXTRA}>
          <ContextBadge run={run} title="Mapping" />
          <MetaChip glyph="trigger" title="Trigger">
            {triggerLabel(run)}
          </MetaChip>
          <MetaChip glyph="actor" title="Started by">
            {run.actor}
          </MetaChip>
        </span>
      </div>
      <RunTiming run={run} />
      {actions}
    </li>
  );
}

export interface RunListProps {
  runs: TaskRun[];
  total: number;
  /** No page yet since the list mounted: the skeleton rows. */
  loading: boolean;
  /** A fetch is out (search, filters, paging, refresh): the rows or the empty state stay, only marked busy. */
  refreshing?: boolean;
  typeTitle: (key: string) => string;
  autoRefresh: string;
  onAutoRefresh: (value: string) => void;
  /** Right side of the header row: the filter dropdowns. */
  filters?: ReactNode;
  /** Shown in place of rows when the page has none. */
  empty?: ReactNode;
  /** Under the rows: paging. */
  footer?: ReactNode;
  /** The right edge of each row: its kebab menu. */
  rowActions?: (run: TaskRun) => ReactNode;
}

export type RunAction = "view" | "cancel" | "delete";

/** The Cancel run and Delete confirms, word for word the same from the list's menu and the run page. */
export function runConfirm(act: "cancel" | "delete", name: string): ConfirmOptions {
  return act === "cancel"
    ? {
        title: `Cancel ${name}?`,
        body: "Work already done is kept; the run stops at its next checkpoint.",
        confirmLabel: "Cancel run",
        cancelLabel: "Keep running",
        confirmVariant: "ghost-red",
      }
    : {
        title: `Delete ${name}?`,
        body: "The run and its report are deleted. This cannot be undone.",
        confirmLabel: "Delete",
        cancelLabel: "Keep",
        confirmVariant: "ghost-red",
      };
}

/** The row kebab: View always; Cancel run while queued or running; Delete once finished, as on the run page. */
export function RunMenu({ run, onAction }: { run: TaskRun; onAction: (act: RunAction, run: TaskRun) => void }) {
  const live = ACTIVE_STATUSES.includes(run.status);
  const items: MenuItem[] = [{ act: "view", label: "View", onSelect: () => onAction("view", run) }];
  if (live) items.push({ act: "cancel", label: "Cancel run", onSelect: () => onAction("cancel", run) });
  else items.push({ act: "delete", label: "Delete", danger: true, onSelect: () => onAction("delete", run) });
  return (
    <div className={ROW_MENU}>
      <RowMenu kind="channel" items={items} />
    </div>
  );
}

export function RunList({
  runs,
  total,
  loading,
  refreshing = false,
  typeTitle,
  autoRefresh,
  onAutoRefresh,
  filters,
  empty,
  footer,
  rowActions,
}: RunListProps) {
  return (
    <div className={LIST_FRAME}>
      <div className={FRAME_HEAD}>
        <div className={FRAME_HEAD_LEFT}>
          {loading ? <RunCountLoading /> : <span className={cx(RUN_COUNT, FADE_IN)}>{runCountText(total)}</span>}
          <ToolbarSelect
            id="tasks-autorefresh"
            label={AUTO_REFRESH_LABEL}
            prefix={AUTO_REFRESH_LABEL}
            value={autoRefresh}
            options={AUTO_REFRESH_OPTIONS}
            onChange={onAutoRefresh}
          />
        </div>
        {filters ? <div className={FRAME_HEAD_FILTERS}>{filters}</div> : null}
      </div>
      <div className={LIST_BODY}>
        {loading ? (
          <RunRowsLoading />
        ) : runs.length === 0 ? (
          <div aria-busy={refreshing || undefined} className={cx(FADE_IN, refreshing && BUSY)}>
            {empty ?? <EmptyState plain title="No task runs yet" />}
          </div>
        ) : (
          <ul aria-label="Task runs" aria-busy={refreshing || undefined} className={cx("m-0 list-none p-0", FADE_IN, refreshing && BUSY)}>
            {runs.map((r) => (
              <RunRow key={r.id} run={r} typeTitle={typeTitle(r.type)} actions={rowActions?.(r)} />
            ))}
          </ul>
        )}
      </div>
      {footer}
    </div>
  );
}
