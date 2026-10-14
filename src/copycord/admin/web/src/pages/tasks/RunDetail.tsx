// One run at /tasks/runs/:id: the header with its status, number and meta line, and the Steps, Log and
// Report sections. The sections show what the run carries today; their full panels plug in by name.

import type { ReactNode } from "react";
import { Link } from "react-router";
import { EmptyState } from "../../components/ui/EmptyState";
import { Skeleton } from "../../components/ui/Skeleton";
import { errorText } from "../../lib/api";
import type { TaskRun } from "../../types/tasks";
import { ContextBadge, formatWhen, runDuration, triggerText, useNow } from "./RunList";
import { useTaskRun } from "./runStore";
import { STATUS_LABEL, StatusIcon } from "./StatusIcon";
import { FRAME } from "./TasksSkeleton";

export function RunSection({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section aria-label={title} className={FRAME}>
      <h3 className="m-0 border-b border-border px-4 py-2.5 text-sm font-semibold text-text">{title}</h3>
      <div className="px-4 py-3 text-sm text-text-2">{children}</div>
    </section>
  );
}

export function StepsSection({ run }: { run: TaskRun }) {
  const n = run.steps?.length ?? 0;
  return <RunSection title="Steps">{n === 1 ? "1 step" : `${n} steps`}</RunSection>;
}

export function LogSection({ run }: { run: TaskRun }) {
  const n = Array.isArray(run.log_lines) ? run.log_lines.length : 0;
  return <RunSection title="Log">{n === 1 ? "1 line" : `${n} lines`}</RunSection>;
}

export function ReportSection({ run }: { run: TaskRun }) {
  return <RunSection title="Report">{run.report == null ? "No report yet" : "Report ready"}</RunSection>;
}

export function RunHeader({ run, typeTitle }: { run: TaskRun; typeTitle: string }) {
  const now = useNow(run.status === "running");
  const duration = runDuration(run, now);
  return (
    <header className="mb-4">
      <nav aria-label="Breadcrumb" className="mb-2 text-sm text-muted">
        <Link to="/tasks" className="text-accent no-underline hover:underline">
          All tasks
        </Link>
        {" / "}
        <Link to={`/tasks?type=${encodeURIComponent(run.type)}`} className="text-accent no-underline hover:underline">
          {typeTitle}
        </Link>
      </nav>
      <div className="flex items-center gap-2">
        <StatusIcon status={run.status} size={20} />
        <h2 className="m-0 text-[22px] font-bold text-text">{run.title}</h2>
        <span className="text-[22px] text-muted">#{run.number}</span>
      </div>
      <div className="mt-1 flex flex-wrap items-center gap-x-3 gap-y-1 text-sm text-muted">
        <span>{triggerText(run)}</span>
        <ContextBadge run={run} />
        <span>{formatWhen(run.started_at ?? run.created_at)}</span>
        {duration ? <span>{duration}</span> : null}
        <span>{STATUS_LABEL[run.status]}</span>
      </div>
      {run.error ? <p className="mt-2 text-sm text-danger">{run.error}</p> : null}
    </header>
  );
}

export function RunDetail({ id, typeTitle }: { id: string; typeTitle: (key: string) => string }) {
  const { run, loading, error } = useTaskRun(id);
  if (!run) {
    if (loading) return <Skeleton.Block height={120} />;
    return <EmptyState title="Run not found" description={error ? errorText(error, "Could not load the run.") : undefined} />;
  }
  return (
    <div className="flex flex-col gap-4">
      <RunHeader run={run} typeTitle={typeTitle(run.type)} />
      <StepsSection run={run} />
      <LogSection run={run} />
      <ReportSection run={run} />
    </div>
  );
}
